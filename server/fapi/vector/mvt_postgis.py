import logging
import re

from fastapi import HTTPException, status
from asyncpg import Connection
from asyncpg.pool import Pool

from typing import Dict, List, Optional
from math import pi

from server.datasources import DataSourceVector, VectorDataLayer

logger = logging.getLogger(__name__)

MAP_WIDTH_IN_METRES: float = 40075016.68557849
TILE_WIDTH_IN_PIXELS: float = 4096.0
STANDARDIZED_PIXEL_SIZE: float = 0.00028


def tile_to_envelope(z: int, x: int, y: int) -> Dict[str, float]:

    # Width of world in EPSG:3857
    world_mercator_max: float = 20037508.3427892
    world_mercator_min: float = -1 * world_mercator_max
    world_mercator_size: float = world_mercator_max - world_mercator_min

    # Width in tiles
    world_tile_size: int = 2**z

    # Tile width in EPSG:3857
    tile_merc_size: float = world_mercator_size / world_tile_size

    # Calculate geographic bounds from tile coordinates
    # XYZ tile coordinates are in "image space" so origin is
    # top-left, not bottom right
    env: Dict[str, float] = {}
    env["xmin"] = world_mercator_min + tile_merc_size * x
    env["xmax"] = world_mercator_min + tile_merc_size * (x + 1)
    env["ymin"] = world_mercator_max - tile_merc_size * (y + 1)
    env["ymax"] = world_mercator_max - tile_merc_size * (y)

    return env


def envelope_to_bounds_sql(bounds: Dict[str, float]) -> str:
    DENSIFY_FACTOR = 4
    bounds["segment_size"] = (bounds["xmax"] - bounds["xmin"]) / DENSIFY_FACTOR
    segment: str = (
        "ST_Segmentize(ST_MakeEnvelope({xmin}, {ymin}, {xmax}, {ymax}, 3857),{segment_size})"
    )
    return segment.format(**bounds)


def tolerance(zoom: int, extent: int) -> float:
    """
    https://github.com/chronhq/backend/blob/d928f451ffea17f6b99bf69861f757e47123ebd0/project/api/views/endpoints/mvt_stv.py#L43
    Takes a web mercator zoom level and returns the pixel resolution for that
    scale according to the global TILE_WIDTH_IN_PIXELS size
    """
    tolerance_multiplier: float = 1 if zoom > 5 else (2.2 - 0.2 * zoom)
    t: float = tolerance_multiplier * MAP_WIDTH_IN_METRES / (extent * (2**zoom))
    return t


def tolerance2(zoom: int) -> float:
    return 6378137 * 2 * pi / (2 ** (zoom + 8))


def query_layer_of_mvt(
    z: int,
    x: int,
    y: int,
    simplify: bool,
    sub_query_layer: str,
    extent: int,
    margin: str,
    fields_from_subquery: str,
    geom_field: str,
    layer_id: str,
) -> Optional[str]:
    if simplify:
        scale = tolerance(z, extent) if z > 11 else tolerance2(z)
        simplify_str: str = f"ST_SimplifyPreserveTopology(t.{geom_field}, {scale})"
    else:
        simplify_str = f"t.{geom_field}"

    q: str = (
        f"(\
            WITH mvtgeom AS (\
                SELECT\
                    ST_AsMVTGeom({simplify_str}, ST_TileEnvelope({z}, {x}, {y})) AS geom{fields_from_subquery}\
                FROM ({sub_query_layer}) AS t WHERE t.{geom_field} IS NOT NULL\
                    AND t.{geom_field} && ST_TileEnvelope({z}, {x}, {y}{margin})\
            )\
            SELECT ST_AsMVT(mvtgeom.*, '{layer_id}', {extent}, 'geom') AS mvt FROM mvtgeom\
        )"
    )
    return q


async def generate_mvt(
    ds: DataSourceVector, db_pool: Pool, z: int, x: int, y: int
) -> bytes:

    if ds.layers is not None:
        queries: List[str] = []
        for layer in ds.layers:  # type: VectorDataLayer
            if z < layer.minzoom or z > layer.maxzoom:
                continue

            if layer.queries is not None:
                ql_mvt: Optional[str] = query_layer_of_mvt_from_sql(
                    z, x, y, ds.margin, layer
                )
            else:
                ql_mvt = query_layer_of_mvt(
                    z,
                    x,
                    y,
                    layer.simplify,
                    layer.query,
                    ds.extent,
                    ds.margin,
                    layer.fields_from_subquery,
                    layer.geom_field,
                    layer.id,
                )

            if ql_mvt is not None:
                queries.append(ql_mvt)

        # None of the layers fall within the zoom range
        if len(queries) == 0:
            return b""

        queries_joined = "||".join(queries)
        query = re.sub(r"\s+", " ", f"SELECT {queries_joined} AS mvt_tile")

        try:
            connection: Connection
            async with db_pool.acquire() as connection:
                mvt: Optional[bytes] = await connection.fetchval(query)
        except Exception as e:
            message: str = (
                f"'generate_mvt': error create vector tile {z}/{x}/{y}: {str(e)}"
            )
            logger.error(message)
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=message
            )

        return mvt

    return b""


def query_layer_of_mvt_from_sql(
    z: int, x: int, y: int, margin: str, layer: VectorDataLayer
) -> Optional[str]:
    layer_queries: List[str] = []
    for layer_query in layer.queries:  # type: LayerQuerySQL
        if z >= layer_query.minzoom and z <= layer_query.maxzoom:
            layer_query_sql: str = layer_query.sql
            layer_query_sql = layer_query_sql.strip()
            layer_query_sql = re.sub(r"\s+", " ", layer_query_sql)
            layer_query_sql = layer_query_sql.replace(";", "")
            layer_query_sql = layer_query_sql.replace("$zoom", str(z))
            sub_query: str = (
                f"SELECT\
                    ST_AsMVTGeom(t.geom, ST_TileEnvelope({z}, {x}, {y})) AS geom,\
                    t.tags - 'id' AS tags\
                FROM ({layer_query_sql}) AS t WHERE t.geom IS NOT NULL\
                    AND t.geom && ST_TileEnvelope({z}, {x}, {y}{margin})"
            )
            layer_queries.append(sub_query)

    if len(layer_queries) > 0:
        layer_queries_joined = "\nUNION ALL\n".join(layer_queries)
        layer_queries_format_str = f"(SELECT ST_AsMVT(mvtGeom.*, '{layer.id}') FROM ({layer_queries_joined}) AS mvtGeom)"
        return layer_queries_format_str

    return None
