import os
import orjson
import logging
import multiprocessing

from typing import Optional, Dict, Any, List, Union, Tuple
from enum import StrEnum

from fastapi import FastAPI
from asyncpg import Connection
from asyncpg.pool import Pool

from vector_tiles.jfe.parser import parse_jfe
from vector_tiles.jfe.evaluate import to_sql_where
from vector_tiles.jfe.ast import Node

logger = logging.getLogger(__name__)

BUFFER = 64
EXTENT = 4096
MINZOOM = 0
MAXZOOM = 20


class DataType(StrEnum):
    vector = "vector"
    raster = "raster"


class StoreType(StrEnum):
    mbtiles = "mbtiles"
    internal = "internal"
    tiles = "tiles"
    tilejson = "tilejson"


class LayerType(StrEnum):
    point = "point"
    line = "line"
    polygon = "polygon"
    raster = "raster"


class EncodingType(StrEnum):
    mapbox = "mapbox"
    terrarium = "terrarium"
    f32 = "f32"
    none = "none"


class ResamplingType(StrEnum):
    average = "average"
    antialias = "antialias"
    nearest = "nearest"
    bilinear = "bilinear"
    cubic = "cubic"
    cubicspline = "cubicspline"
    lanczos = "lanczos"
    min = "min"
    max = "max"
    med = "med"


class Extensions(StrEnum):
    tif = ".tif"
    tiff = ".tiff"
    TIF = ".TIF"
    TIFF = "TIFF"


VECTOR = DataType.vector
RASTER = DataType.raster
DATA_TYPE = set(dt.value for dt in DataType)
STORE_TYPE = set(st.value for st in StoreType)
LAYER_TYPE = set(lt.value for lt in LayerType)
EXTENSIONS = set(ext.value for ext in Extensions)
RESAMPLING = set(rt.value for rt in ResamplingType)


# ============================== Raster ==============================


class DataStoreRasterInternal:
    __slots__ = (
        "type",
        "store",
        "host",
        "port",
        "dataset",
        "file",
        "folder",
        "encoding",
    )

    def __init__(
        self,
        host: Optional[str] = None,
        port: Optional[str] = None,
        dataset: Optional[str] = None,
        file: Optional[str] = None,
        folder: Optional[str] = None,
    ):
        self.type = DataType.raster
        self.store = StoreType.internal
        self.host = host
        self.port = port
        self.dataset = dataset
        self.file = file
        self.folder = folder

        if file is not None and self.dataset is None:
            self.dataset, _ = os.path.splitext(self.file)
        if folder is not None and self.dataset is None:
            self.dataset = folder

        self.encoding = EncodingType("f32")


class DataStoreRasterMBTiles:
    __slots__ = ("type", "store", "path", "encoding")

    def __init__(self, path: str, encoding_type: str):
        self.type = DataType.raster
        self.store = StoreType.mbtiles
        self.path = path
        self.encoding = EncodingType(encoding_type)


class DataStoreRasterTiles:
    __slots__ = ("type", "store", "tiles", "encoding")

    def __init__(self, tiles: List[str], encoding_type: str):
        self.type = DataType.raster
        self.store = StoreType.tiles
        self.tiles = tiles
        self.encoding = EncodingType(encoding_type)


class DataStoreRasterTileJson:
    __slots__ = ("type", "store", "url", "encoding")

    def __init__(self, url: str, encoding_type: str):
        self.type = DataType.raster
        self.store = StoreType.tilejson
        self.url = url
        self.encoding = EncodingType(encoding_type)


# ============================== Vector ==============================


class DataStoreVectorInternal:
    __slots__ = (
        "type",
        "store",
    )

    def __init__(self):
        self.type = DataType.vector
        self.store = StoreType.internal


class DataStoreVectorMBTiles:
    __slots__ = ("type", "store", "path")

    def __init__(self, path: str):
        self.type = DataType.vector
        self.store = StoreType.mbtiles
        self.path = path


class DataStoreVectorTiles:
    __slots__ = ("type", "store", "tiles", "keys")

    def __init__(self, tiles: List[str], keys: Optional[List[str]] = None):
        self.type = DataType.vector
        self.store = StoreType.tiles
        self.tiles = tiles
        if keys is not None:
            self.keys = keys
        else:
            self.keys = []


class DataStoreVectorTileJson:
    __slots__ = ("type", "store", "url")

    def __init__(self, url: str):
        self.type = DataType.vector
        self.store = StoreType.tilejson
        self.url = url


DataStoreRaster = Union[
    DataStoreRasterInternal,
    DataStoreRasterMBTiles,
    DataStoreRasterTiles,
    DataStoreRasterTileJson,
]

DataStoreVector = Union[
    DataStoreVectorInternal,
    DataStoreVectorMBTiles,
    DataStoreVectorTiles,
    DataStoreVectorTileJson,
]

DataStore = Union[DataStoreVector, DataStoreRaster]


class LayerQuerySQL:
    __slots__ = ("minzoom", "maxzoom", "sql")

    def __init__(
        self,
        minzoom: int,
        maxzoom: int,
        sql: str,
    ):
        self.minzoom = minzoom
        self.maxzoom = maxzoom
        self.sql = sql

    def __str__(self):
        return f"SubQuerySQL(minzoom = {self.minzoom}, maxzoom = {self.maxzoom}, sql = '{self.sql}')"

    def __repr__(self):
        return f"SubQuerySQL(minzoom = {self.minzoom}, maxzoom = {self.maxzoom}, sql = '{self.sql}')"


class Field:
    __slots__ = ("name", "name_in_db", "encode", "description")

    def __init__(
        self,
        name: str,
        name_in_db: Optional[str] = None,
        encode: bool = False,
        description: Optional[str] = None,
    ):
        self.name = name
        if name_in_db is None:
            self.name_in_db = name
        else:
            self.name_in_db = name_in_db

        self.encode = encode
        self.description = description

    def __str__(self):
        return f"Field(name = {self.name}, name_in_db = {self.name_in_db}, encode = {self.encode})"

    def __repr__(self):
        return f"Field(name = {self.name}, name_in_db = {self.name_in_db}, encode = {self.encode})"


def is_encode_field(field: Field, geom_field: str) -> bool:
    if field.name_in_db != geom_field and field.encode:
        return True
    return False


class VectorDataLayer:
    __slots__ = (
        "id",
        "layer_type",
        "store_layer",
        "geom_field",
        "description",
        "minzoom",
        "maxzoom",
        "simplify",
        "filter",
        "fields",
        "queries",
        "field_mapping",
        "fields_from_subquery",
        "where_clause",
        "select",
        "query",
    )

    def __init__(
        self,
        identifier: str,
        layer_type: str,
        store_layer: str,
        geom_field: str,
        description: Optional[str] = None,
        minzoom: int = MINZOOM,
        maxzoom: int = MAXZOOM,
        simplify: bool = False,
        filter: Optional[List[Any]] = None,
        fields: Optional[List[Field]] = None,
        queries: Optional[List[LayerQuerySQL]] = None,
    ):
        self.id: str = identifier
        self.layer_type: LayerType = LayerType(layer_type)
        self.store_layer: str = store_layer
        self.geom_field: str = geom_field
        self.description: str = description
        self.minzoom: int = minzoom
        self.maxzoom: int = maxzoom
        self.simplify: bool = simplify
        self.filter: Optional[List[Any]] = filter
        self.fields: Optional[List[Field]] = fields
        self.queries: Optional[List[LayerQuerySQL]] = queries

        self.field_mapping: Optional[Dict[str, str]] = None
        self.fields_from_subquery: str = ""
        self.where_clause: Optional[str] = None

        if fields is not None:
            # Query and encoding geometry + attributes from fields where encode=true
            encode_fields: List[str] = [
                f"t.{field.name_in_db}"
                for field in fields
                if is_encode_field(field, geom_field)
            ]
            if len(encode_fields) > 0:
                self.fields_from_subquery = ", " + ", ".join(encode_fields)

            base_fields: List[str] = [
                f'"{field.name_in_db}"'
                for field in fields
                if field.name_in_db != geom_field
            ]
            if len(base_fields) > 0:
                fields_query: str = ", " + ", ".join(base_fields)
            else:
                fields_query = ""

            self.select: str = f'SELECT "{geom_field}"{fields_query} FROM {store_layer}'
            self.field_mapping = {field.name: field.name_in_db for field in fields}
        else:
            # Query only geometry from layer
            self.select = f'SELECT "{geom_field}" FROM {store_layer}'

        if filter is not None and self.field_mapping is not None:
            parsed_jfe: Node = parse_jfe(filter, geom_field)
            self.where_clause: str = to_sql_where(parsed_jfe, self.field_mapping)
        if self.where_clause is not None and self.where_clause != "":
            self.query = f"{self.select} WHERE {self.where_clause}"
        else:
            self.query = self.select


class TileCacheParam:
    __slots__ = ("maxzoom", "maxsize", "preseed", "preseed_maxzoom")

    def __init__(self, maxzoom: int, maxsize: int, preseed: bool, preseed_maxzoom: int):
        self.maxzoom = maxzoom
        self.maxsize = maxsize
        self.preseed = preseed
        self.preseed_maxzoom = preseed_maxzoom


class DataSourceBounds:
    __slots__ = ("lng_w", "lat_s", "lng_e", "lat_n")

    def __init__(self, lng_w: float, lat_s: float, lng_e: float, lat_n: float):
        self.lng_w = lng_w
        self.lat_s = lat_s
        self.lng_e = lng_e
        self.lat_n = lat_n

    def __str__(self):
        return f"lng_w: {self.lng_w}, lat_s: {self.lat_s}, lng_e: {self.lng_e}, lat_n: {self.lat_n}"


class DataSourceCenter:
    __slots__ = ("lng", "lat", "zoom")

    def __init__(self, lng: float, lat: float, zoom: Optional[int] = None):
        self.lng = lng
        self.lat = lat
        self.zoom = zoom

    def __str__(self):
        return f"lng: {self.lng}, lat: {self.lat}, zoom: {self.zoom}"


class RasterTileSettings:
    def __init__(self, ds: Dict[str, Any]):
        verbose = ds.get("verbose")
        if verbose is not None:
            self.verbose: bool = verbose
        else:
            self.verbose = False

        self.resampling: str = ds.get("resampling") or "average"
        self.tiledriver: str = ds.get("tiledriver") or "PNG"
        self.tile_size: int = ds.get("tile_size") or 256

        xyz = ds.get("xyz")
        if xyz is not None:
            self.xyz: bool = xyz
        else:
            self.xyz = True

        self.count_processes: int = (
            ds.get("count_processes") or multiprocessing.cpu_count()
        )
        self.minzoom: int = ds.get("minzoom") or MINZOOM
        self.maxzoom: int = ds.get("maxzoom") or MINZOOM

        mbtiles = ds.get("mbtiles")
        if mbtiles is not None:
            self.mbtiles: bool = mbtiles
        else:
            self.mbtiles = True

        self.warnings: bool = ds.get("warnings") or False

        save_tile_detail_db = ds.get("save_tile_detail_db")
        if save_tile_detail_db is not None:
            self.save_tile_detail_db: bool = save_tile_detail_db
        else:
            self.save_tile_detail_db = True

        self.warp: bool = ds.get("warp") or False
        self.resampling_warp: str = ds.get("resampling_warp") or "average"
        self.remove_processing_raster_files: bool = (
            ds.get("remove_processing_raster_files") or False
        )

        encode_to_rgba = ds.get("encode_to_rgba")
        if encode_to_rgba is not None:
            self.encode_to_rgba: bool = encode_to_rgba
        else:
            self.encode_to_rgba = True

        self.mosaic_merge: bool = ds.get("mosaic_merge") or False
        self.nodata_default: float = ds.get("nodata_default") or -999999
        self.pixel_selection_method: str = (
            ds.get("pixel_selection_method") or "FirstMethod"
        )

        merge = ds.get("merge")
        if merge is not None:
            self.merge: bool = merge
        else:
            self.merge = True


class VectorTileSettings:
    def __init__(self, ds: Dict[str, Any]):
        self.count_processes: int = (
            ds.get("count_processes") or multiprocessing.cpu_count()
        )
        self.minzoom: int = ds.get("minzoom") or MINZOOM
        self.maxzoom: int = ds.get("maxzoom") or MINZOOM


def setup_data_store(store: str, data_store_dict: Dict[str, Any]) -> DataStore:
    data_store_type: str = data_store_dict.get("type")
    assert (
        data_store_type in DATA_TYPE
    ), f"Data store type must have one of the values {DATA_TYPE}, but got '{data_store_type}'"

    # Vector data
    if data_store_type == DataType.vector:
        if store == StoreType.mbtiles:
            raise Exception(f"Vector Store type '{StoreType.mbtiles}' not implemented")
        elif store == StoreType.internal:
            # Internal case
            # Generating tiles offline from OSM data using PostGIS
            return DataStoreVectorInternal()

        elif store == StoreType.tiles:
            # Use external tiles from MapTiler
            # raise Exception(f"Vector Store type '{StoreType.tiles}' not implemented")
            tiles: List[str] = data_store_dict.get("tiles")
            assert isinstance(
                tiles, list
            ), f"'tiles' has wrong format: {tiles}, must be a List"
            keys: List[str] = data_store_dict.get("keys")
            return DataStoreVectorTiles(tiles, keys)

        elif store == StoreType.tilejson:
            raise Exception(f"Vector Store type '{StoreType.tilejson}' not implemented")
        else:
            raise Exception(f"Vector Store type '{store}' unsupported")

    # Raster data
    if data_store_type == DataType.raster:
        if store == StoreType.mbtiles:
            raise Exception(f"Raster Store type '{StoreType.mbtiles}' not implemented")
        elif store == StoreType.internal:
            # Internal case
            # Defaults host, port for PostgreSQL
            host: str = data_store_dict.get("host")
            port: int = data_store_dict.get("port")
            dataset: str = data_store_dict.get("dataset")
            file: Optional[str] = data_store_dict.get("file")
            folder: Optional[str] = data_store_dict.get("folder")

            return DataStoreRasterInternal(
                host,
                port,
                dataset,
                file=file,
                folder=folder,
            )

        elif store == StoreType.tiles:
            # Use external tiles from MapTiler
            # raise Exception(f"Vector Store type '{StoreType.tiles}' not implemented")
            tiles = data_store_dict.get("tiles")
            assert isinstance(
                tiles, list
            ), f"'tiles' has wrong format: {tiles}, must be a List"
            return DataStoreRasterTiles(tiles)

        elif store == StoreType.tilejson:
            raise Exception(f"Vector Store type '{StoreType.tilejson}' not implemented")
        else:
            raise Exception(f"Vector Store type '{store}' unsupported")


class DataSourceRaster:
    def __init__(self, ds: Dict[str, Any]):
        self.id = ds.get("id")

        data_type = ds.get("type")
        assert (
            data_type == DataType.raster
        ), f"Type of raster datasource must have '{DataType.raster}', but got '{data_type}'"
        self.type = DataType(data_type)

        self.mosaics = ds.get("mosaics")

        data_store: Dict[str, Any] = ds.get("dataStore")
        assert isinstance(data_store, dict), f"'dataStore' has wrong type: {data_store}"

        store: str = data_store.get("store")
        assert (
            store in STORE_TYPE
        ), f"'store' must have one of the values {STORE_TYPE}, but got '{store}'"
        self.data_store = setup_data_store(store, data_store)
        self.mbtiles = ds.get("mbtiles") or False

        bounds = ds.get("bounds")
        if bounds is not None:
            assert (
                isinstance(bounds, dict) and len(bounds) == 4
            ), f"'bounds' has wrong type: {bounds}"
            self.bounds: DataSourceBounds = DataSourceBounds(**bounds)
        else:
            self.bounds = None

        center = ds.get("center")
        if center is not None:
            assert isinstance(center, list) and (
                len(center) == 3 or len(center) == 2
            ), f"'center' has wrong format: {center}"
            self.center: DataSourceCenter = DataSourceCenter(*center)
        else:
            self.center = None

        self.minzoom: int = ds.get("minzoom") or MINZOOM
        self.maxzoom: int = ds.get("maxzoom") or 13

        pyr_settings: Optional[Dict[str, Any]] = ds.get("pyramidSettings")
        if pyr_settings is not None:
            self.pyr_settings = RasterTileSettings(pyr_settings)
        else:
            self.pyr_settings = RasterTileSettings({})

        self.pyr_settings.mbtiles = self.mbtiles
        enc: str = ds.get("encoding") or "f32"
        self.encoding = EncodingType(enc)
        self.data_store.encoding = EncodingType(enc)

        self.use_cache_only = ds.get("use_cache_only", False)
        self.compress_tiles = ds.get("compress_tiles", False)


class DataSourceVector:
    __slots__ = (
        "id",
        "type",
        "data_store",
        "attribution",
        "description",
        "version",
        "minzoom",
        "maxzoom",
        "mbtiles",
        "pyr_settings",
        "buffer",
        "extent",
        "margin",
        "bounds",
        "center",
        "layers",
        "use_cache_only",
        "compress_tiles",
    )

    def __init__(self, ds: Dict[str, Any]):
        self.id: str = ds.get("id")

        data_type = ds.get("type")
        assert (
            data_type == DataType.vector
        ), f"Type of vector datasource must have '{DataType.vector}', but got '{data_type}'"
        self.type: DataType = DataType(data_type)

        data_store_dict = ds.get("dataStore")
        assert isinstance(
            data_store_dict, dict
        ), f"'dataStore' has wrong type: {data_store_dict}"

        store = data_store_dict.get("store")
        assert (
            store in STORE_TYPE
        ), f"'store' must have one of the values {STORE_TYPE}, but got '{store}'"
        self.data_store: DataStore = setup_data_store(store, data_store_dict)
        self.mbtiles = ds.get("mbtiles") or False

        pyr_settings: Optional[Dict[str, Any]] = ds.get("pyramidSettings")
        if pyr_settings is not None:
            self.pyr_settings = VectorTileSettings(pyr_settings)
        else:
            self.pyr_settings = VectorTileSettings({})

        self.attribution: Optional[str] = ds.get("attribution")
        self.description: Optional[str] = ds.get("description")
        self.version: Optional[str] = ds.get("version")

        self.minzoom: int = ds.get("minzoom") or MINZOOM
        self.maxzoom: int = ds.get("maxzoom") or MAXZOOM

        self.buffer: int = ds.get("buffer") or BUFFER
        self.extent: int = ds.get("extent") or EXTENT
        self.margin: str = f", margin => ({self.buffer}/{self.extent})"

        bounds = ds.get("bounds")
        if bounds is not None:
            assert (
                isinstance(bounds, dict) and len(bounds) == 4
            ), f"'bounds' has wrong type: {bounds}"
            self.bounds: DataSourceBounds = DataSourceBounds(**bounds)
        else:
            self.bounds = None

        center = ds.get("center")
        if center is not None:
            assert isinstance(center, list) and (
                len(center) == 3 or len(center) == 2
            ), f"'center' has wrong format: {center}"
            self.center: DataSourceCenter = DataSourceCenter(*center)
        else:
            self.center = None

        self.use_cache_only = ds.get("use_cache_only", False)
        self.compress_tiles = ds.get("compress_tiles", False)

        # Layers remain undefined for external source of tiles - DataStoreVectorTiles
        self.layers: Optional[List[VectorDataLayer]] = None

        # TODO: for now layers processing for internal store type
        if isinstance(self.data_store, DataStoreVectorInternal):
            layers = ds.get("layers")
            assert (
                isinstance(layers, list) and len(layers) > 0
            ), f"'layers' has wrong format: {layers}"

            self.layers = []
            for layer in layers:
                assert isinstance(layer, dict), f"'layer' has wrong format: {layer}"

                identifier: str = layer.get("id")
                layer_type: LayerType = LayerType(layer.get("type"))
                store_layer: str = layer.get("storeLayer")
                geom_field: str = layer.get("geomField")
                description: str = (
                    layer.get("description")
                    or f"{identifier}, {layer_type}, {store_layer}"
                )

                minzoom: int = layer.get("minzoom") or MINZOOM
                maxzoom: int = layer.get("maxzoom") or MAXZOOM
                simplify: bool = layer.get("simplify") or False
                filter: Optional[List[Any]] = layer.get("filter")
                fields: Optional[List[Field]] = None
                queries: Optional[List[LayerQuerySQL]] = None

                _fields: Optional[List[Field]] = layer.get("fields")
                if _fields is not None and isinstance(_fields, list):
                    fields = []
                    for _field in _fields:
                        assert isinstance(
                            _field, dict
                        ), f"'field' has wrong format: {_field}"
                        fields.append(Field(**_field))

                _queries: Optional[List[LayerQuerySQL]] = layer.get("queries")
                if _queries is not None and isinstance(_queries, list):
                    queries = []
                    for _query in _queries:
                        assert isinstance(
                            _query, dict
                        ), f"'query' has wrong format: {_query}"
                        queries.append(LayerQuerySQL(**_query))

                vector_layer: VectorDataLayer = VectorDataLayer(
                    identifier,
                    layer_type,
                    store_layer,
                    geom_field,
                    description,
                    minzoom,
                    maxzoom,
                    simplify,
                    filter,
                    fields,
                    queries,
                )
                self.layers.append(vector_layer)

        elif isinstance(self.data_store, DataStoreVectorTiles):
            pass
        elif isinstance(self.data_store, DataStoreVectorTileJson):
            pass
        elif isinstance(self.data_store, DataStoreVectorMBTiles):
            pass
        else:
            pass


DataSource = Union[DataSourceVector, DataSourceRaster]


async def load_datasources_from_db(
    app: FastAPI,
) -> Tuple[Optional[Dict[str, DataSource]], Optional[Dict[str, Dict[str, Any]]]]:
    dss_not_exception: Dict[str, Dict[str, Any]] = {}
    datasources: Dict[str, DataSource] = {}

    try:
        db_pool: Pool = app.state.db_pool
        connection: Connection
        async with db_pool.acquire() as connection:
            datasources_from_db: List[Any] = await connection.fetch(
                "SELECT * FROM datasource"
            )
    except Exception as e:
        message: str = (
            f"'load_datasources_from_db' error load DataSources from DB: {str(e)}"
        )
        logging.error(message)
        raise Exception(message)

    for ds in datasources_from_db:
        identifier: str = ds["identifier"]

        if isinstance(ds["data"], str):
            data: Dict[str, Any] = orjson.loads(ds["data"])
        elif isinstance(ds["data"], dict):
            data = ds["data"]

        if data["type"] == DataType.vector:
            try:
                ds_vector = DataSourceVector(data)
            except Exception as e:
                logger.error(
                    f"Error load Vector DataSource '{identifier}' from DB: {str(e)}"
                )
                continue

            if isinstance(ds_vector.data_store, DataStoreVectorInternal) and hasattr(
                app.state, "postgis_version"
            ):
                # Check version PostGIS/GEOS
                if (app.state.postgis_version[0] < 3) or (
                    app.state.postgis_version[0] == 3
                    and app.state.postgis_version[1] < 1
                ):
                    ds_vector.margin = ""
            if isinstance(ds_vector.data_store, DataStoreVectorTiles):
                app.state.datasource_keys[identifier] = ds_vector.data_store.keys

            datasources[identifier] = ds_vector
        elif data["type"] == DataType.raster:
            try:
                ds_raster = DataSourceRaster(data)
            except Exception as e:
                logger.error(
                    f"Error load Raster DataSource '{identifier}' from DB: {str(e)}"
                )
                continue
            datasources[identifier] = ds_raster
        else:
            logger.error(f'Unsupported type of DataSource: {data["type"]}')
            continue

        dss_not_exception[identifier] = ds

    return datasources, dss_not_exception
