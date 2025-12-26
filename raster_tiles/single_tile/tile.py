import os
import logging
import sqlite3
import warnings
import asyncio

from typing import List, Optional, Tuple

from starlette.concurrency import run_in_threadpool
from starlette.exceptions import HTTPException
from rio_tiler.io.rasterio import Reader

from server.tile_utils import (
    NeighborTile,
    get_mosaic_tile,
    get_tile,
    get_tile_job,
    post_process_tiles,
)

logger = logging.getLogger(__name__)


# TODO: Only profile Mercator
async def tile(
    root_path: str,
    datasource_id: str,
    dataset: str,
    z: int,
    x: int,
    y: int,
    ext: str,
    mbtiles: bool,
    reader: Optional[Reader] = None,
    tile_job: Optional[sqlite3.Row] = None,
) -> Tuple[Optional[bytes], List[NeighborTile]]:
    # ignore warnings from rio-tiler, rasterio
    warnings.filterwarnings("ignore")

    db_path: str = os.path.join(root_path, "data", datasource_id, f"{dataset}.db")
    if tile_job is None:
        tile_job: Optional[sqlite3.Row] = await get_tile_job(db_path)

    # GDAL Translate and gdaladdo not finished, need wait
    if tile_job is None:
        logger.warn(f"GDAL Translate and gdaladdo not finished for '{dataset}'")
        raise HTTPException(
            status_code=523,
            detail=f"GDAL Translate and gdaladdo not finished for '{dataset}'",
        )

    tile_driver: str = tile_job[2]
    tilesize: int = tile_job[4]
    input_file: str = tile_job[9]
    is_has_alpha_band: int = tile_job[10]
    nodata: Optional[float] = tile_job[11]
    if nodata is None:
        nodata = -999999.0
    encode_to_rgba: int = tile_job[12]
    resampling_method: str = tile_job[14]

    mbtiles_db: str = os.path.join(
        f"{root_path}",
        "tiles",
        datasource_id,
        f"{datasource_id}.mbtiles",
    )

    # Create base tile from GDAL Dataset
    buffer_requested, nts = await run_in_threadpool(
        get_tile,
        datasource_id,
        input_file,
        dataset,
        x,
        y,
        z,
        tilesize,
        tile_driver,
        is_has_alpha_band,
        encode_to_rgba,
        nodata,
        ext,
        resampling_method,
        reader,
    )

    if buffer_requested is not None:
        asyncio.create_task(post_process_tiles(mbtiles, mbtiles_db, nts))
        return (buffer_requested, nts)

    if buffer_requested is None and len(nts) > 0:
        asyncio.create_task(post_process_tiles(mbtiles, mbtiles_db, nts))
        logger.info(
            f"'tile': data of tile {x}:{y}:{z} is full empty/nodata, dataset '{dataset}'"
        )
        return (buffer_requested, nts)

    file: str = os.path.basename(input_file)
    raise HTTPException(
        status_code=204,
        detail=f"Error get tile {x}:{y}:{z} for dataset '{file}'",
    )


async def mosaic_tile(
    root_path: str,
    datasource_id: str,
    dataset: str,
    z: int,
    x: int,
    y: int,
    ext: str,
    mbtiles: bool,
    tile_job: Optional[sqlite3.Row] = None,
) -> Tuple[Optional[bytes], List[NeighborTile]]:
    # ignore warnings from rio-tiler, rasterio
    warnings.filterwarnings("ignore")

    db_path: str = os.path.join(root_path, "data", datasource_id, f"{dataset}.db")
    if tile_job is None:
        tile_job: Optional[sqlite3.Row] = await get_tile_job(db_path)

    # GDAL Translate and gdaladdo not finished, need wait
    if tile_job is None:
        logger.warn(f"GDAL Translate and gdaladdo not finished for '{dataset}'")
        raise HTTPException(
            status_code=523,
            detail=f"GDAL Translate and gdaladdo not finished for '{dataset}'",
        )

    tile_driver: str = tile_job[2]
    tilesize: int = tile_job[4]
    nodata: Optional[float] = tile_job[11]
    if nodata is None:
        nodata = -999999.0
    encode_to_rgba: int = tile_job[12]
    pixel_selection_method: str = tile_job[13]
    resampling_method: str = tile_job[14]
    if tile_job[15] is not None:
        merge: bool = True if tile_job[15] == 1 else False
    else:
        merge = False

    mbtiles_db: str = os.path.join(
        f"{root_path}",
        "tiles",
        datasource_id,
        f"{datasource_id}.mbtiles",
    )

    # Create base tile from GDAL Dataset
    buffer_requested, nts = await run_in_threadpool(
        get_mosaic_tile,
        datasource_id,
        dataset,
        x,
        y,
        z,
        tilesize,
        tile_driver,
        encode_to_rgba,
        nodata,
        ext,
        pixel_selection_method,
        resampling_method,
        merge,
        False,
    )

    if buffer_requested:
        asyncio.create_task(post_process_tiles(mbtiles, mbtiles_db, nts))
        return (buffer_requested, nts)

    if buffer_requested is None and len(nts) > 0:
        asyncio.create_task(post_process_tiles(mbtiles, mbtiles_db, nts))
        logger.info(
            f"'mosaic_tile': data of tile {x}:{y}:{z} is full empty/nodata, dataset '{dataset}'"
        )
        return (buffer_requested, nts)

    raise HTTPException(
        status_code=204,
        detail=f"'mosaic_tile': mosaic tile {x}:{y}:{z} for dataset '{dataset}' and its neighbors outside the raster boundaries",
    )
