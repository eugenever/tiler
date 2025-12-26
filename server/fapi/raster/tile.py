import os
import logging
import sqlite3
import warnings

import aiofiles.os as aios
from typing import Optional

from fastapi import Response, Request, status
from starlette.exceptions import HTTPException

from raster_tiles.single_tile.tile import mosaic_tile, tile
from server.tile_utils import get_tile_job
from server.datasources import DataSource, DataStoreRaster

logger = logging.getLogger(__name__)


async def raster_tile(
    request: Request,
    datasource_id: str,
    z: int,
    x: int,
    y: int,
    ext: str,
    root_path: str,
) -> Response:
    ds: DataSource = request.app.state.datasources.get(datasource_id)
    data_store: DataStoreRaster = ds.data_store
    if ds.mosaics:
        tile: Response = await get_mosaics_tile(
            datasource_id,
            request,
            data_store.dataset,
            z,
            x,
            y,
            ext,
            ds.mbtiles,
            root_path,
        )
    else:
        tile: Response = await get_tile(
            datasource_id,
            request,
            data_store.dataset,
            z,
            x,
            y,
            ext,
            ds.mbtiles,
            root_path,
        )

    return tile


async def get_tile(
    datasource_id: str,
    request: Request,
    dataset: str,
    z: int,
    x: int,
    y: int,
    ext: str,
    mbtiles: bool,
    root_path: str,
) -> Response:
    # ignore warnings from rio-tiler, rasterio
    warnings.filterwarnings("ignore")
    global_dependencies = request.app.state.global_dependencies
    try:
        if dataset in global_dependencies:
            if isinstance(global_dependencies[dataset], dict):
                db_path: str = global_dependencies[dataset]["db_path"]
                modified_time_from_cache: float = global_dependencies[dataset][
                    "modified_time"
                ]
                modified_time: float = os.path.getmtime(db_path)
                # if db of dataset change than update tile_job
                if modified_time != modified_time_from_cache:
                    tile_job_update: Optional[sqlite3.Row] = await get_tile_job(db_path)
                    global_dependencies[dataset]["tile_job"] = tile_job_update
                    tile_job: Optional[sqlite3.Row] = tile_job_update
                    global_dependencies[dataset]["modified_time"] = modified_time
                else:
                    tile_job: Optional[sqlite3.Row] = global_dependencies[dataset][
                        "tile_job"
                    ]
        else:
            db_path: str = os.path.join(
                root_path, "data", datasource_id, f"{dataset}.db"
            )

            if not await aios.path.isfile(db_path):
                return Response(
                    content=f"Database file for datasource '{datasource_id}' not found",
                    status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                )

            tile_job: Optional[sqlite3.Row] = await get_tile_job(db_path)
            modified_time = os.path.getmtime(db_path)
            global_dependencies[dataset] = {
                "tile_job": tile_job,
                "db_path": db_path,
                "modified_time": modified_time,
            }

        tile_bytes, nts = await tile(
            request.app.state.root_path,
            datasource_id,
            dataset,
            z,
            x,
            y,
            ext,
            mbtiles,
            None,
            tile_job,
        )
    except HTTPException as e:
        return Response(status_code=e.status_code, content=e.detail)
    except Exception as e:
        logger.error(f"'get_tile': error generate tile - {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"{e}"
        )

    if tile_bytes:
        return Response(content=tile_bytes, media_type="image/png")

    if tile_bytes is None and len(nts) > 1:
        value_header_nts = ",".join(f"{nt.t.x}_{nt.t.y}_{nt.t.z}" for nt in nts)
        return Response(
            status_code=status.HTTP_204_NO_CONTENT,
            headers={"Nts": value_header_nts},
        )

    return Response(status_code=status.HTTP_204_NO_CONTENT)


async def get_mosaics_tile(
    datasource_id: str,
    request: Request,
    dataset: str,
    z: int,
    x: int,
    y: int,
    ext: str,
    mbtiles: bool,
    root_path: str,
) -> Response:
    # ignore warnings from rio-tiler, rasterio
    warnings.filterwarnings("ignore")
    global_dependencies = request.app.state.global_dependencies
    try:
        dst = f"mosaics_{dataset}"
        if dst in global_dependencies:
            if isinstance(global_dependencies[dst], dict):
                db_path: str = global_dependencies[dst]["db_path"]
                modified_time_from_cache: float = global_dependencies[dst][
                    "modified_time"
                ]
                modified_time: float = os.path.getmtime(db_path)
                # if db of dataset change than update tile_job
                if modified_time != modified_time_from_cache:
                    tile_job_update: Optional[sqlite3.Row] = await get_tile_job(db_path)
                    global_dependencies[dst]["tile_job"] = tile_job_update
                    tile_job: Optional[sqlite3.Row] = tile_job_update
                    global_dependencies[dst]["modified_time"] = modified_time
                else:
                    tile_job: Optional[sqlite3.Row] = global_dependencies[dst][
                        "tile_job"
                    ]
        else:
            db_path: str = os.path.join(
                root_path, "data", datasource_id, f"{dataset}.db"
            )
            if not await aios.path.isfile(db_path):
                return Response(
                    content=f"Database file for datasource '{datasource_id}' not found",
                    status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                )

            tile_job: Optional[sqlite3.Row] = await get_tile_job(db_path)
            modified_time = os.path.getmtime(db_path)
            global_dependencies[dst] = {
                "tile_job": tile_job,
                "db_path": db_path,
                "modified_time": modified_time,
            }

        tile_bytes, nts = await mosaic_tile(
            request.app.state.root_path,
            datasource_id,
            dataset,
            z,
            x,
            y,
            ext,
            mbtiles,
            tile_job,
        )
    except HTTPException as e:
        return Response(status_code=e.status_code, content=e.detail)
    except Exception as e:
        logger.error(f"'get_mosaics_tile': error generate tile - {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"{e}"
        )

    if tile_bytes:
        return Response(content=tile_bytes, media_type="image/png")

    if tile_bytes is None and len(nts) > 1:
        value_header_nts = ",".join(f"{nt.t.x}_{nt.t.y}_{nt.t.z}" for nt in nts)
        return Response(
            status_code=status.HTTP_204_NO_CONTENT,
            headers={"Nts": value_header_nts},
        )

    return Response(status_code=status.HTTP_204_NO_CONTENT)
