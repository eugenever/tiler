import logging

from concurrent_log_handler import ConcurrentTimedRotatingFileHandler as _
from uuid import uuid4
from typing import Optional
from fastapi import BackgroundTasks, HTTPException, Request, status
from fastapi.responses import JSONResponse

from server.pyramid_utils import (
    Pyramid,
    check_running_pyramid_for_dataset,
    set_state_pyramid,
    exist_dataset,
    exist_mosaic_dataset,
    pyramid_to_tiles_options,
    pyramid_to_mosaics_options,
    run_pyramid_in_threadpool,
    run_mosaics_pyramid_in_threadpool,
)
from server.datasources import DataSource, RasterTileSettings


logger = logging.getLogger(__name__)


async def single_pyramid(
    request: Request, p: Pyramid, background_tasks: BackgroundTasks
) -> JSONResponse:
    ds: DataSource = request.app.state.datasources.get(p.datasource_id)
    pyr_settings_to_pyramid(ds.pyr_settings, p)
    root_path: str = request.app.state.root_path
    is_exist_dataset = await exist_dataset(ds.data_store.file, root_path)
    if not is_exist_dataset:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Dataset '{ds.data_store.file}' not found",
        )

    running_pyramid_id: Optional[str] = await check_running_pyramid_for_dataset(
        ds.data_store.file, p.datasource_id, root_path
    )
    if running_pyramid_id:
        # pyramid allready running
        return {"pyramid_id": running_pyramid_id, "already_running": True}

    try:
        options = pyramid_to_tiles_options(p, ds.data_store.dataset, root_path)
    except Exception as e:
        logger.error(f"'single_pyramid': validation error: {e}")
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"'pyramid': validation error: {e}",
        )

    id = str(uuid4())
    background_tasks.add_task(
        run_pyramid_in_threadpool, ds.data_store.file, options, id, root_path
    )
    await set_state_pyramid(
        1, 0, id, root_path, options, ds.data_store.file, p.datasource_id
    )

    return JSONResponse(
        content={"pyramid_id": id, "already_running": False},
        status_code=status.HTTP_202_ACCEPTED,
    )


async def mosaics_pyramid(
    request: Request, p: Pyramid, background_tasks: BackgroundTasks
) -> JSONResponse:
    ds: DataSource = request.app.state.datasources.get(p.datasource_id)
    pyr_settings_to_pyramid(ds.pyr_settings, p)
    root_path: str = request.app.state.root_path
    is_exist_dataset = await exist_mosaic_dataset(ds.data_store.folder, root_path)
    if not is_exist_dataset:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Dataset '{ds.data_store.folder}' not found",
        )

    running_pyramid_id: Optional[str] = await check_running_pyramid_for_dataset(
        ds.data_store.folder, p.datasource_id, root_path
    )
    if running_pyramid_id:
        # pyramid allready running
        return {"pyramid_id": running_pyramid_id, "already_running": True}

    try:
        options = pyramid_to_mosaics_options(p, ds.data_store.folder, root_path)
    except Exception as e:
        logger.error(f"'mosaics_pyramid': validation error: {e}")
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"'mosaics_pyramid': validation error: {e}",
        )

    id = str(uuid4())
    background_tasks.add_task(
        run_mosaics_pyramid_in_threadpool, ds.data_store.folder, options, id, root_path
    )
    await set_state_pyramid(
        1, 0, id, root_path, options, ds.data_store.folder, p.datasource_id
    )

    return JSONResponse(
        content={"pyramid_id": id, "already_running": False},
        status_code=status.HTTP_202_ACCEPTED,
    )


def pyr_settings_to_pyramid(ts: RasterTileSettings, p: Pyramid):
    p.mbtiles = ts.mbtiles
    p.resampling = ts.resampling
    p.tiledriver = ts.tiledriver
    p.tile_size = ts.tile_size
    p.xyz = ts.xyz
    p.count_processes = ts.count_processes
    p.zoom = [ts.minzoom, ts.maxzoom]
    p.mbtiles = ts.mbtiles
    p.warnings = ts.warnings
    p.save_tile_detail_db = ts.save_tile_detail_db
    p.warp = ts.warp
    p.resampling_warp = ts.resampling_warp
    p.remove_processing_raster_files = ts.remove_processing_raster_files
    p.encode_to_rgba = ts.encode_to_rgba
    p.mosaic_merge = ts.mosaic_merge
    p.nodata_default = ts.nodata_default
    p.pixel_selection_method = ts.pixel_selection_method
    p.merge = ts.merge
