import logging

from concurrent_log_handler import ConcurrentTimedRotatingFileHandler as _
from typing import Optional

from fastapi.responses import JSONResponse
from fastapi import APIRouter, BackgroundTasks, status, HTTPException, Request

from server.pyramid_utils import Pyramid
from server.datasources import DataSource, DataType
from server.fapi.utils import try_load_datasource_from_db
from server.fapi.raster.pyramid import (
    mosaics_pyramid,
    single_pyramid as raster_single_pyramid,
)
from server.fapi.vector.pyramid import single_pyramid as vector_single_pyramid

pyramids_router = APIRouter()
logger = logging.getLogger(__name__)


@pyramids_router.post("/pyramid", status_code=status.HTTP_202_ACCEPTED)
async def pyramid(
    request: Request, p: Pyramid, background_tasks: BackgroundTasks
) -> JSONResponse:
    ds: Optional[DataSource] = request.app.state.datasources.get(p.datasource_id)
    if ds is None:
        ds = await try_load_datasource_from_db(request, p.datasource_id)

    if ds.type == DataType.raster:
        if ds.mosaics:
            response: JSONResponse = await mosaics_pyramid(request, p, background_tasks)
        else:
            response = await raster_single_pyramid(request, p, background_tasks)
        return response
    elif ds.type == DataType.vector:
        response = await vector_single_pyramid(request, p, background_tasks)
        return response

    raise HTTPException(
        status_code=status.HTTP_400_BAD_REQUEST,
        detail=f"DataSource id '{p.datasource_id}', data type '{ds.type}' not implemented",
    )
