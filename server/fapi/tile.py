import logging
import orjson

from typing import Optional
from fastapi import APIRouter, Response, Request, BackgroundTasks, HTTPException, status

from server.datasources import DataSource, DataType
from server.fapi.raster.tile import raster_tile
from server.fapi.vector.tile import vector_tile
from server.fapi.utils import try_load_datasource_from_db

tiles_router = APIRouter()
logger = logging.getLogger(__name__)


@tiles_router.get("/tile/{datasource_id}/{z}/{x}/{y}.{ext}", response_class=Response)
async def get_tile(
    request: Request,
    datasource_id: str,
    z: int,
    x: int,
    y: int,
    ext: str,
    background_tasks: BackgroundTasks,
) -> Response:
    ds: Optional[DataSource] = request.app.state.datasources.get(datasource_id)
    if ds is None:
        ds = await try_load_datasource_from_db(request, datasource_id)

    # check DataSource zoom range
    if z > ds.maxzoom or z < ds.minzoom:
        return Response(
            status_code=status.HTTP_400_BAD_REQUEST,
            media_type="application/json",
            content=orjson.dumps(
                {
                    "message": f"Zoom should be in range {ds.minzoom}-{ds.maxzoom}, got {z}"
                }
            ),
        )

    try:
        root_path: str = request.app.state.root_path
        if ds.type == DataType.raster:
            response = await raster_tile(
                request, datasource_id, z, x, y, ext, root_path
            )
        elif ds.type == DataType.vector:
            response = await vector_tile(
                request, datasource_id, z, x, y, ext, root_path, background_tasks
            )
        return response
    except HTTPException as http_exc:
        raise HTTPException(status_code=http_exc.status_code, detail=http_exc.detail)
    except Exception as e:
        message: str = f"Error 'get_tile' {z}/{x}/{y}.{ext}: {str(e)}"
        logger.error(message)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=message
        )
