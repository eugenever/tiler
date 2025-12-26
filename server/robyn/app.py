import json
import asyncio
import logging
import os
import sqlite3
import warnings
import rio_tiler as _

from pathlib import Path
from anyio import to_thread
from typing import Any, Dict, Optional, Union
from uuid import uuid4

from server.robyn import (
    Robyn,
    Request,
    Response,
    status_codes,
    jsonify,
)

from concurrent_log_handler import ConcurrentTimedRotatingFileHandler as _
from starlette.exceptions import HTTPException

from server.tile_utils import get_tile_job
from raster_tiles.single_tile.tile import mosaic_tile, tile
from server.pyramid_utils import (
    check_running_pyramid_for_dataset,
    json_to_tiles_options,
    json_to_mosaic_options,
    exist_dataset,
    exist_mosaic_dataset,
    run_mosaics_pyramid_in_threadpool,
    run_pyramid_in_threadpool,
    set_state_pyramid,
    bad_request,
)

app = Robyn(__file__)
logger = logging.getLogger(__name__)


@app.startup_handler
async def startup_handler() -> None:
    # Set the maximum number of worker threads
    to_thread.current_default_thread_limiter().total_tokens = 500


@app.get("/api/health")
async def health():
    pid = os.getpid()
    return f"Robyn process with ID {pid} is running"


@app.get("/api/tile/:dataset/:z/:x/:y_ext")  # type: ignore
async def get_tile(
    request: Request,
    router_dependencies: Dict[str, Any],
    global_dependencies: Dict[str, Any],
) -> Union[Dict[str, Any], Response]:
    headers_json = {
        "Content-type": "application/json",
        "Access-Control-Allow-Origin": "*",
    }

    if (
        "dataset" not in request.path_params
        or "z" not in request.path_params
        or "x" not in request.path_params
        or "y_ext" not in request.path_params
    ):
        return bad_request(request)

    try:
        if "dataset" in request.path_params:
            dataset = request.path_params.get("dataset")

        if "z" in request.path_params:
            z = int(request.path_params.get("z"))

        if "x" in request.path_params:
            x = int(request.path_params.get("x"))

        if "y_ext" in request.path_params:
            params = request.path_params.get("y_ext").split(".")
            y = int(params[0])
            ext = params[1]
    except Exception as e:
        logger.error(f"'get_tile': retrieving request parameters: {e}")
        return bad_request(request)

    # ignore warnings from rio-tiler, rasterio
    warnings.filterwarnings("ignore")

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
            parent_dir: Path = Path(__file__).parents[2]
            db_path: str = os.path.join(
                str(parent_dir), "data", dataset, f"{dataset}.db"
            )
            tile_job: Optional[sqlite3.Row] = await get_tile_job(db_path)
            modified_time = os.path.getmtime(db_path)
            global_dependencies[dataset] = {
                "tile_job": tile_job,
                "db_path": db_path,
                "modified_time": modified_time,
            }

        tile_bytes, nts = await tile(dataset, z, x, y, ext, None, tile_job)

        value_header_nts: Optional[str] = None
        if len(nts) > 1:
            value_header_nts = ",".join(f"{nt.t.x}_{nt.t.y}_{nt.t.z}" for nt in nts)
    except HTTPException as e:
        return Response(
            status_code=e.status_code,
            headers=headers_json,
            description=jsonify({"message": f"{e.detail}"}),
        )
    except Exception as e:
        logger.error(f"'get_tile': error generate tile, request '{request}'")
        return Response(
            status_code=status_codes.HTTP_500_INTERNAL_SERVER_ERROR,
            headers=headers_json,
            description=jsonify({"message": f"{e}"}),
        )

    if tile_bytes:
        headers = {
            "Content-type": "image/png",
            "Access-Control-Allow-Origin": "*",
            "Cache-Control": "max-age=0",
        }
        if value_header_nts:
            headers["Nts"] = value_header_nts
        return Response(
            status_code=status_codes.HTTP_200_OK,
            headers=headers,
            description=tile_bytes,
        )

    if tile_bytes is None and len(nts) > 1:
        if value_header_nts:
            headers_json["Nts"] = value_header_nts
        return Response(
            status_code=status_codes.HTTP_204_NO_CONTENT,
            headers=headers_json,
            description=jsonify(
                {
                    "message": f"Dataset: {dataset}, tile {x}:{y}:{z} is empty or out of bounds"
                }
            ),
        )

    body = jsonify(
        {
            "message": f"'get_tile': tile {x}:{y}:{z} for dataset '{dataset}' and its neighbors outside the raster boundaries"
        }
    )
    return Response(
        status_code=status_codes.HTTP_204_NO_CONTENT,
        description=body,
        headers=headers_json,
    )


@app.post("/api/pyramid")  # type: ignore
async def pyramid(
    request: Request,
    router_dependencies: Dict[str, Any],
    global_dependencies: Dict[str, Any],
) -> Union[Response, Dict[str, Any]]:
    json_body = json.loads(request.body)
    empty_headers = {"Access-Control-Allow-Origin": "*"}
    status_bad_request = status_codes.HTTP_400_BAD_REQUEST

    if "dataset" in json_body:
        dataset = json_body["dataset"]
    else:
        b = jsonify({"message": f"Dataset is undefined, request: {request}"})
        return Response(
            status_code=status_bad_request,
            headers=empty_headers,
            description=b,
        )

    is_exist_dataset = await exist_dataset(dataset)
    if not is_exist_dataset:
        b = jsonify({"message": f"Dataset '{dataset}' not found, request: '{request}'"})
        return Response(
            status_code=status_bad_request,
            headers=empty_headers,
            description=b,
        )

    running_pyramid_id: Optional[str] = await check_running_pyramid_for_dataset(dataset)
    if running_pyramid_id:
        # pyramid allready running
        headers = {
            "Content-type": "application/json",
            "Access-Control-Allow-Origin": "*",
        }

        b = jsonify({"pyramid_id": running_pyramid_id, "already_running": True})
        return Response(status_code=status_bad_request, headers=headers, description=b)

    try:
        options = json_to_tiles_options(json_body)
    except Exception as e:
        logger.error(f"'pyramid': validation error for request '{request.body}': {e}")
        message = jsonify({"message": f"{e}"})
        return bad_request(request, message)

    id = str(uuid4())
    asyncio.create_task(run_pyramid_in_threadpool(dataset, options, id))
    await set_state_pyramid(1, 0, id, options, dataset)

    body = jsonify({"pyramid_id": id, "already_running": False})
    return Response(
        status_code=status_codes.HTTP_202_ACCEPTED,
        headers={"Access-Control-Allow-Origin": "*"},
        description=body,
    )


@app.get("/api/mosaics/tile/:dataset/:z/:x/:y_ext")  # type: ignore
async def get_mosaics_tile(
    request: Request,
    router_dependencies: Dict[str, Any],
    global_dependencies: Dict[str, Any],
) -> Union[Dict[str, Any], Response]:

    headers_json = {
        "Content-type": "application/json",
        "Access-Control-Allow-Origin": "*",
    }

    if (
        "dataset" not in request.path_params
        or "z" not in request.path_params
        or "x" not in request.path_params
        or "y_ext" not in request.path_params
    ):
        return bad_request(request)

    try:
        if "dataset" in request.path_params:
            dataset = request.path_params.get("dataset")

        if "z" in request.path_params:
            z = int(request.path_params.get("z"))

        if "x" in request.path_params:
            x = int(request.path_params.get("x"))

        if "y_ext" in request.path_params:
            params = request.path_params.get("y_ext").split(".")
            y = int(params[0])
            ext = params[1]
    except Exception as e:
        logger.error(f"'get_mosaics_tile': retrieving request parameters: {e}")
        return bad_request(request)

    # ignore warnings from rio-tiler, rasterio
    warnings.filterwarnings("ignore")

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
            parent_dir: Path = Path(__file__).parents[2]
            db_path: str = os.path.join(
                str(parent_dir), "data", "mosaics", dataset, f"{dataset}.db"
            )
            tile_job: Optional[sqlite3.Row] = await get_tile_job(db_path)
            modified_time = os.path.getmtime(db_path)
            global_dependencies[dst] = {
                "tile_job": tile_job,
                "db_path": db_path,
                "modified_time": modified_time,
            }

        tile_bytes, nts = await mosaic_tile(dataset, z, x, y, ext, tile_job)
        value_header_nts: Optional[str] = None
        if len(nts) > 1:
            value_header_nts = ",".join(f"{nt.t.x}_{nt.t.y}_{nt.t.z}" for nt in nts)
    except HTTPException as e:
        return Response(
            status_code=e.status_code,
            headers=headers_json,
            description=jsonify({"message": f"{e.detail}"}),
        )
    except Exception as e:
        logger.error(
            f"'get_mosaics_tile': error generate mosaics tile, request '{request}'"
        )
        return Response(
            status_code=status_codes.HTTP_500_INTERNAL_SERVER_ERROR,
            headers=headers_json,
            description=jsonify({"message": f"{e}"}),
        )

    if tile_bytes:
        headers = {"Content-type": "image/png", "Access-Control-Allow-Origin": "*"}
        if value_header_nts:
            headers["Nts"] = value_header_nts
        return Response(
            status_code=status_codes.HTTP_200_OK,
            headers=headers,
            description=tile_bytes,
        )

    if tile_bytes is None and len(nts) > 1:
        if value_header_nts:
            headers_json["Nts"] = value_header_nts
        return Response(
            status_code=status_codes.HTTP_204_NO_CONTENT,
            headers=headers_json,
            description=jsonify(
                {
                    "message": f"Tile '{x}:{y}:{z}' for dataset '{dataset}' is empty or outside the raster boundaries"
                }
            ),
        )

    body = jsonify(
        {
            "message": f"'get_mosaics_tile': mosaic tile {x}:{y}:{z} for dataset '{dataset}' and its neighbors outside the raster boundaries"
        }
    )
    return Response(
        status_code=status_codes.HTTP_204_NO_CONTENT,
        description=body,
        headers=headers_json,
    )


@app.post("/api/mosaics/pyramid")  # type: ignore
async def mosaics_pyramid(
    request: Request,
    router_dependencies: Dict[str, Any],
    global_dependencies: Dict[str, Any],
) -> Union[Response, Dict[str, Any]]:
    json_body = json.loads(request.body)
    empty_headers = {"Access-Control-Allow-Origin": "*"}
    status_bad_request = status_codes.HTTP_400_BAD_REQUEST

    # dataset in the case of mosaics = assets_dir
    if "dataset" in json_body:
        dataset: str = json_body["dataset"]
    else:
        b = jsonify({"message": f"Dataset is undefined, request: {request}"})
        return Response(
            status_code=status_bad_request,
            headers=empty_headers,
            description=b,
        )

    is_exist_dataset = await exist_mosaic_dataset(dataset)
    if not is_exist_dataset:
        b = jsonify(
            {"message": f"Mosaics dataset '{dataset}' not found, request: '{request}'"}
        )
        return Response(
            status_code=status_bad_request,
            headers=empty_headers,
            description=b,
        )

    running_pyramid_id: Optional[str] = await check_running_pyramid_for_dataset(dataset)
    if running_pyramid_id:
        # pyramid allready running
        headers = {"Content-type": "application/json"}
        b = jsonify({"pyramid_id": running_pyramid_id, "already_running": True})
        return Response(status_code=status_bad_request, headers=headers, description=b)

    try:
        options = json_to_mosaic_options(dataset, json_body)
    except Exception as e:
        logger.error(
            f"'mosaics_pyramid': validation error for request '{request.body}': {e}"
        )
        message = jsonify({"message": f"{e}"})
        return bad_request(request, message)

    id = str(uuid4())
    asyncio.create_task(run_mosaics_pyramid_in_threadpool(dataset, options, id))
    await set_state_pyramid(1, 0, id, options, dataset)

    body = jsonify({"pyramid_id": id, "already_running": False})
    return Response(
        status_code=status_codes.HTTP_202_ACCEPTED,
        headers={"Access-Control-Allow-Origin": "*"},
        description=body,
    )
