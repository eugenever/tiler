import json
import logging
import os
import sqlite3
import datetime as dt
import aiofiles.os as aos
import aiosqlite
import multiprocessing
import psutil

from pathlib import Path
from typing import Any, Dict, Optional, List, Union, Tuple

from pydantic import BaseModel, validator
from starlette.exceptions import HTTPException
from starlette.concurrency import run_in_threadpool
from fastapi import status

from raster_tiles.main import generate_tiles_in_separate_processes
from raster_tiles.models.tiles_options import TilesOptions
from raster_tiles.defaults import PIXEL_SELECTION_METHOD, RESAMPLING, Resampling
from raster_tiles.models.tile_driver import TileDriver
from raster_tiles.mosaic.mosaic_options import MosaicOptions, is_raster_asset
from raster_tiles.mosaic.multiprocess import (
    generate_mosaics_tiles_in_separate_processes,
)
from server.sqlite_db import sqlite_db_connect_async

from server.robyn import (
    Request,
    Response,
    status_codes,
    jsonify,
)

logger = logging.getLogger(__name__)


class Pyramid(BaseModel):
    verbose: Optional[bool] = False
    resampling: Optional[str] = "average"
    tiledriver: Optional[str] = "PNG"
    tile_size: Optional[int] = 256
    xyz: Optional[bool] = True
    datasource_id: str
    count_processes: Optional[int] = multiprocessing.cpu_count()
    zoom: Optional[Union[List[int], Tuple[int, int]]] = None
    mbtiles: Optional[bool] = True
    warnings: Optional[bool] = False
    save_tile_detail_db: Optional[bool] = True
    warp: Optional[bool] = False
    resampling_warp: Optional[str] = "average"
    remove_processing_raster_files: Optional[bool] = False
    encode_to_rgba: Optional[bool] = True
    mosaic_merge: Optional[bool] = False
    nodata_default: Optional[float] = -999999
    pixel_selection_method: Optional[str] = "FirstMethod"
    merge: Optional[bool] = True

    @validator("resampling")
    def validate_resampling(cls, value):
        if value not in [
            "average",
            "antialias",
            "nearest",
            "bilinear",
            "cubic",
            "cubicspline",
            "lanczos",
            "min",
            "max",
            "med",
        ]:
            raise ValueError(
                f"Pyramid.resampling must have one of the values ['average', 'antialias', 'near', 'bilinear', 'cubic', 'cubicspline', 'lanczos']"
            )
        return value

    @validator("resampling_warp")
    def validate_resampling_warp(cls, value):
        if value not in [
            "average",
            "antialias",
            "nearest",
            "bilinear",
            "cubic",
            "cubicspline",
            "lanczos",
            "min",
            "max",
            "med",
        ]:
            raise ValueError(
                f"Pyramid.resampling must have one of the values ['average', 'antialias', 'near', 'bilinear', 'cubic', 'cubicspline', 'lanczos', 'min', 'max', 'med']"
            )
        return value

    @validator("tiledriver")
    def validate_tiledriver(cls, value):
        if value not in ["PNG"]:
            raise ValueError(f"Pyramid.tiledriver must have one of the values [PNG]")
        return value

    @validator("tile_size")
    def validate_tile_size(cls, value):
        if value > 512 or value < 128:
            raise ValueError(
                f"Pyramid.tile_size must have one of the values [128, 256, 512]"
            )
        return value

    @validator("count_processes")
    def validate_count_processes(cls, value):
        cpus = multiprocessing.cpu_count()
        if value > cpus or value < 1:
            raise ValueError(
                f"Pyramid.count_processes must be in range '1-{cpus}', found '{value}'"
            )
        return value

    @validator("zoom")
    def validate_zoom(cls, value):
        if not isinstance(value, (list, tuple)):
            raise ValueError(
                f"Pyramid.zoom: must be List or Tuple. Got: '{type(value)}'"
            )
        if len(value) != 2:
            raise ValueError(
                f"Pyramid.zoom: length data zoom must be 2, found '{len(value)}'"
            )
        if value[0] < 0 or value[1] > 18:
            raise ValueError(
                f"Pyramid.zoom: values of zoom must be in range '0-18', found '{value}'"
            )
        return value

    @validator("pixel_selection_method")
    def validate_pixel_selection_method(cls, value):
        if value not in PIXEL_SELECTION_METHOD:
            raise ValueError(
                f"Pyramid.pixel_selection_method must have one of the values ['FirstMethod', 'HighestMethod', 'LowestMethod', 'MeanMethod']"
            )
        return value


async def set_state_pyramid(
    running: int,
    complete: int,
    id: str,
    root_path: str,
    options: Optional[Union[TilesOptions, MosaicOptions]] = None,
    dataset: Optional[str] = None,
    datasource_id: Optional[str] = None,
) -> None:
    tiler_db: str = os.path.join(root_path, "data", "tiler.db")

    if await aos.path.isfile(tiler_db):
        try:
            connection: aiosqlite.Connection = await sqlite_db_connect_async(tiler_db)
            connection.isolation_level = None
            cursor: aiosqlite.Cursor = await connection.cursor()
            await cursor.execute("begin")

            if running == 1 and complete == 0:
                start_time = dt.datetime.now()
                if options is not None:
                    if isinstance(options, TilesOptions):
                        params: str = options.toJSON()
                    if isinstance(options, MosaicOptions):
                        # 'pixel_selection_method' by default don't serialize to JSON
                        d = {
                            "resampling": options.resampling.name,
                            "tiledriver": options.tiledriver.name,
                            "zoom": options.zoom,
                            "tile_size": options.tile_size,
                            "tileext": options.tileext,
                            "xyz": options.XYZ,
                            "count_processes": options.count_processes,
                            "mbtiles": options.mbtiles,
                            "remove_processing_raster_files": options.remove_processing_raster_files,
                            "warnings": options.warnings,
                            "encode_to_rgba": options.encode_to_rgba,
                            "pixel_selection_method": options.pixel_selection_method.__name__,
                            "nodata_default": options.nodata_default,
                            "merge": options.merge,
                        }
                        params = json.dumps(d)
                else:
                    params = ""

                await cursor.execute(
                    "INSERT INTO pyramids (id, dataset, datasource_id, start_time, params, running, complete) values (?,?,?,?,?,?,?)",
                    (id, dataset, datasource_id, start_time, params, 1, 0),
                )

            elif running == 0 and complete == 1:
                await cursor.execute(
                    "UPDATE pyramids SET finish_time = ?, running = ?, complete = ? WHERE id = ?",
                    (dt.datetime.now(), running, complete, id),
                )

            await cursor.execute("commit")
            await cursor.close()
            await connection.close()
        except Exception as e:
            message: str = f"Error insert Pyramid into '{tiler_db}': {e}"
            logger.error(message)
            await cursor.execute("rollback")
            await cursor.close()
            await connection.close()
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=message
            )


async def check_running_pyramid_for_dataset(
    dataset: str, datasource_id: str, root_path: str
) -> Optional[str]:
    tiler_db: str = os.path.join(root_path, "data", "tiler.db")

    if await aos.path.isfile(tiler_db):
        try:
            connection: aiosqlite.Connection = await sqlite_db_connect_async(tiler_db)
            connection.isolation_level = None
            cursor: aiosqlite.Cursor = await connection.cursor()
            await cursor.execute("begin")

            cur_exec: aiosqlite.Cursor = await cursor.execute(
                "SELECT id FROM pyramids WHERE dataset = ? AND datasource_id = ? AND running = ? AND complete = ? ORDER BY start_time DESC LIMIT 1",
                (dataset, datasource_id, 1, 0),
            )
            running_pyramid: Optional[sqlite3.Row] = await cur_exec.fetchone()
            await cursor.execute("commit")
            await cursor.close()
            await connection.close()

            if running_pyramid:
                run_pyr: str = running_pyramid[0]
                return run_pyr

        except Exception as e:
            message: str = (
                f"Error select pyramids for dataset = '{dataset}', datasource_id = '{datasource_id}': {e}"
            )
            logger.error(message)
            await cursor.execute("rollback")
            await cursor.close()
            await connection.close()
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=message
            )

    return None


async def exist_dataset(dataset: str, root_path: str) -> bool:
    dataset_file: str = os.path.join(root_path, "data", dataset)
    if not await aos.path.isfile(dataset_file):
        return False
    return True


async def exist_mosaic_dataset(dataset: str, root_path: str) -> bool:
    dataset_dir: str = os.path.join(root_path, "data", "mosaics", dataset)

    if not await aos.path.isdir(dataset_dir):
        return False

    assets = await aos.listdir(dataset_dir)
    assets_files: List[str] = [
        os.path.join(dataset_dir, asset)
        for asset in assets
        if is_raster_asset(os.path.join(dataset_dir, asset))
    ]

    if len(assets_files) == 0:
        return False

    return True


async def exist_mosaic(mosaic: str) -> None:
    parent_dir: Path = Path(__file__).parents[2]
    mosaic_dir: str = os.path.join(str(parent_dir), "data", "mosaics", mosaic)

    if not await aos.path.isdir(mosaic_dir):
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Mosaic folder '../data/mosaics/{mosaic}' not found",
        )


def json_to_tiles_options(json_body: Dict[str, Any]) -> TilesOptions:
    options = TilesOptions()

    if "verbose" in json_body:
        v = json_body["verbose"]
        if not isinstance(v, bool):
            raise Exception(f"'verbose' must be bool, got '{v}'")
        options.verbose = v

    if "resampling" in json_body:
        r = json_body["resampling"]
        if not isinstance(r, str):
            raise Exception(f"'resampling' must be string, got '{r}'")
        if r not in RESAMPLING:
            raise Exception(f"'resampling' must have valid values, got '{r}'")
        options.resampling = Resampling[r]

    if "tiledriver" in json_body:
        td: str = json_body["tiledriver"]
        if td.lower() != "png":
            raise Exception(f"'tiledriver' must be 'PNG', got '{td}'")
        options.tiledriver = TileDriver[td]

    if "tile_size" in json_body:
        ts = json_body["tile_size"]
        if not isinstance(ts, int):
            raise Exception(f"'tile_size' must be int, got '{ts}'")
        if ts not in [128, 256, 512, 1024]:
            raise Exception(
                f"'tile_size' should take values [128, 256, 512, 1024], got '{ts}'"
            )
        options.tile_size = ts

    if "xyz" in json_body:
        options.XYZ = json_body["xyz"]

    if "count_processes" in json_body:
        cp = json_body["count_processes"]
        if not isinstance(cp, int):
            raise Exception(f"'count_processes' must be int, got '{cp}'")
        if cp < 0 or cp > multiprocessing.cpu_count() * 2:
            raise Exception(
                f"'count_processes' must be in range {0}-{multiprocessing.cpu_count()*2}, got '{cp}'"
            )
        options.count_processes = cp

    if "zoom" in json_body:
        zoom = json_body["zoom"]
        if not isinstance(zoom, (list, tuple)) or len(zoom) != 2:
            raise Exception(
                f"'zoom' must be list or tuple from two integers, got '{zoom}'"
            )
        options.zoom = zoom

    if "mbtiles" in json_body:
        options.mbtiles = json_body["mbtiles"]
    if "warnings" in json_body:
        options.warnings = json_body["warnings"]
    if "save_tile_detail_db" in json_body:
        options.save_tile_detail_db = json_body["save_tile_detail_db"]
    if "warp" in json_body:
        options.warp = json_body["warp"]

    if "resampling_warp" in json_body:
        rw = json_body["resampling_warp"]
        if not isinstance(rw, str):
            raise Exception(f"'resampling_warp' must be string, got '{rw}'")
        if rw not in RESAMPLING:
            raise Exception(f"'resampling_warp' must have valid values, got '{rw}'")
        options.resampling_warp = Resampling[rw]

    if "remove_processing_raster_files" in json_body:
        options.remove_processing_raster_files = json_body[
            "remove_processing_raster_files"
        ]
    if "encode_to_rgba" in json_body:
        options.encode_to_rgba = json_body["encode_to_rgba"]
    if "mosaic_merge" in json_body:
        options.mosaic_merge = json_body["mosaic_merge"]
    if "mosaic_merge_options" in json_body:
        options.mosaic_merge_options = json_body["mosaic_merge_options"]

    return options


def json_to_mosaic_options(assets_dir: str, json_body: Dict[str, Any]) -> MosaicOptions:

    if "zoom" in json_body:
        zoom = json_body["zoom"]
        if not isinstance(zoom, (list, tuple)) or len(zoom) != 2:
            raise Exception(
                f"'zoom' must be list or tuple from two integers, got '{zoom}'"
            )
        options = MosaicOptions(assets_dir, zoom)
    else:
        options = MosaicOptions(assets_dir)

    if "verbose" in json_body:
        v = json_body["verbose"]
        if not isinstance(json_body["verbose"], bool):
            raise Exception(f"'verbose' must be bool, got '{v}'")
        options.verbose = v

    if "resampling" in json_body:
        r = json_body["resampling"]
        if not isinstance(json_body["resampling"], str):
            raise Exception(f"'resampling' must be string, got '{r}'")
        if r not in RESAMPLING:
            raise Exception(f"'resampling' must have valid values, got '{r}'")
        options.resampling = Resampling[r]

    if "tiledriver" in json_body:
        td: str = json_body["tiledriver"]
        if td.lower() != "png":
            raise Exception(f"'tiledriver' must be 'PNG', got '{td}'")
        options.tiledriver = TileDriver[td]

    if "tile_size" in json_body:
        ts = json_body["tile_size"]
        if not isinstance(ts, int):
            raise Exception(f"'tile_size' must be int, got '{ts}'")
        if ts not in [128, 256, 512, 1024]:
            raise Exception(
                f"'tile_size' should take values [128, 256, 512, 1024], got '{ts}'"
            )
        options.tile_size = ts

    if "nodata" in json_body:
        nodata = json_body["nodata"]
        if not isinstance(nodata, int):
            raise Exception(f"'nodata_default' must be int, got '{nodata}'")
        options.nodata_default = nodata

    if "xyz" in json_body:
        options.XYZ = json_body["xyz"]

    if "count_processes" in json_body:
        cp = json_body["count_processes"]
        if not isinstance(cp, int):
            raise Exception(f"'count_processes' must be int, got '{cp}'")
        if cp < 0 or cp > multiprocessing.cpu_count() * 2:
            raise Exception(
                f"'count_processes' must be in range {0}-{multiprocessing.cpu_count()*2}, got '{cp}'"
            )
        options.count_processes = cp

    if "mbtiles" in json_body:
        options.mbtiles = json_body["mbtiles"]
    if "warnings" in json_body:
        options.warnings = json_body["warnings"]
    if "remove_processing_raster_files" in json_body:
        options.remove_processing_raster_files = json_body[
            "remove_processing_raster_files"
        ]
    if "encode_to_rgba" in json_body:
        options.encode_to_rgba = json_body["encode_to_rgba"]

    if "pixel_selection_method" in json_body:
        psm = json_body["pixel_selection_method"]
        if not isinstance(psm, str):
            raise Exception(f"'pixel_selection_method' must be string, got '{psm}'")
        if psm not in PIXEL_SELECTION_METHOD:
            raise Exception(
                f"'pixel_selection_method' must have valid values, got '{psm}'"
            )
        options.pixel_selection_method = PIXEL_SELECTION_METHOD[psm]

    if "merge" in json_body:
        options.merge = json_body["merge"]

    return options


def create_datasource_tiles_directory(datasource_id: str) -> str:
    datasource_dir: str = os.path.join(
        str(Path(__file__).parents[1]), "tiles", datasource_id
    )
    if not os.path.isdir(datasource_dir):
        os.makedirs(datasource_dir, exist_ok=True)

    return datasource_dir


def pyramid_to_tiles_options(p: Pyramid, dataset: str, root_path: str) -> TilesOptions:
    options = TilesOptions()

    options.datasource_id = p.datasource_id
    options.verbose = p.verbose
    options.resampling = Resampling[p.resampling]
    options.tiledriver = TileDriver[p.tiledriver]
    options.tile_size = p.tile_size
    options.XYZ = p.xyz
    options.count_processes = p.count_processes
    options.zoom = p.zoom
    options.mbtiles = p.mbtiles

    options.mbtiles_db = os.path.join(
        root_path,
        "tiles",
        p.datasource_id,
        f"{p.datasource_id}.mbtiles",
    )
    options.datasource_dir = create_datasource_tiles_directory(p.datasource_id)

    options.warnings = p.warnings
    options.save_tile_detail_db = p.save_tile_detail_db
    options.warp = p.warp
    options.resampling_warp = Resampling[p.resampling_warp]
    options.remove_processing_raster_files = p.remove_processing_raster_files
    options.encode_to_rgba = p.encode_to_rgba
    options.mosaic_merge = p.mosaic_merge

    return options


def pyramid_to_mosaics_options(
    p: Pyramid, mosaic_assets_dir: str, root_path: str
) -> MosaicOptions:
    options = MosaicOptions(mosaic_assets_dir, p.datasource_id, p.zoom)

    options.datasource_id = p.datasource_id
    options.verbose = p.verbose
    options.resampling = Resampling[p.resampling]
    options.tiledriver = TileDriver[p.tiledriver]
    options.tile_size = p.tile_size
    options.nodata_default = p.nodata_default
    options.XYZ = p.xyz
    options.count_processes = p.count_processes
    options.mbtiles = p.mbtiles

    options.mbtiles_db = os.path.join(
        root_path,
        "tiles",
        p.datasource_id,
        f"{p.datasource_id}.mbtiles",
    )
    options.datasource_dir = create_datasource_tiles_directory(p.datasource_id)

    options.warnings = p.warnings
    options.remove_processing_raster_files = p.remove_processing_raster_files
    options.encode_to_rgba = p.encode_to_rgba
    options.pixel_selection_method = PIXEL_SELECTION_METHOD[p.pixel_selection_method]
    options.merge = p.merge

    return options


async def run_pyramid_in_threadpool(
    dataset: str, options: TilesOptions, id: str, root_path: str
) -> None:
    try:
        await run_in_threadpool(generate_tiles_in_separate_processes, dataset, options)
    except Exception as e:
        await set_state_pyramid(0, 1, id, root_path)
        logger.error(f"Error generate tiles for '{dataset}' with id '{id}': {e}")
        terminate_child_processes()
    finally:
        await set_state_pyramid(0, 1, id, root_path)


async def run_mosaics_pyramid_in_threadpool(
    assets_dir: str,
    options: MosaicOptions,
    id: str,
    root_path: str,
) -> None:
    try:
        await run_in_threadpool(
            generate_mosaics_tiles_in_separate_processes, assets_dir, options
        )
    except Exception as e:
        await set_state_pyramid(0, 1, id, root_path)
        logger.error(
            f"Error generate mosaics tiles for '{assets_dir}' with id '{id}': {e}"
        )
        terminate_child_processes()
    finally:
        await set_state_pyramid(0, 1, id, root_path)


def bad_request(
    request: Request, message: Optional[str] = None
) -> Union[Response, Dict[str, Any]]:
    if message:
        b = message
    else:
        b = jsonify({"message": f"Invalid request '{request}'"})
    return Response(
        status_code=status_codes.HTTP_400_BAD_REQUEST,
        headers={
            "Content-type": "application/json",
            "Access-Control-Allow-Origin": "*",
        },
        description=b,
    )


def terminate_child_processes():
    current_process = psutil.Process()
    children = current_process.children(recursive=True)
    for child in children:
        for _child in child.children(recursive=True):
            try:
                _child.kill()
                logger.error(f"Terminate child process '{_child.pid}' after exception")
            except:
                pass
        try:
            child.kill()
            logger.error(f"Terminate child process '{child.pid}' after exception")
        except:
            pass
