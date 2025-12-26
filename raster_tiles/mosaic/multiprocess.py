import multiprocessing
import os
import sqlite3
import threading
import logging
import time
import queue
import warnings

from multiprocessing import Manager, Pipe
from multiprocessing.pool import Pool
from multiprocessing.context import SpawnProcess
from time import perf_counter as pc
from typing import Dict, List, Optional

from rio_tiler.io.rasterio import Reader

from raster_tiles.models.tile_job import TileJob
from raster_tiles.mosaic.preprocessing import (
    create_base_tile,
    processing_input_assets,
)
from raster_tiles.mosaic.mosaic_options import MosaicOptions
from raster_tiles.utils import DividedCache
from server.tile_utils import is_warped_raster_asset
from server.sqlite_db import sqlite_db_connect

import __main__

if not hasattr(__main__, "__spec__"):
    __main__.__spec__ = None


threadLocal = threading.local()
logger = logging.getLogger(__name__)


def save_mbtile(mosaic_options: MosaicOptions, q: multiprocessing.Queue):
    connection: sqlite3.Connection = sqlite_db_connect(mosaic_options.mbtiles_db)
    cursor: sqlite3.Cursor = connection.cursor()

    while True:
        try:
            result = q.get(timeout=10)
            # if result is not empty tile (not None) than save it's
            if result:
                if isinstance(result, str):
                    if result == "terminate":
                        break
                if isinstance(result, Exception):
                    logger.error(
                        f"Mosaics assets '{mosaic_options.assets_dir}': workers raised following exceptions: {result}"
                    )
                if mosaic_options.mbtiles:
                    # List of tuples[bytes, x, y, z]
                    if isinstance(result, list):
                        for res in result:
                            # tuple[bytes, x, y, z]
                            if isinstance(res, tuple):
                                try:
                                    cursor.execute(
                                        """INSERT OR IGNORE INTO tiles (zoom_level, tile_column, tile_row, tile_data) values (?, ?, ?, ?);""",
                                        (
                                            res[3],
                                            res[1],
                                            res[2],
                                            sqlite3.Binary(res[0]),
                                        ),
                                    )
                                    connection.commit()
                                except sqlite3.OperationalError:
                                    # Exception `database is locked`
                                    attempts = 10
                                    for i in range(0, attempts):
                                        time.sleep(0.2)
                                        try:
                                            cursor.execute(
                                                """INSERT OR IGNORE INTO tiles (zoom_level, tile_column, tile_row, tile_data) values (?, ?, ?, ?);""",
                                                (
                                                    res[3],
                                                    res[1],
                                                    res[2],
                                                    sqlite3.Binary(res[0]),
                                                ),
                                            )
                                            connection.commit()
                                        except Exception as exc:
                                            if i == (attempts - 1):
                                                logger.error(
                                                    f"After {attempts} attempts error save tiles in DB '{mosaic_options.mbtiles_db}': {exc}"
                                                )
                                                break
                                            continue
                                        break
                                except Exception as e:
                                    logger.error(
                                        f"Error save tiles in DB '{mosaic_options.mbtiles_db}': {str(e)}"
                                    )
            q.task_done()
        except queue.Empty:
            pass
        except multiprocessing.TimeoutError:
            pass
        except Exception as e:
            logger.error(f"Error get message from queue: {e}")
            raise Exception(e)

    cursor.close()
    connection.close()


def multi_process_mosaics_tiling(
    assets_dir: str, mosaic_options: MosaicOptions, pool: Pool
) -> Optional[MosaicOptions]:
    """Generate mosaics tiles with multiprocessing."""
    # Only for embedded Python on Windows
    # if platform.system() == "Windows":
    #     sys.executable = os.path.join(sys.exec_prefix, "python.exe")

    if mosaic_options.verbose:
        logger.info(f"Begin the tiles detail calculate for '{assets_dir}'")

    try:
        st = pc()
        # Preprocessing runs in a separate process
        mp_context = multiprocessing.get_context("spawn")
        receiver, sender = Pipe()
        preprocessing = mp_context.Process(
            target=processing_input_assets,
            args=(mosaic_options,),
            kwargs={"sender": sender},
        )
        preprocessing.start()
        tile_job: TileJob = receiver.recv()
        preprocessing.join()

        logger.info(f"Time preprocessing '{assets_dir}': {round(pc() - st, 3)} secs")

        if not tile_job:
            pool.terminate()
            return None
    except Exception as e:
        logger.error(f"'tile_job_details' function execution error: {e}")
        pool.terminate()
        return None

    if mosaic_options.verbose:
        logger.info(f"Tiles details calculate complete for '{assets_dir}'")

    logger.info(f"Generate tiles for '{assets_dir}'")

    warped_assets_files: List[str] = [
        os.path.join(mosaic_options.assets_dir, asset)
        for asset in os.listdir(mosaic_options.assets_dir)
        if is_warped_raster_asset(os.path.join(mosaic_options.assets_dir, asset))
    ]
    mosaic_options.warped_assets_files = warped_assets_files

    try:
        # separate process for saving to mbtiles
        if mosaic_options.mbtiles:
            manager = Manager()
            queue = manager.Queue()
            mp_context = multiprocessing.get_context("spawn")
            process_save_mbtiles = mp_context.Process(
                target=save_mbtile, args=[mosaic_options, queue]
            )
            process_save_mbtiles.start()
        else:
            queue = None
    except Exception as e:
        logger.error(f"Pool terminated, 'save_mbtile' function execution error: {e}")
        pool.terminate()
        return None

    tl = pc()

    try:
        for tz in range(tile_job.tmaxz, tile_job.tminz - 1, -1):
            for tile_detail in tile_job.tile_details[tz]:
                pool.apply_async(
                    create_base_tile,
                    (mosaic_options, warped_assets_files, tile_detail),
                    {"queue": queue},
                )
    except Exception as e:
        logger.error(f"Mosaics Pool execution error: {e}")
        queue.put("terminate")
        pool.terminate()
        return None

    pool.close()
    pool.join()

    if mosaic_options.mbtiles:
        queue.put("terminate")
        process_save_mbtiles.join()

    logger.info(
        f"Time generation mosaics tiles '{mosaic_options.assets_dir}': {round(pc() - tl, 3)} secs"
    )

    if mosaic_options.mbtiles:
        connection = sqlite_db_connect(mosaic_options.mbtiles_db)
        cursor = connection.cursor()
        cursor.execute("""PRAGMA journal_mode=DELETE""")
        connection.commit()
        connection.close()

    return mosaic_options


def generate_mosaics_tiles(assets_dir: str, options: MosaicOptions) -> None:
    with DividedCache(options.count_processes), multiprocessing.get_context(
        "spawn"
    ).Pool(processes=options.count_processes) as pool:
        warp_options = multi_process_mosaics_tiling(assets_dir, options, pool)

    # Remove source raster files (gdalwarp + gdal_translate + gdaladdo) after build pyramid of tiles
    # You can't delete it right away
    # It is necessary to wait for the mbtiles or tile files to be completely saved to disk
    # Otherwise in tile handler can may be exception since there is no place to get the tile from
    time.sleep(5)

    if options.remove_processing_raster_files and warp_options is not None:
        clean_warping_rasters(warp_options)


def process_mosaics_tiling_in_separate_processes(
    assets_dir: str, mosaic_options: MosaicOptions
) -> Optional[MosaicOptions]:
    """Generate mosaics tiles in separate processes"""
    # Only for embedded Python on Windows
    # if platform.system() == "Windows":
    #     sys.executable = os.path.join(sys.exec_prefix, "python.exe")

    if mosaic_options.verbose:
        logger.info(f"Begin the tiles detail calculate for '{assets_dir}'")

    mp_context = multiprocessing.get_context("spawn")
    manager = Manager()

    try:
        st = pc()
        # Preprocessing runs in a separate process
        receiver, sender = Pipe()
        preprocessing = mp_context.Process(
            target=processing_input_assets,
            args=(mosaic_options,),
            kwargs={"sender": sender},
        )
        preprocessing.start()
        tile_job: TileJob = receiver.recv()
        preprocessing.join()

        logger.info(f"Time preprocessing '{assets_dir}': {round(pc() - st, 3)} secs")

        if not tile_job:
            return None
    except Exception as e:
        logger.error(f"'processing_input_assets' function execution error: {e}")
        return None

    if mosaic_options.verbose:
        logger.info(f"Tiles details calculate complete for '{assets_dir}'")

    logger.info(f"Generate tiles for '{assets_dir}'")

    if not mosaic_options.merge:
        warped_assets_files: List[str] = [
            os.path.join(mosaic_options.data_dir, mosaic_options.datasource_id, asset)
            for asset in os.listdir(
                os.path.join(mosaic_options.data_dir, mosaic_options.datasource_id)
            )
            if is_warped_raster_asset(
                os.path.join(
                    mosaic_options.data_dir, mosaic_options.datasource_id, asset
                )
            )
        ]
        mosaic_options.nodata_default = tile_job.nodata
    else:
        warped_assets_files = [tile_job.merged_asset]
        mosaic_options.merged_asset = tile_job.merged_asset

    mosaic_options.warped_assets_files = warped_assets_files

    try:
        # separate process for saving to mbtiles
        if mosaic_options.mbtiles:
            out_queue = manager.Queue()
            process_save_mbtiles = mp_context.Process(
                target=save_mbtile, args=[mosaic_options, out_queue]
            )
            process_save_mbtiles.start()
        else:
            out_queue = None
    except Exception as e:
        logger.error(
            f"Mosaics Pool terminated, 'save_mbtile' function execution error: {e}"
        )
        return None

    processes: List[SpawnProcess] = []
    in_queues: List[queue.Queue] = []
    for _ in range(mosaic_options.count_processes):
        in_queue = manager.Queue(maxsize=5)
        p = mp_context.Process(
            target=create_base_tile_queue, args=[mosaic_options, in_queue, out_queue]
        )
        in_queues.append(in_queue)
        processes.append(p)

    [p.start() for p in processes]

    tl = pc()

    try:
        index = 0
        for tz in range(tile_job.tmaxz, tile_job.tminz - 1, -1):
            for tile_detail in tile_job.tile_details[tz]:
                if index > len(in_queues) - 1:
                    index = 0
                try:
                    in_queues[index].put((tile_detail,), timeout=180)
                except queue.Full:
                    logger.error(
                        f"Error put tile detail in queue index={index}, tz={tz}, tx={tile_detail.tx}, ty={tile_detail.ty}"
                    )
                index = index + 1
    except Exception as e:
        logger.error(f"Mosaics Pool execution error: {e}")
        out_queue.put("terminate")
        [p.terminate() for p in processes]
        return None

    [in_queue.put("terminate") for in_queue in in_queues]
    [p.join() for p in processes]

    if mosaic_options.mbtiles:
        out_queue.put("terminate")
        process_save_mbtiles.join()

    logger.info(
        f"Time generation mosaics tiles '{mosaic_options.assets_dir}': {round(pc() - tl, 3)} secs"
    )

    if mosaic_options.mbtiles:
        wal_file: str = f"{mosaic_options.mbtiles_db}-wal"
        shm_file: str = f"{mosaic_options.mbtiles_db}-shm"
        if not os.path.isfile(wal_file) and not os.path.isfile(shm_file):
            connection = sqlite_db_connect(mosaic_options.mbtiles_db)
            cursor = connection.cursor()
            cursor.execute("""PRAGMA journal_mode=DELETE""")
            connection.commit()
            connection.close()

    return mosaic_options


def generate_mosaics_tiles_in_separate_processes(
    assets_dir: str, options: MosaicOptions
) -> None:
    warnings.filterwarnings("ignore")

    with DividedCache(options.count_processes):
        warp_options = process_mosaics_tiling_in_separate_processes(assets_dir, options)

    # Remove source raster files (gdalwarp + gdal_translate + gdaladdo) after build pyramid of tiles
    # You can't delete it right away
    # It is necessary to wait for the mbtiles or tile files to be completely saved to disk
    # Otherwise in tile handler can may be exception since there is no place to get the tile from
    time.sleep(5)

    if options.remove_processing_raster_files and warp_options is not None:
        clean_warping_rasters(warp_options)


def create_base_tile_queue(
    mosaic_options: MosaicOptions,
    in_queue: Optional[multiprocessing.Queue],
    out_queue: Optional[multiprocessing.Queue],
):
    readers: Dict[str, Reader] = {}
    for asset in mosaic_options.warped_assets_files:
        readers[asset] = Reader(asset)

    while True:
        try:
            message = in_queue.get(timeout=1)
            if message and isinstance(message, tuple):
                create_base_tile(mosaic_options, readers, message[0], queue=out_queue)
            if message and isinstance(message, str):
                if message == "terminate":
                    break
            in_queue.task_done()
        except queue.Empty:
            pass
        except multiprocessing.TimeoutError:
            pass
        except Exception as e:
            logger.error(
                f"'create_base_tile_queue': error get message from in_queue: {e}"
            )
            break


def clean_warping_rasters(warp_options: MosaicOptions):
    warped_assets_files: List[str] = [
        os.path.join(warp_options.assets_dir, asset)
        for asset in os.listdir(warp_options.assets_dir)
        if is_warped_raster_asset(os.path.join(warp_options.assets_dir, asset))
    ]

    if warp_options.merged_asset is not None:
        # TODO: Don`t remove merged assets raster for dynamic tiling
        # warped_assets_files.extend(
        #     [warp_options.merged_asset, f"{warp_options.merged_asset}.ovr"]
        # )
        if warp_options.pixel_selection_method.__name__ == "MeanMethod":
            sum_raster = warp_options.merged_asset.replace("_MEAN", "_SUM")
            count_raster = warp_options.merged_asset.replace("_MEAN", "_COUNT")
            warped_assets_files.extend([sum_raster, count_raster])

    for warp_raster in warped_assets_files:
        if os.path.isfile(warp_raster):
            os.remove(warp_raster)

        overviews = warp_raster + ".ovr"
        if os.path.isfile(overviews):
            os.remove(overviews)
