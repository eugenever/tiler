import multiprocessing
import sqlite3
import threading
import logging
import time
import queue
import os

from multiprocessing import Manager, Pipe
from multiprocessing.pool import Pool
from multiprocessing.context import SpawnProcess
from time import perf_counter as pc
from typing import List, Optional

from raster_tiles.tiles_processing import create_base_tile, tile_job_details
from raster_tiles.models.tiles_options import TilesOptions
from raster_tiles.models.tile_job import TileJob
from server.sqlite_db import sqlite_db_connect

import __main__

if not hasattr(__main__, "__spec__"):
    __main__.__spec__ = None


threadLocal = threading.local()
logger = logging.getLogger(__name__)


def save_mbtile(options: TilesOptions, input_file: str, q: multiprocessing.Queue):
    connection: sqlite3.Connection = sqlite_db_connect(options.mbtiles_db)
    cursor: sqlite3.Cursor = connection.cursor()

    while True:
        try:
            result = q.get(timeout=10)
            if result:
                if isinstance(result, str):
                    if result == "terminate":
                        break
                if isinstance(result, Exception):
                    logger.error(
                        f"Input raster '{input_file}': workers raised following exceptions: {result}"
                    )
                if options.mbtiles:
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
                                                    f"After {attempts} attempts error save tiles in DB '{options.mbtiles_db}': {exc}"
                                                )
                                                break
                                            continue
                                        break
                                except Exception as e:
                                    logger.error(
                                        f"Error save tiles in DB '{options.mbtiles_db}': {str(e)}"
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


def multi_process_tiling(
    input_file: str, options: TilesOptions, pool: Pool
) -> Optional[TileJob]:
    """Generate tiles with multiprocessing."""
    # Only for embedded Python on Windows
    # if platform.system() == "Windows":
    #     sys.executable = os.path.join(sys.exec_prefix, "python.exe")

    if options.verbose:
        logger.info(f"Begin the tiles detail calculate for '{input_file}'")

    try:
        st = pc()
        # Preprocessing runs in a separate process
        mp_context = multiprocessing.get_context("spawn")
        receiver, sender = Pipe()
        preprocessing = mp_context.Process(
            target=tile_job_details,
            args=(input_file, options),
            kwargs={"sender": sender},
        )
        preprocessing.start()
        tile_job: TileJob = receiver.recv()
        preprocessing.join()

        logger.info(f"Time preprocessing '{input_file}': {round(pc() - st, 3)} secs")

        if not tile_job:
            pool.terminate()
            return None
    except Exception as e:
        logger.error(f"'tile_job_details' function execution error: {e}")
        pool.terminate()
        return None

    if options.verbose:
        logger.info(f"Tiles detail calculate complete for '{input_file}'")

    logger.info(f"Generate tiles for '{input_file}'")

    try:
        # separate process for saving to mbtiles
        if tile_job.options.mbtiles:
            manager = Manager()
            queue = manager.Queue()
            mp_context = multiprocessing.get_context("spawn")
            process_save_mbtiles = mp_context.Process(
                target=save_mbtile, args=[tile_job.options, input_file, queue]
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
                    (tile_job, tile_detail),
                    {"queue": queue},
                )
    except Exception as e:
        logger.error(f"Pool execution error: {e}")
        queue.put("terminate")
        pool.terminate()
        return None

    pool.close()
    pool.join()

    if tile_job.options.mbtiles:
        queue.put("terminate")
        process_save_mbtiles.join()

    logger.info(f"Time generation tiles '{input_file}': {round(pc() - tl, 3)} secs")

    if tile_job.options.mbtiles:
        connection = sqlite_db_connect(tile_job.options.mbtiles_db)
        cursor = connection.cursor()
        cursor.execute("""PRAGMA journal_mode=DELETE""")
        connection.commit()
        connection.close()

    return tile_job


def workers_successful(results):
    while True:
        time.sleep(0.2)
        try:
            ready = [r.ready() for r in results]
            successful = [r.successful() for r in results]
        except Exception:
            continue
        if all(successful):
            break
        if all(ready) and not all(successful):
            raise Exception(
                f"Workers raised following exceptions {[r._value for r in results if not r.successful()]}"
            )


def process_tiling_in_separate_processes(
    input_file: str, options: TilesOptions
) -> Optional[TileJob]:
    """Generate tiles with custom separate processes"""

    if options.verbose:
        logger.info(f"Begin the tiles detail calculate for '{input_file}'")

    # strictly 'spawn' processes
    mp_context = multiprocessing.get_context("spawn")
    manager = Manager()

    try:
        st = pc()
        # Preprocessing runs in a separate process
        receiver, sender = Pipe()
        preprocessing = mp_context.Process(
            target=tile_job_details,
            args=(input_file, options),
            kwargs={"sender": sender},
        )
        preprocessing.start()
        tile_job: TileJob = receiver.recv()
        preprocessing.join()

        logger.info(f"Time preprocessing '{input_file}': {round(pc() - st, 3)} secs")

        if not tile_job:
            return None
    except Exception as e:
        logger.error(f"'tile_job_details' function execution error: {e}")
        return None

    if options.verbose:
        logger.info(f"Tiles detail calculate complete for '{input_file}'")

    logger.info(f"Generate tiles for '{input_file}'")

    try:
        # separate process for saving to mbtiles
        if tile_job.options.mbtiles:
            out_queue = manager.Queue()
            process_save_mbtiles = mp_context.Process(
                target=save_mbtile, args=[tile_job.options, input_file, out_queue]
            )
            process_save_mbtiles.start()
        else:
            out_queue = None
    except Exception as e:
        logger.error(f"Pool terminated, 'save_mbtile' function execution error: {e}")
        return None

    processes: List[SpawnProcess] = []
    in_queues: List[queue.Queue] = []
    for _ in range(options.count_processes):
        in_queue = manager.Queue(maxsize=5)
        p = mp_context.Process(
            target=create_base_tile_queue, args=[in_queue, out_queue]
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
                    in_queues[index].put((tile_job, tile_detail), timeout=180)
                except queue.Full:
                    logger.error(
                        f"Error put tile detail in queue index={index}, tz={tz}, tx={tile_detail.tx}, ty={tile_detail.ty}"
                    )
                index = index + 1

    except Exception as e:
        logger.error(f"Pool execution error: {e}")
        out_queue.put("terminate")
        [p.terminate() for p in processes]
        return None

    [in_queue.put("terminate") for in_queue in in_queues]
    [p.join() for p in processes]

    if tile_job.options.mbtiles:
        out_queue.put("terminate")
        process_save_mbtiles.join()

    logger.info(f"Time generation tiles '{input_file}': {round(pc() - tl, 3)} secs")

    if tile_job.options.mbtiles:
        wal_file: str = f"{tile_job.options.mbtiles_db}-wal"
        shm_file: str = f"{tile_job.options.mbtiles_db}-shm"
        if not os.path.isfile(wal_file) and not os.path.isfile(shm_file):
            connection = sqlite_db_connect(tile_job.options.mbtiles_db)
            cursor = connection.cursor()
            cursor.execute("""PRAGMA journal_mode=DELETE""")
            connection.commit()
            connection.close()

    return tile_job


def create_base_tile_queue(
    in_queue: Optional[multiprocessing.Queue],
    out_queue: Optional[multiprocessing.Queue],
):
    while True:
        try:
            message = in_queue.get(timeout=1)
            if message and isinstance(message, tuple):
                create_base_tile(message[0], message[1], queue=out_queue)
            if message and isinstance(message, str):
                if message == "terminate":
                    break
            in_queue.task_done()
        except queue.Empty:
            pass
        except multiprocessing.TimeoutError:
            pass
        except Exception as e:
            logger.error(f"Error get message from in_queue: {e}")
            break
