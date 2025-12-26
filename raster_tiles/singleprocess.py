import logging
import multiprocessing

from time import perf_counter as pc
from typing import Optional
from multiprocessing import Manager, Pipe
from multiprocessing.pool import Pool

from raster_tiles.multiprocess import save_mbtile
from raster_tiles.models.tiles_options import TilesOptions
from raster_tiles.models.tile_job import TileJob
from raster_tiles.tiles_processing import create_base_tile, tile_job_details
from server.sqlite_db import sqlite_db_connect


logger = logging.getLogger(__name__)


def single_threaded_tiling(
    input_file: str, options: TilesOptions, pool: Pool
) -> Optional[TileJob]:
    """
    Keep a single threaded version that stays clear of multiprocessing, for platforms that would not
    support it
    """
    st = pc()
    if options.verbose:
        logger.info(f"Begin tiles details calculation for '{input_file}'")

    # Preprocessing runs in a separate process
    mp_context = multiprocessing.get_context("spawn")
    receiver, sender = Pipe()
    preprocessing = mp_context.Process(
        target=tile_job_details, args=(input_file, options), kwargs={"sender": sender}
    )
    preprocessing.start()
    tile_job: TileJob = receiver.recv()
    preprocessing.join()

    logger.info(f"Time preprocessing '{input_file}': {round(pc() - st, 3)} secs")

    if not tile_job:
        return None

    if options.verbose:
        logger.info(f"Tiles details calculation complete for '{input_file}'")

    logger.info(f"Generate tiles for '{input_file}'")

    # separate process for saving to mbtiles
    if tile_job.options.mbtiles:
        manager = Manager()
        queue = manager.Queue()
        process_save_mbtiles = mp_context.Process(
            target=save_mbtile, args=[tile_job.options, input_file, queue]
        )
        process_save_mbtiles.start()
    else:
        queue = None

    tl = pc()
    for tz in range(tile_job.tmaxz, tile_job.tminz - 1, -1):
        for tile_detail in tile_job.tile_details[tz]:
            pool.apply_async(
                create_base_tile,
                (tile_job, tile_detail),
                {"queue": queue},
            )

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
