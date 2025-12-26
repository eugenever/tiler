import os
import logging
import asyncio
import aiosqlite
import sqlite3
import aiofiles.os as aio_os
import multiprocessing
import copy
import morecantile
import asyncpg
import math
import gzip

from asyncpg.pool import Pool
from itertools import islice
from concurrent_log_handler import ConcurrentTimedRotatingFileHandler as _
from uuid import uuid4
from typing import List, Optional, Iterator, Generator, Tuple, Any, Iterable, Set
from asyncio import Task

from bestconfig import Config
from morecantile.commons import Tile
from starlette.concurrency import run_in_threadpool
from fastapi import BackgroundTasks, HTTPException, Request, status
from fastapi.responses import JSONResponse

from server.fapi.db import dsn_postgresql, set_connection_type_codec
from server.fapi.utils import initialize_event_loop
from server.fapi.vector.mvt_postgis import generate_mvt
from server.datasources import DataSource, VECTOR, StoreType
from server.pyramid_utils import (
    Pyramid,
    check_running_pyramid_for_dataset,
    set_state_pyramid,
    terminate_child_processes,
)
from server.sqlite_db import sqlite_db_connect_async
from server.tile_utils import save_tile_on_disk
from server.mbtiles import async_mbtiles_setup


logger = logging.getLogger(__name__)


async def single_pyramid(
    request: Request, p: Pyramid, background_tasks: BackgroundTasks
) -> JSONResponse:
    ds: DataSource = request.app.state.datasources.get(p.datasource_id)
    if ds.data_store.store != StoreType.internal:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Creating a pyramid of vector tiles is only possible for the internal type store. Got '{ds.data_store.store}'",
        )

    root_path: str = request.app.state.root_path
    running_pyramid_id: Optional[str] = await check_running_pyramid_for_dataset(
        VECTOR, p.datasource_id, root_path
    )
    if running_pyramid_id:
        # pyramid allready running
        return {"pyramid_id": running_pyramid_id, "already_running": True}

    id_pyramid = str(uuid4())
    await set_state_pyramid(1, 0, id_pyramid, root_path, None, VECTOR, p.datasource_id)

    background_tasks.add_task(
        run_vector_pyramid_in_threadpool,
        ds,
        p.datasource_id,
        root_path,
        id_pyramid,
    )

    return JSONResponse(
        content={"pyramid_id": id_pyramid, "already_running": False},
        status_code=status.HTTP_202_ACCEPTED,
    )


def batched(
    iterable: Iterable[Tile], count_processes: int
) -> Generator[Tuple[Tile], Any, None]:
    "Batch data into tuples of length n. The last batch may be shorter."
    # batched('ABCDEFG', 3) --> ABC DEF G
    if count_processes < 1:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Count processes must be at least one, got {count_processes}",
        )
    it = iter(iterable)
    while batch := tuple(islice(it, count_processes)):
        yield batch


def tiles_iterator(bounds: List[float], minzoom: int, maxzoom: int) -> Iterator[Tile]:
    tms = morecantile.tms.get("WebMercatorQuad")
    tiles: Iterator[Tile] = tms.tiles(*bounds, zooms=range(minzoom, maxzoom + 1))
    return tiles


async def run_vector_pyramid_in_threadpool(
    ds: DataSource,
    datasource_id: str,
    root_path: str,
    id_pyramid: str,
) -> None:
    try:
        await run_in_threadpool(
            vector_pyramid_in_separate_process,
            ds,
            datasource_id,
            root_path,
            id_pyramid,
        )
    except Exception as e:
        await set_state_pyramid(0, 1, id_pyramid, root_path)
        logger.error(
            f"'run_vector_pyramid_in_threadpool': error generate vector tiles for '{datasource_id}' with id '{id_pyramid}': {e}"
        )
        terminate_child_processes()
    finally:
        await set_state_pyramid(0, 1, id_pyramid, root_path)


def vector_pyramid_in_separate_process(
    ds: DataSource,
    datasource_id: str,
    root_path: str,
    id_pyramid: str,
) -> None:
    # strictly 'spawn' processes
    mp_context = multiprocessing.get_context("spawn")
    try:
        vp = mp_context.Process(
            target=vector_pyramid_with_loop,
            args=(ds, datasource_id, root_path, id_pyramid),
            kwargs={},
        )
        vp.start()
        vp.join()
    except Exception as e:
        logger.error(
            f"'vector_pyramid_in_separate_process' function execution error: {e}"
        )
        vp.terminate()


def vector_pyramid_with_loop(
    ds: DataSource,
    datasource_id: str,
    root_path: str,
    id_pyramid: str,
) -> None:
    initialize_event_loop()
    loop = asyncio.get_event_loop()
    loop.run_until_complete(vector_pyramid(ds, datasource_id, root_path, id_pyramid))
    loop.close()


async def vector_pyramid(
    ds: DataSource,
    datasource_id: str,
    root_path: str,
    id_pyramid: str,
):
    mbtiles_db: Optional[str] = None
    connection: Optional[aiosqlite.Connection] = None
    cursor: Optional[aiosqlite.Cursor] = None
    if ds.mbtiles:
        await asyncio.sleep(2)

        mbtiles_db = os.path.join(
            root_path, "tiles", datasource_id, f"{datasource_id}.mbtiles"
        )
        if await aio_os.path.isfile(mbtiles_db):
            try:
                await aio_os.remove(mbtiles_db)
            except Exception:
                pass

        wal_file: str = f"{mbtiles_db}-wal"
        shm_file: str = f"{mbtiles_db}-shm"
        if await aio_os.path.isfile(wal_file):
            try:
                await aio_os.remove(wal_file)
                await aio_os.remove(shm_file)
            except Exception:
                pass

        await asyncio.sleep(2)

        if not await aio_os.path.isfile(mbtiles_db):
            connection = await sqlite_db_connect_async(mbtiles_db)
            cursor = await connection.cursor()
            await async_mbtiles_setup(cursor)
        else:
            connection = await sqlite_db_connect_async(mbtiles_db)
            cursor = await connection.cursor()
            await cursor.execute("DELETE FROM tiles;")
            await connection.commit()
            await cursor.execute("VACUUM;")
            await connection.commit()

    try:
        db_pool, db_pool_size = await init_db_pool(ds)
        bounds: List[float] = [
            ds.bounds.lng_w,
            ds.bounds.lat_s,
            ds.bounds.lng_e,
            ds.bounds.lat_n,
        ]
        ti = tiles_iterator(bounds, ds.pyr_settings.minzoom, ds.pyr_settings.maxzoom)
        running_tasks: Set[Task] = set()
        for batched_tiles in batched(ti, db_pool_size):
            tasks_mvt = [
                get_vector_tile(
                    db_pool,
                    ds,
                    datasource_id,
                    tile.z,
                    tile.x,
                    tile.y,
                    "mvt",
                    root_path,
                )
                for tile in batched_tiles
            ]
            try:
                results = await asyncio.gather(*tasks_mvt, return_exceptions=True)
                tiles_for_save: List[MVTile] = []
                for res in results:
                    if isinstance(res, Exception):
                        continue
                    if res is not None and isinstance(res, MVTile):
                        tiles_for_save.append(res)

                if len(tiles_for_save) > 0:
                    task: Task = asyncio.create_task(
                        save_batched_mvt_tiles(
                            copy.deepcopy(tiles_for_save),
                            ds.mbtiles,
                            mbtiles_db,
                            connection,
                            cursor,
                            ds.compress_tiles,
                        )
                    )
                    tiles_for_save.clear()
                    # Add task to the set. This creates a strong reference.
                    running_tasks.add(task)
                    # To prevent keeping references to finished tasks forever,
                    # make each task remove its own reference from the set after completion:
                    task.add_done_callback(running_tasks.discard)
            except Exception as e:
                logger.error(f"Error get MVT tile: {e}")
    except Exception as e:
        logger.error(f"Error 'vector_pyramid': {e}")
    finally:
        if cursor is not None and connection is not None:
            attempts = 30
            for i in range(0, attempts):
                await asyncio.sleep(1)
                if len(running_tasks) == 0:
                    await cursor.close()
                    await connection.close()
                    break
                if attempts == i + 1:
                    for task in running_tasks:
                        try:
                            task.cancel()
                        except:
                            pass
                    await asyncio.sleep(5)
                    await cursor.close()
                    await connection.close()

        await set_state_pyramid(0, 1, id_pyramid, root_path)


class MVTile:
    __slots__ = ("x", "y", "z", "mvt", "tile_file_name")

    def __init__(self, z: int, x: int, y: int, mvt: bytes, tile_file_name: str):
        self.z = z
        self.x = x
        self.y = y
        self.mvt = mvt
        self.tile_file_name = tile_file_name


async def get_vector_tile(
    db_pool: Pool,
    ds: DataSource,
    datasource_id: str,
    z: int,
    x: int,
    y: int,
    ext: str,
    root_path: str,
) -> Optional[MVTile]:
    mvt: bytes = await generate_mvt(ds, db_pool, z, x, y)
    # Save only not empty tiles
    if mvt != b"":
        tile_file_name: str = os.path.join(
            f"{root_path}",
            "tiles",
            datasource_id,
            f"{z}",
            f"{x}",
            f"{y}.{ext}",
        )
        return MVTile(z, x, y, mvt, tile_file_name)

    return None


async def save_batched_mvt_tiles(
    tiles: List[MVTile],
    mbtiles: bool,
    mbtiles_db: Optional[str],
    connection: Optional[aiosqlite.Connection],
    cursor: Optional[aiosqlite.Cursor],
    compress_tiles: bool,
) -> None:
    if (
        mbtiles
        and mbtiles_db is not None
        and connection is not None
        and cursor is not None
    ):
        ts = [
            (
                t.z,
                t.x,
                t.y,
                sqlite3.Binary(gzip.compress(t.mvt) if compress_tiles else t.mvt),
            )
            for t in tiles
        ]
        try:
            await cursor.executemany(
                """INSERT OR IGNORE INTO tiles (zoom_level, tile_column, tile_row, tile_data) values (?, ?, ?, ?);""",
                ts,
            )
            await connection.commit()
        except aiosqlite.OperationalError as e:
            # Exception `database is locked`
            attempts = 10
            for i in range(0, attempts):
                await asyncio.sleep(0.2)
                try:
                    await cursor.executemany(
                        """INSERT OR IGNORE INTO tiles (zoom_level, tile_column, tile_row, tile_data) values (?, ?, ?, ?);""",
                        ts,
                    )
                    await connection.commit()
                except Exception as exc:
                    if i == (attempts - 1):
                        logger.error(
                            f"After {attempts} attempts error save tiles in DB '{mbtiles_db}': {exc}"
                        )
                        break
                    continue
                break
        except Exception as e:
            logger.error(f"Error save MVTiles in DataBase '{mbtiles_db}': {str(e)}")
    else:
        tasks_save = [
            save_tile_on_disk(
                t.x,
                t.y,
                t.z,
                t.tile_file_name,
                t.mvt,
            )
            for t in tiles
        ]
        try:
            await asyncio.gather(*tasks_save, return_exceptions=True)
        except Exception as e:
            logger.error(f"Error save MVTiles on disk: {e}")


async def init_db_pool(ds: DataSource) -> Tuple[Pool, int]:
    dsn, _ = dsn_postgresql()
    db_pool_size: int = ds.pyr_settings.count_processes or math.ceil(
        multiprocessing.cpu_count() / 3
    )
    db_pool: Pool = await asyncpg.create_pool(
        dsn=dsn,
        init=set_connection_type_codec,
        min_size=1,
        max_size=db_pool_size,
        max_queries=5000,
        max_inactive_connection_lifetime=300,
        timeout=180,  # 3 Minutes
        ssl=False,
    )
    return db_pool, db_pool_size
