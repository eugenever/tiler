import os
import sqlite3
import asyncio
import aiosqlite
import logging
import time

logger = logging.getLogger(__name__)


def sqlite_db_connect(db_name: str) -> sqlite3.Connection:
    try:
        connection: sqlite3.Connection = sqlite3.connect(
            db_name, timeout=240, check_same_thread=False
        )
        optimize_connection(connection.cursor())
        return connection
    except sqlite3.OperationalError:
        # Exception `database is locked`
        attempts = 10
        for i in range(0, attempts):
            time.sleep(0.5)
            try:
                connection: sqlite3.Connection = sqlite3.connect(
                    db_name, timeout=240, check_same_thread=False
                )
                optimize_connection(connection.cursor())
                return connection
            except Exception as exc:
                if i == (attempts - 1):
                    logger.error(
                        f"After {attempts} attempts error connecting to SQLite DataBase '{db_name}': {exc}"
                    )
                    raise Exception(exc)
                continue
    except Exception as e:
        raise Exception(f"Error connecting to SQLite DataBase {db_name}: {e}")


def optimize_connection(cursor: sqlite3.Cursor) -> None:
    cursor.execute("""PRAGMA synchronous=NORMAL;""")
    cursor.execute("""PRAGMA journal_mode=WAL;""")
    cursor.execute("""PRAGMA optimize;""")
    cursor.execute("""PRAGMA analysis_limit=8192;""")
    cursor.execute("""PRAGMA cache_size=-2000;""")
    cursor.execute("""PRAGMA page_size=65536;""")
    cursor.execute("""PRAGMA foreign_keys=1;""")
    cursor.execute("""PRAGMA busy_timeout=240000;""")


def tiler_setup(cursor: sqlite3.Cursor) -> None:
    cursor.execute(
        """
        CREATE TABLE pyramids (
            id text NOT NULL,
            dataset text NOT NULL,
            start_time timestamp,
            finish_time timestamp,
            params text NOT NULL,
            running integer,
            complete integer,
            PRIMARY KEY(id)
        );
        """
    )


async def sqlite_db_connect_async(db_name: str) -> aiosqlite.Connection:
    try:
        connection: aiosqlite.Connection = await aiosqlite.connect(
            db_name, timeout=240, check_same_thread=False
        )
        await optimize_connection_async(await connection.cursor())
        return connection
    except aiosqlite.OperationalError:
        # Exception `database is locked`
        attempts = 10
        for i in range(0, attempts):
            await asyncio.sleep(0.5)
            try:
                connection: aiosqlite.Connection = await aiosqlite.connect(
                    db_name, timeout=240, check_same_thread=False
                )
                await optimize_connection_async(await connection.cursor())
                return connection
            except Exception as exc:
                if i == (attempts - 1):
                    logger.error(
                        f"After {attempts} attempts error connecting to MBTiles DataBase '{db_name}': {exc}"
                    )
                    raise Exception(exc)
                continue
    except Exception as e:
        raise Exception(f"Error connecting to MBTiles DataBase {db_name}: {e}")


async def optimize_connection_async(cursor: aiosqlite.Cursor) -> None:
    await cursor.execute("""PRAGMA synchronous=NORMAL;""")
    await cursor.execute("""PRAGMA journal_mode=WAL;""")
    await cursor.execute("""PRAGMA optimize;""")
    await cursor.execute("""PRAGMA analysis_limit=8192;""")
    await cursor.execute("""PRAGMA cache_size=-2000;""")
    await cursor.execute("""PRAGMA page_size=65536;""")
    await cursor.execute("""PRAGMA foreign_keys=1;""")
    await cursor.execute("""PRAGMA busy_timeout=240000;""")


def init_db():
    cwd: str = os.getcwd()
    tiler_db: str = os.path.join(cwd, "data", "tiler.db")
    if not os.path.isfile(tiler_db):
        connection: sqlite3.Connection = sqlite_db_connect(tiler_db)
        cursor: sqlite3.Cursor = connection.cursor()
        tiler_setup(cursor)
        cursor.close()
        connection.commit()
        connection.close()
    else:
        # Reset not completed pyramids due to Exceptions
        connection = sqlite_db_connect(tiler_db)
        cursor = connection.cursor()
        cursor.execute(
            "UPDATE pyramids SET complete = 1 WHERE running = 1 AND finish_time IS NULL"
        )
        cursor.close()
        connection.commit()
        connection.close()
