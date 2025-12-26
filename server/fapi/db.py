import asyncpg
import orjson
import logging
import granian

from typing import Tuple, Dict
from asyncpg.pool import Pool
from asyncpg import Connection
from fastapi import FastAPI
from server.fapi.utils import load_environments_from_file

logger = logging.getLogger(__name__)


async def set_connection_type_codec(connection: Connection):
    await connection.set_type_codec(
        "jsonb",
        encoder=lambda v: orjson.dumps(v).decode(),
        decoder=orjson.loads,
        schema="pg_catalog",
    )


def dsn_postgresql() -> Tuple[str, int]:
    config: Dict[str, str] = load_environments_from_file()
    dsn = f'postgresql://{config["DBUSER"]}:{config["DBPASS"]}@{config["DBHOST"]}:{config["DBPORT"]}/{config["DBNAME"]}'
    db_pool_size: int = int(config["DBPOOLSIZE"])
    return dsn, db_pool_size


async def connect_to_db(app: FastAPI) -> None:
    dsn, db_pool_size = dsn_postgresql()
    app.state.db_pool: Pool = await asyncpg.create_pool(
        dsn=dsn,
        init=set_connection_type_codec,
        min_size=1,
        max_size=db_pool_size,
        max_queries=5000,
        max_inactive_connection_lifetime=300,
        timeout=180,  # 3 Minutes
        ssl=False,
    )
    try:
        # check version PostGIS
        connection: Connection
        async with app.state.db_pool.acquire() as connection:
            versions = await connection.fetchrow(
                "SELECT current_setting('server_version_num'), postgis_lib_version();"
            )
            app.state.pgsql_version: int = int(versions[0])
            app.state.postgis_version: Tuple[int, int, int] = tuple(
                map(int, versions[1].split("."))
            )
    except asyncpg.exceptions.UndefinedFunctionError as e:
        message: str = f"'connect_to_db': PostGIS probably not installed, {str(e)}"
        logging.error(message)
        granian.server.logger.error(f"\n{message}\n")
    except Exception as e:
        message: str = f"'connect_to_db' error: {str(e)}"
        logging.error(message)
        raise Exception(e)


async def close_db_connection(app: FastAPI) -> None:
    await app.state.db_pool.close()
