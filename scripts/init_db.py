import asyncpg
import orjson
import os
import asyncio

from uuid import uuid4
from asyncpg import Connection, UniqueViolationError
from dotenv import dotenv_values
from pathlib import Path
from typing import Dict, Any, Optional, List, Set

CLEAR_DATASOURCE = """
DROP TABLE IF EXISTS datasource;
"""

DATASOURCE_TABLE = """
CREATE TABLE IF NOT EXISTS datasource (
    id SERIAL PRIMARY KEY,
    identifier VARCHAR NOT NULL UNIQUE,
    data_type VARCHAR,
    host VARCHAR,
    port INTEGER,
    store_type VARCHAR,
    mbtiles BOOLEAN,
    name VARCHAR,
    description TEXT,
    attribution TEXT,
    minzoom SMALLINT,
    maxzoom SMALLINT,
    bounds JSONB,
    center JSONB,
    data JSONB NOT NULL
)
"""


async def main():
    root_path: str = str(Path(__file__).parents[1])
    dotenv_path: str = os.path.join(root_path, ".env")
    config: Dict[str, Any] = dotenv_values(dotenv_path=dotenv_path)

    # Check exist database
    res: Optional[str] = await create_db(config)
    if res is not None:
        try:
            dsn = f'postgresql://{config["DBUSER"]}:{config["DBPASS"]}@{config["DBHOST"]}:{config["DBPORT"]}/postgres'
            connection: Connection = await asyncpg.connect(dsn)
            await connection.execute(res)
        except Exception as e:
            print(f'Error create database {config["DBNAME"]}: {e}')
        finally:
            await connection.close()

    dsn = f'postgresql://{config["DBUSER"]}:{config["DBPASS"]}@{config["DBHOST"]}:{config["DBPORT"]}/{config["DBNAME"]}'
    connection: Connection = await asyncpg.connect(dsn)
    await connection.set_type_codec(
        "jsonb",
        encoder=lambda v: orjson.dumps(v).decode(),
        decoder=orjson.loads,
        schema="pg_catalog",
    )
    await connection.set_type_codec(
        "json",
        encoder=lambda v: orjson.dumps(v).decode(),
        decoder=orjson.loads,
        schema="pg_catalog",
    )

    # drop datasource table
    await connection.execute(CLEAR_DATASOURCE)
    # create datasource table
    result = await connection.execute(DATASOURCE_TABLE)
    print(f"{result}: 'datasource'")

    ds_path: str = os.path.join(root_path, "datasources")
    ds_vector_path: str = os.path.join(ds_path, "vector")
    ds_raster_path: str = os.path.join(ds_path, "raster")

    # Load Vector DataSources
    if os.path.isdir(ds_vector_path):
        await upload_datasources_to_db(ds_vector_path, connection)

    # Load Raster DataSources
    if os.path.isdir(ds_raster_path):
        await upload_datasources_to_db(ds_raster_path, connection)

    await connection.close()


async def upload_datasources_to_db(ds_path: str, connection: Connection) -> None:
    query: str = """
        INSERT INTO datasource(
            identifier,
            data_type,
            store_type,
            host,
            port,
            mbtiles,
            description,
            attribution,
            minzoom,
            maxzoom,
            bounds,
            center,
            data
        )
        VALUES($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)
    """
    dss: Set[str] = set()

    for ds_file in os.listdir(ds_path):
        file = os.path.join(ds_path, ds_file)
        with open(file, "rb") as f:
            ds = orjson.loads(f.read())

            if not isinstance(ds, dict):
                raise Exception(f"Datasource '{ds_file}' is not dictionary")

            identifier = ds.get("id")
            assert isinstance(
                identifier, str
            ), f"ID of datasource '{ds_file}' must be string type, got '{identifier}'"
            assert identifier not in dss, f"ID already exist: '{identifier}'"
            dss.add(identifier)

            data_type: str = ds.get("type")
            data_store_dict: Dict[str, Any] = ds.get("dataStore")
            store_type: str = data_store_dict.get("store")
            host: str = data_store_dict.get("host")
            port: int = data_store_dict.get("port")
            description: Optional[str] = ds.get("description")
            attribution: Optional[str] = ds.get("attribution")
            minzoom: int = ds.get("minzoom") or 0
            maxzoom: int = ds.get("maxzoom") or 20
            bounds: Dict[str, float] = ds.get("bounds")
            center: List[float] = ds.get("center")
            mbtiles: bool = ds.get("mbtiles") or False

            params = (
                identifier,
                data_type,
                store_type,
                host,
                port,
                mbtiles,
                description,
                attribution,
                minzoom,
                maxzoom,
                bounds,
                center,
                ds,
            )

            try:
                res = await connection.execute(
                    query,
                    *params,
                )
            except UniqueViolationError:
                new_id: str = str(uuid4())
                ds["id"] = new_id
                new_params = (new_id,) + params[1:]
                await connection.execute(
                    query,
                    *new_params,
                )
            except Exception as e:
                raise Exception(f"Error insert DataSource '{ds.get('id')}' in DB: {e}")

            print(f"INSERT Datasource '{ds_file}' result: {res}")


async def create_db(config: Dict[str, Any]) -> Optional[str]:
    try:
        dsn = f'postgresql://{config["DBUSER"]}:{config["DBPASS"]}@{config["DBHOST"]}:{config["DBPORT"]}/postgres'
        connection: Connection = await asyncpg.connect(dsn)
        db: str = config["DBNAME"]
        query: str = (
            f"\
            SELECT 'CREATE DATABASE {db};'\
            WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = '{db}')"
        )
        result = await connection.fetchval(query)
        await connection.close()
        return result
    except Exception as e:
        print(f"Error: {e}")
    finally:
        await connection.close()


if __name__ == "__main__":
    asyncio.run(main())
