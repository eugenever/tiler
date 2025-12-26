import logging
import reqsnaked
import datetime
import os
import aiofiles.os as aio_os
import aiosqlite
import orjson
import asyncio
import gzip

from typing import List, Optional, Union, Mapping

from fastapi import Response, Request, status, BackgroundTasks, FastAPI
from starlette.exceptions import HTTPException

from server.fapi.utils import MAPTILER_ERROR
from server.datasources import DataSource, DataStoreVectorTiles, DataStoreVectorInternal
from server.fapi.vector.mvt_postgis import generate_mvt
from server.tile_utils import save_tile_on_disk
from server.sqlite_db import sqlite_db_connect_async

logger = logging.getLogger(__name__)


async def vector_tile(
    request: Request,
    datasource_id: str,
    z: int,
    x: int,
    y: int,
    ext: str,
    root_path: str,
    background_tasks: BackgroundTasks,
) -> Response:
    ds: DataSource = request.app.state.datasources.get(datasource_id)
    data_store = ds.data_store

    mvt: bytes = b""
    headers: Optional[Mapping[str, str]] = None

    if isinstance(data_store, DataStoreVectorTiles):
        mvt = await request_tile(
            request.app,
            data_store.tiles,
            data_store.keys,
            request.app.state.http_client,
            z,
            x,
            y,
        )
        # In case of an error, we return it directly
        if isinstance(mvt, Response):
            return mvt
    elif isinstance(data_store, DataStoreVectorInternal):
        mvt = await generate_mvt(ds, request.app.state.db_pool, z, x, y)

    # Save only not empty tiles
    if mvt != b"":
        if ds.compress_tiles:
            mvt = gzip.compress(mvt)
            headers = {"Content-Encoding": "gzip"}

        background_tasks.add_task(
            save_tile, x, y, z, ext, datasource_id, ds.mbtiles, mvt, root_path
        )

    response_code = status.HTTP_204_NO_CONTENT if mvt == b"" else status.HTTP_200_OK

    return Response(
        content=mvt,
        media_type="application/vnd.mapbox-vector-tile",
        headers=headers,
        status_code=response_code,
    )


async def request_tile(
    app: FastAPI,
    tiles: List[str],
    keys: List[str],
    http_client: reqsnaked.Client,
    z: int,
    x: int,
    y: int,
) -> Union[bytes, Response]:

    if len(tiles) < 1:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Wrong format tiles: {tiles}",
        )

    tiles_url: str = tiles[0]
    body: Optional[reqsnaked.Bytes] = None

    # try for case of problems with Internet connection
    try:
        count_keys: int = len(keys)
        # MapTiler case
        if count_keys > 0:
            index_key: int = 0
            while index_key < len(keys):
                key: str = keys[index_key]
                # After 'CHECK_KEYS_AFTER_DAYS' day we check the key for validity
                if key in app.state.invalid_keys:
                    last_date: datetime.datetime = app.state.invalid_keys[key]
                    now: datetime.datetime = datetime.datetime.now()
                    days: int = (now - last_date).days
                    env_var_days: Optional[str] = app.state.env_vars.get(
                        "CHECK_KEYS_AFTER_DAYS"
                    )
                    if env_var_days is not None:
                        check_keys_after_days = int(env_var_days)
                    else:
                        check_keys_after_days = 1
                    if days >= check_keys_after_days:
                        app.state.invalid_keys.pop(key, None)
                    else:
                        index_key += 1
                        continue

                request: reqsnaked.Request = reqsnaked.Request(
                    "GET",
                    tiles_url.format(z=z, x=x, y=y, k=key),
                    timeout=datetime.timedelta(seconds=30),
                )
                response: reqsnaked.Response = await http_client.send(request)
                status_response: int = response.status

                # In case of any error, take another key
                if round(status_response / 100) != 2:
                    # On 16 zoom get error 'Out of bounds' and status code = 400
                    body_error: reqsnaked.Bytes = await response.read()
                    error: str = body_error.as_bytes().decode("utf-8")
                    if (
                        status_response == status.HTTP_400_BAD_REQUEST
                        and error == MAPTILER_ERROR
                    ) or error == MAPTILER_ERROR:
                        return Response(
                            status_code=status_response,
                            content=orjson.dumps({"error": error}),
                            media_type="application/json",
                        )

                    app.state.invalid_keys[key] = datetime.datetime.now()
                    index_key += 1
                else:
                    body = await response.read()
                    break

        # ARCGIS case
        elif count_keys == 0:
            body = await request_tile_without_key(http_client, tiles_url, z, x, y)
            # In case of an error, we return it directly
            if isinstance(body, Response):
                return body

    except Exception as e:
        logger.error(f"External vector tile 'request_tile': {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"External vector tile 'request_tile': {e}",
        )

    if body is not None:
        return body.as_bytes()

    return b""


async def request_tile_without_key(
    http_client: reqsnaked.Client,
    tiles_url: str,
    z: int,
    x: int,
    y: int,
    key: Optional[str] = None,
) -> Union[Response, reqsnaked.Bytes]:

    if key is not None:
        url: str = tiles_url.format(z=z, x=x, y=y, k=key)
    else:
        url = tiles_url.format(z=z, x=x, y=y)

    request = reqsnaked.Request(
        "GET",
        url,
        timeout=datetime.timedelta(seconds=30),
    )
    response: reqsnaked.Response = await http_client.send(request)
    status_response: int = response.status

    # In case of any error, take another key
    if round(status_response / 100) != 2:
        body_error: reqsnaked.Bytes = await response.read()
        error: str = body_error.as_bytes().decode("utf-8")

        return Response(
            status_code=status_response,
            content=orjson.dumps({"error": error}),
            media_type="application/json",
        )

    body: reqsnaked.Bytes = await response.read()
    return body


async def save_tile(
    x: int,
    y: int,
    z: int,
    ext: str,
    datasource_id: str,
    mbtiles: bool,
    buffer: bytes,
    root_path: str,
):
    tile_file_name: str = os.path.join(
        f"{root_path}",
        "tiles",
        datasource_id,
        f"{z}",
        f"{x}",
        f"{y}.{ext}",
    )

    if mbtiles:
        mbtiles_db: str = os.path.join(
            root_path, "tiles", datasource_id, f"{datasource_id}.mbtiles"
        )
        if await aio_os.path.isfile(mbtiles_db):
            connection: aiosqlite.Connection = await sqlite_db_connect_async(mbtiles_db)
            cursor: aiosqlite.Cursor = await connection.cursor()
            try:
                await cursor.execute(
                    """INSERT OR IGNORE INTO tiles (zoom_level, tile_column, tile_row, tile_data) values (?, ?, ?, ?);""",
                    (z, x, y, buffer),
                )
                await connection.commit()
            except aiosqlite.OperationalError as e:
                # Exception `database is locked`
                attempts = 10
                for i in range(0, attempts):
                    await asyncio.sleep(0.2)
                    try:
                        await cursor.execute(
                            """INSERT OR IGNORE INTO tiles (zoom_level, tile_column, tile_row, tile_data) values (?, ?, ?, ?);""",
                            (z, x, y, buffer),
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
                logger.error(f"Error save tiles in DataBase '{mbtiles_db}': {str(e)}")
            finally:
                await cursor.close()
                await connection.close()
        else:
            await save_tile_on_disk(x, y, z, tile_file_name, buffer)
    else:
        await save_tile_on_disk(x, y, z, tile_file_name, buffer)
