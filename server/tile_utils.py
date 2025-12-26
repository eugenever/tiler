import logging
import os
import sqlite3
import aiosqlite
import rio_tiler as _
import asyncio
import aiofiles
import aiofiles.os as aio_os
import numpy as np

from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from morecantile.commons import BoundingBox, Tile
from morecantile.defaults import tms

from rio_tiler.io.rasterio import Reader
from rio_tiler.models import ImageData
from rio_tiler.types import Indexes
from rio_tiler.errors import EmptyMosaicError, TileOutsideBounds
from rio_tiler.utils import has_alpha_band

from fastapi import status
from starlette.exceptions import HTTPException

from raster_tiles.mosaic.reader import mosaic_reader
from raster_tiles.utils import encode_raster_to_rgba
from server.sqlite_db import sqlite_db_connect_async
from server.datasources import EXTENSIONS
from raster_tiles.defaults import (
    PIXEL_SELECTION_METHOD,
    PIXEL_SELECTION_METHOD_TO_MERGE_METHOD,
    MERGE_MOSAIC_METHODS,
)

logger = logging.getLogger(__name__)


def is_warped_raster_asset(asset: str) -> bool:
    if not os.path.isfile(asset):
        return False

    ext = os.path.splitext(asset)[-1].lower()
    if (
        ext != ".ovr"
        and ("_warp_tr_ov." in asset.lower())
        and (ext in EXTENSIONS)
        and not any([f"_{method.upper()}" in asset for method in MERGE_MOSAIC_METHODS])
    ):
        return True
    else:
        return False


class NeighborTile:
    def __init__(
        self, t: Tile, buffer: bytes, datasource_id: str, dataset: str, ext: str
    ):
        self.t = t
        self.buffer = buffer

        parent_dir: Path = Path(__file__).parents[1]

        tile_file_name: str = os.path.join(
            f"{parent_dir}",
            "tiles",
            datasource_id,
            f"{t.z}",
            f"{t.x}",
            f"{t.y}.{ext}",
        )

        self.tile_file_name = tile_file_name


class NeighborMosaicTile:
    def __init__(
        self, t: Tile, buffer: bytes, datasource_id: str, dataset: str, ext: str
    ):
        self.t = t
        self.buffer = buffer

        parent_dir: Path = Path(__file__).parents[1]

        tile_file_name: str = os.path.join(
            f"{parent_dir}",
            "tiles",
            datasource_id,
            f"{t.z}",
            f"{t.x}",
            f"{t.y}.{ext}",
        )

        self.tile_file_name = tile_file_name


def meta_tile(
    reader: Reader,
    x: int,
    y: int,
    z: int,
    tilesize: int,
    tile_driver: str,
    indexes: Optional[Indexes],
    nodata_default: float,
    encode_to_rgba: bool,
    resampling_method: str,
    is_has_alpha_band: int,
    add_neighbors: bool = False,
) -> Optional[List[Tuple[bytes, int, int, int]]]:

    t = Tile(x, y, z)
    neighbor_tiles: List[Tile] = [t]
    # TODO: Disable metatile
    if add_neighbors:
        neighbor_tiles.extend(reader.tms.neighbors(t))

    # If has alpha band then use his, non casting mask from rio-tiler
    if is_has_alpha_band == 1:
        # Need specified all bands from input raster
        indexes = reader.dataset.indexes
        add_mask = False
    else:
        indexes = None
        add_mask = True

    nts: List[(bytes, int, int, int)] = []
    for n in neighbor_tiles:
        try:
            img_data_n: ImageData = reader.tile(
                n.x,
                n.y,
                n.z,
                tilesize=tilesize,
                resampling_method=resampling_method,
                indexes=indexes,
                nodata=nodata_default,
            )
        except TileOutsideBounds:
            continue
        except Exception as e:
            logger.error(f"'meta_tile' tile {n}, error: {e}")
            continue

        is_nan = np.isnan(np.sum(img_data_n.data))
        if is_nan:
            img_data_n.data[np.isnan(img_data_n.data)] = nodata_default
        if np.all(img_data_n.data == nodata_default):
            continue

        if encode_to_rgba:
            img_data_n = encode_raster_to_rgba(img_data_n, nodata_default)
            if img_data_n:
                # in this case "add_mask" always = False
                buffer: bytes = img_data_n.render(
                    img_format=tile_driver, add_mask=False
                )
        else:
            if np.all(img_data_n.data == 0):
                continue
            buffer = img_data_n.render(img_format=tile_driver, add_mask=add_mask)

        nts.append((buffer, n.x, n.y, n.z))

    if len(nts) > 0:
        return nts

    return None


def get_tile(
    datasource_id: str,
    input_file: str,
    dataset: str,
    x: int,
    y: int,
    z: int,
    tilesize: int,
    tile_driver: str,
    is_has_alpha_band: int,
    encode_to_rgba: int,
    nodata_default: float,
    ext: str,
    resampling_method: str,
    reader: Optional[Reader] = None,
) -> Tuple[Optional[bytes], List[NeighborTile]]:
    try:
        if reader is None:
            reader = Reader(input_file)

        if not reader.tile_exists(x, y, z):
            logger.warn(
                f"'get_tile': tile '{x}:{y}:{z}' out of bounds dataset, dataset '{dataset}'"
            )
            raise HTTPException(
                status_code=status.HTTP_204_NO_CONTENT,
                detail=f"'get_tile': tile '{x}:{y}:{z}' out of bounds dataset, dataset '{dataset}'",
            )

        if is_has_alpha_band == 1:
            # Need specified all bands from input raster
            indexes = reader.dataset.indexes
        else:
            indexes = None

        nts: List[NeighborTile] = []
        buffer_requested: Optional[bytes] = None
        neighbor_tiles = meta_tile(
            reader,
            x,
            y,
            z,
            tilesize,
            tile_driver,
            indexes,
            nodata_default,
            encode_to_rgba,
            resampling_method,
            is_has_alpha_band,
            False,
        )

        if neighbor_tiles is None:
            return (None, nts)

        for neighbor in neighbor_tiles:
            n = NeighborTile(
                Tile(neighbor[1], neighbor[2], neighbor[3]),
                neighbor[0],
                datasource_id,
                dataset,
                ext,
            )
            nts.append(n)
            if neighbor[1] == x and neighbor[2] == y and neighbor[3] == z:
                buffer_requested = neighbor[0]

        return (buffer_requested, nts)

    except HTTPException as e:
        raise e
    except Exception as e:
        raise_exception(
            status.HTTP_500_INTERNAL_SERVER_ERROR,
            f"'get tile', dataset '{dataset}', tile {x}:{y}:{z}: {e}",
            logging.ERROR,
        )


def get_mosaic_tile(
    datasource_id: str,
    dataset: str,
    x: int,
    y: int,
    z: int,
    tilesize: int,
    tile_driver: str,
    encode_to_rgba: int,
    nodata_default: float,
    ext: str,
    pixel_selection_method: str,
    resampling_method: str,
    merge: bool,
    add_neighbors: bool = False,
) -> Tuple[Optional[bytes], List[NeighborMosaicTile]]:
    try:
        nts: List[NeighborMosaicTile] = []
        buffer_requested: Optional[bytes] = None

        parent_dir: Path = Path(__file__).parents[1]
        data_dir: str = os.path.join(parent_dir, "data")
        assets_dir: str = os.path.join(data_dir, datasource_id)
        psm = PIXEL_SELECTION_METHOD[pixel_selection_method]

        mosaic_assets: List[str] = []
        if not merge:
            mosaic_assets = [
                os.path.join(assets_dir, f)
                for f in os.listdir(assets_dir)
                if is_warped_raster_asset(os.path.join(assets_dir, f))
            ]
        else:
            method: str = PIXEL_SELECTION_METHOD_TO_MERGE_METHOD[psm.__name__]
            merged_raster = os.path.join(assets_dir, f"{dataset}_{method.upper()}.tif")
            mosaic_assets.append(merged_raster)

        t = Tile(x, y, z)
        neighbor_tiles: List[Tile] = [t]
        # TODO: Disable metatile
        if add_neighbors:
            tms_wmq = tms.get("WebMercatorQuad")
            neighbor_tiles.extend(tms_wmq.neighbors(t))

        for neighbor in neighbor_tiles:
            try:
                if not merge:
                    image_data, _ = mosaic_reader(
                        mosaic_assets,
                        reader_mosaic_tile,
                        neighbor.x,
                        neighbor.y,
                        neighbor.z,
                        tilesize,
                        resampling_method=resampling_method,
                        pixel_selection=psm,
                    )
                    if psm.__name__ == "MeanMethod":
                        nd = nodata_default
                        if nodata_default is None:
                            nd = -999999
                        mask = np.ma.getmask(image_data.array)
                        image_data.data[mask] = nd
                else:
                    image_data = reader_mosaic_tile(
                        merged_raster,
                        neighbor.x,
                        neighbor.y,
                        neighbor.z,
                        tilesize,
                        resampling_method=resampling_method,
                    )
            except EmptyMosaicError:
                logger.info(
                    f"'get_mosaic_tile': dataset {dataset}, tile {x}:{y}:{z} outside the mosaic boundaries"
                )
                continue

            if not image_data:
                continue

            is_nan = np.isnan(np.sum(image_data.data))
            if is_nan:
                image_data.data[np.isnan(image_data.data)] = nodata_default
            if np.all(image_data.data == nodata_default):
                continue

            if encode_to_rgba == 1:
                img_data = encode_raster_to_rgba(image_data, nodata_default)
                img_data.assets = image_data.assets
                img_data.crs = image_data.crs
                img_data.bounds = image_data.bounds
                img_data.band_names = image_data.band_names
                img_data.metadata = image_data.metadata
                img_data.dataset_statistics = image_data.dataset_statistics
            else:
                img_data = image_data

            # Tile is out of bounds raster OR all data is NODATA, exclude this tiles
            if np.all(img_data.data == 0):
                del img_data
                continue

            if encode_to_rgba == 1:
                # in this case "add_mask" always = False
                buffer: bytes = img_data.render(img_format=tile_driver, add_mask=False)
            else:
                buffer = img_data.render(img_format=tile_driver)

            nts.append(
                NeighborMosaicTile(neighbor, buffer, datasource_id, dataset, ext)
            )

            if neighbor.x == x and neighbor.y == y and neighbor.z == z:
                buffer_requested = buffer

        return buffer_requested, nts

    except HTTPException as e:
        raise e
    except Exception as e:
        raise_exception(
            status.HTTP_500_INTERNAL_SERVER_ERROR,
            f"'get_mosaic_tile': error get mosaic tile '{dataset} {x}:{y}:{z}': {e}",
            logging.ERROR,
        )


def reader_mosaic_tile(
    asset: str,
    x: int,
    y: int,
    z: int,
    tilesize: int,
    resampling_method: str,
) -> Optional[ImageData]:
    reader: Reader = Reader(asset)
    if isinstance(reader.dataset.nodatavals, tuple):
        nodata = reader.dataset.nodatavals[0]
        if nodata is None:
            nodata_default = -999999.0
        else:
            nodata_default = nodata

    indexes = None
    if has_alpha_band(reader.dataset):
        indexes = reader.dataset.indexes

    try:
        image_data: ImageData = reader.tile(
            x,
            y,
            z,
            tilesize=tilesize,
            resampling_method=resampling_method,
            indexes=indexes,
            nodata=nodata_default,
        )
    except TileOutsideBounds:
        return None
    except Exception as e:
        logger.error(f"'reader_mosaic_tile' tile {x}:{y}:{z}, error: {e}")
        return None

    if np.all(image_data.data == nodata_default):
        return None

    return image_data


def reader_single_tile(
    asset: str,
    x: int,
    y: int,
    z: int,
    readers: Dict[str, Reader],
    tilesize: int,
    resampling_method: str,
) -> Optional[ImageData]:
    reader: Reader = readers[asset]
    if isinstance(reader.dataset.nodatavals, tuple):
        nodata = reader.dataset.nodatavals[0]
        if nodata is None:
            nodata_default = 0.0
        else:
            nodata_default = nodata

    indexes = None
    if has_alpha_band(reader.dataset):
        indexes = reader.dataset.indexes

    try:
        image_data: ImageData = reader.tile(
            x,
            y,
            z,
            tilesize=tilesize,
            resampling_method=resampling_method,
            indexes=indexes,
            nodata=nodata_default,
        )
    except TileOutsideBounds:
        return None
    except Exception as e:
        logger.error(f"'reader_single_tile' tile {x}:{y}:{z}, error: {e}")
        return None

    if np.all(image_data.data == nodata_default):
        return None

    return image_data


def reader_part(
    asset: str,
    x: int,
    y: int,
    z: int,
    readers: Dict[str, Reader],
    tilesize: int = 256,
    **kwargs: Any,
) -> Optional[ImageData]:
    reader = readers[asset]
    t = Tile(x, y, z)
    tms_wmq = tms.get("WebMercatorQuad")
    neighbors = tms_wmq.neighbors(t)

    # depending on the zoom, the coefficient K will change
    count_nts = len(neighbors)
    k = 3
    if count_nts == 0:
        k = 1
    elif count_nts == 3:
        k = 2

    # add original tile
    neighbors.append(t)
    exists_neighbors = [
        neighbor
        for neighbor in neighbors
        if reader.tile_exists(neighbor.x, neighbor.y, neighbor.z)
    ]

    if len(exists_neighbors) == 0:
        return None

    nodata = reader.dataset.nodata
    left: float = 0.0
    top: float = 0.0
    right: float = 0.0
    bottom: float = 0.0

    for i, nr in enumerate(neighbors):
        try:
            bbox: BoundingBox = tms_wmq.xy_bounds(nr)
            if i == 0:
                left = bbox.left
                top = bbox.top
                right = bbox.right
                bottom = bbox.bottom
                continue

            if left > bbox.left:
                left = bbox.left
            if top < bbox.top:
                top = bbox.top
            if right < bbox.right:
                right = bbox.right
            if bottom > bbox.bottom:
                bottom = bbox.bottom
        except Exception as e:
            logger.error(f"Error define bounds of neighbor {nr}: {e}")

    bbox_general = BoundingBox(left, bottom, right, top)

    dst_crs = reader.tms.rasterio_crs
    image_data = reader.part(
        bbox=bbox_general,
        dst_crs=dst_crs,
        bounds_crs=dst_crs,
        height=k * tilesize,
        width=k * tilesize,
        max_size=None,
        indexes=None,
        expression=None,
        buffer=0.0,
        nodata=nodata,
        **kwargs,
    )

    return image_data


async def save_mbtile(mbtiles_db: str, x: int, y: int, z: int, buffer: bytes) -> None:
    try:
        connection: aiosqlite.Connection = await sqlite_db_connect_async(mbtiles_db)
        cursor: aiosqlite.Cursor = await connection.cursor()
        await cursor.execute(
            """INSERT OR IGNORE INTO tiles (zoom_level, tile_column, tile_row, tile_data) values (?, ?, ?, ?);""",
            (z, x, y, sqlite3.Binary(buffer)),
        )
        await connection.commit()
        await cursor.close()
        await connection.close()
    except Exception as e:
        logger.error(f"Error save mbtile {x}:{y}:{z} in database '{mbtiles_db}': {e}")
        await cursor.close()
        await connection.close()


async def save_tile_on_disk(
    x: int, y: int, z: int, tile_file_name: str, buffer: bytes
) -> None:
    try:
        if not await aio_os.path.isfile(tile_file_name):
            await aio_os.makedirs(os.path.dirname(tile_file_name), exist_ok=True)
            async with aiofiles.open(tile_file_name, "wb") as tile_file:
                await tile_file.write(buffer)
                await tile_file.flush()
    except Exception as e:
        logger.error(f"Error save tile '{tile_file_name}' on disk: {e}")


async def save_tile(
    mbtiles_db: str, x: int, y: int, z: int, buffer: bytes, tile_file_name: str
) -> None:
    if await aio_os.path.isfile(mbtiles_db):
        await save_mbtile(mbtiles_db, x, y, z, buffer)
    else:
        await save_tile_on_disk(x, y, z, tile_file_name, buffer)


async def get_mbtile(mbtiles_db: str, z: int, x: int, y: int) -> Optional[sqlite3.Row]:
    connection = await sqlite_db_connect_async(mbtiles_db)
    cursor = await connection.cursor()
    sql_tile = "SELECT tile_data from tiles where zoom_level = (?) AND tile_column = (?) AND tile_row = (?);"
    cur_exec = await cursor.execute(sql_tile, (z, x, y))
    mbtile: Optional[sqlite3.Row] = await cur_exec.fetchone()
    await cursor.close()
    await connection.close()

    if mbtile is not None:
        return mbtile

    return None


async def get_tile_job(db_path: str) -> Optional[sqlite3.Row]:
    try:
        connection: aiosqlite.Connection = await sqlite_db_connect_async(db_path)
        cursor: aiosqlite.Cursor = await connection.cursor()
        sql: str = (
            "SELECT src_file, data_bands_count, tile_driver, tile_extension, tile_size, profile, querysize, xyz, in_file, input_file, has_alpha_band, nodata, encode_to_rgba, pixel_selection_method, resampling_method, merge from tile_job;"
        )
        cur_exec: aiosqlite.Cursor = await cursor.execute(sql)
        tile_job: Optional[sqlite3.Row] = await cur_exec.fetchone()
    except Exception as e:
        logger.error(f"'get_tile_job': error get tile job: {e}")
        return None
    finally:
        await cursor.close()
        await connection.close()

    if tile_job:
        return tile_job

    return None


async def save_neighbors_tiles(
    mbtiles: bool, mbtiles_db: str, neighbors: List[NeighborTile]
) -> None:
    if len(neighbors) == 0:
        return None

    if mbtiles:
        if await aio_os.path.isfile(mbtiles_db):
            connection: aiosqlite.Connection = await sqlite_db_connect_async(mbtiles_db)
            cursor: aiosqlite.Cursor = await connection.cursor()

            ngrs = [
                (
                    neighbor.t.z,
                    neighbor.t.x,
                    neighbor.t.y,
                    sqlite3.Binary(neighbor.buffer),
                )
                for neighbor in neighbors
            ]

            try:
                await cursor.executemany(
                    """INSERT OR IGNORE INTO tiles (zoom_level, tile_column, tile_row, tile_data) values (?, ?, ?, ?);""",
                    ngrs,
                )
                await connection.commit()
            except aiosqlite.OperationalError as e:
                # Exception `database is locked`
                attempts = 10
                for i in range(0, attempts):
                    await asyncio.sleep(0.5)
                    try:
                        await cursor.executemany(
                            """INSERT OR IGNORE INTO tiles (zoom_level, tile_column, tile_row, tile_data) values (?, ?, ?, ?);""",
                            ngrs,
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
                logger.error(
                    f"Error save neighbors tiles in database '{mbtiles_db}': {e}"
                )
            finally:
                await cursor.close()
                await connection.close()
        else:
            tasks_save = [
                save_tile_on_disk(
                    neighbor.t.x,
                    neighbor.t.y,
                    neighbor.t.z,
                    neighbor.tile_file_name,
                    neighbor.buffer,
                )
                for neighbor in neighbors
            ]
            try:
                await asyncio.gather(*tasks_save, return_exceptions=True)
            except Exception as e:
                logger.error(f"Error save neighbors tiles on disk: {e}")
    else:
        tasks_save = [
            save_tile_on_disk(
                neighbor.t.x,
                neighbor.t.y,
                neighbor.t.z,
                neighbor.tile_file_name,
                neighbor.buffer,
            )
            for neighbor in neighbors
        ]
        try:
            await asyncio.gather(*tasks_save, return_exceptions=True)
        except Exception as e:
            logger.error(f"Error save neighbors tiles on disk: {e}")


async def post_process_tiles(
    mbtiles: bool,
    mbtiles_db: str,
    nts: List[NeighborTile],
) -> None:
    await save_neighbors_tiles(mbtiles, mbtiles_db, nts)


def raise_exception(code: int, detail: str, level: int) -> None:
    if level == logging.INFO:
        logger.info(detail)
    elif level == logging.WARN:
        logger.warn(detail)
    elif level == logging.ERROR:
        logger.error(detail)

    raise HTTPException(status_code=code, detail=detail)
