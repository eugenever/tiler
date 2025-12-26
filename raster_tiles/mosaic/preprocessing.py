import math
import multiprocessing
import os
import sqlite3
import subprocess
import time
import logging
import logging.config
import warnings
import numpy
import queue

from pathlib import Path
from osgeo import gdal
from typing import Any, Dict, List, Optional, Union, Tuple
from multiprocessing.connection import Connection
from multiprocessing.context import SpawnProcess
from multiprocessing import Manager

from concurrent_log_handler import ConcurrentTimedRotatingFileHandler as _

from rio_tiler.io.rasterio import Reader
from rio_tiler.models import ImageData
from rio_tiler.utils import has_alpha_band
from rio_tiler.errors import EmptyMosaicError, TileOutsideBounds

from morecantile.commons import Tile
from morecantile.defaults import tms

from raster_tiles.utils import (
    encode_raster_to_rgba,
    range_negative_step,
    range_positive_step,
    isfile,
)
from raster_tiles.models.profiles import GlobalMercator
from raster_tiles.defaults import (
    GDAL_RESAMPLING,
    PIXEL_SELECTION_METHOD_TO_MERGE_METHOD,
    Profile,
    Resampling,
    MAXZOOMLEVEL,
)
from server.sqlite_db import optimize_connection, sqlite_db_connect
from server.mbtiles import mbtiles_setup
from raster_tiles.models.tile_details import TileDetail
from raster_tiles.mosaic.reader import mosaic_reader
from raster_tiles.models.tile_job import TileJob
from raster_tiles.mosaic.mosaic_options import MosaicOptions
from raster_tiles.mosaic.merge_with_rio_np import mosaics_merge
from server.tile_utils import reader_single_tile

logger = logging.getLogger(__name__)
logging.config.fileConfig("log_app.ini", disable_existing_loggers=False)


def processing_input_assets(
    options: MosaicOptions, sender: Optional[Connection] = None
) -> None:

    if not options.warnings:
        warnings.filterwarnings("ignore")

    ds_path: str = os.path.join(
        options.data_dir,
        options.datasource_id,
    )
    if not os.path.exists(ds_path):
        os.makedirs(ds_path, exist_ok=True)

    skip_warp_translate: bool = False
    if options.merge:
        method: str = PIXEL_SELECTION_METHOD_TO_MERGE_METHOD[
            options.pixel_selection_method.__name__
        ]
        # merged_raster = os.path.join(
        #     options.assets_dir, f"{options.mosaic}_{method.upper()}.tif"
        # )
        merged_raster = os.path.join(
            options.data_dir,
            options.datasource_id,
            f"{options.mosaic}_{method.upper()}.tif",
        )
        if isfile(merged_raster):
            skip_warp_translate = True

    #########################  MP Start #########################

    exists_warped_rasters: List[bool] = []
    # Checking for the presence of warped rasters to avoid starting a process pool
    for af in options.assets_files:
        ext_af: str = os.path.splitext(af)[1]
        asset_filename: str = Path(af).stem
        # otr: str = os.path.join(
        #     options.assets_dir, f"{asset_filename}_warp_tr_ov{ext_af}"
        # )
        otr: str = os.path.join(
            options.data_dir,
            options.datasource_id,
            f"{asset_filename}_warp_tr_ov{ext_af}",
        )
        if os.path.isfile(otr):
            exists_warped_rasters.append(True)
        else:
            exists_warped_rasters.append(False)

    all_warped_rasters_is_exist: bool = False
    if all(exists_warped_rasters):
        all_warped_rasters_is_exist = True

    if not skip_warp_translate and not all_warped_rasters_is_exist:

        processes: List[SpawnProcess] = []
        in_queues: List[queue.Queue] = []
        mp_context = multiprocessing.get_context("spawn")
        manager = Manager()
        for _ in range(min(options.count_processes, len(options.assets_files))):
            in_queue = manager.Queue(maxsize=1)
            p = mp_context.Process(
                target=mp_warp_translate_addo, args=[options, in_queue]
            )
            in_queues.append(in_queue)
            processes.append(p)

        [p.start() for p in processes]

        try:
            index = 0
            for asset in options.assets_files:
                ext: str = os.path.splitext(asset)[1]
                asset_file_name: str = Path(asset).stem
                # output_translate: str = os.path.join(
                #     options.assets_dir, f"{asset_file_name}_warp_tr_ov{ext}"
                # )
                output_translate: str = os.path.join(
                    options.data_dir,
                    options.datasource_id,
                    f"{asset_file_name}_warp_tr_ov{ext}",
                )
                # if warped translated rasters with overviews exists then skip WARP, TRANSLATE, ADD OVERVIEWS stages
                if os.path.isfile(output_translate):
                    continue

                if index > len(in_queues) - 1:
                    index = 0
                try:
                    in_queues[index].put(
                        (asset, asset_file_name, ext, output_translate), timeout=1200
                    )
                except queue.Full:
                    logger.error(
                        f"Error put asset '{asset}' for warping in queue index={index}"
                    )
                index = index + 1
        except Exception as e:
            logger.error(f"Warping Pool execution error: {e}")
            [p.terminate() for p in processes]
            return None

        [in_queue.put("terminate") for in_queue in in_queues]
        [p.join() for p in processes]

    ###########################  MP End ###########################

    assets_files = options.assets_files
    if options.merge:
        if not skip_warp_translate:
            try:
                merged_asset = mosaics_merge(options)
                gdaladdo(merged_asset, options)
            except Exception as e:
                logger.error(f"'mosaics_merge' or 'gdaladdo': {e}")
                sender.send(None)
                return None
        else:
            merged_asset = merged_raster

        options.merged_asset = merged_asset
        assets_files = [merged_asset]

    zooms_tminmax: Dict[int, List[int]] = {}

    tminz: int = 100
    tmaxz: int = 0
    assets: List[str] = []

    try:
        connection: sqlite3.Connection = sqlite_db_connect(options.tiles_details_db)
        cursor: sqlite3.Cursor = connection.cursor()
        optimize_connection(cursor)

        if not options.merge:
            mosaic_nodata = -9999999.0

        for i, asset in enumerate(assets_files):

            if not options.merge:
                asset_file_name: str = Path(asset).stem
                ext: str = os.path.splitext(asset)[1]
                output_translate: str = os.path.join(
                    options.data_dir,
                    options.datasource_id,
                    f"{asset_file_name}_warp_tr_ov{ext}",
                )
                ds = gdal.Open(asset, gdal.GA_ReadOnly)
                asset_nodata = ds.GetRasterBand(1).GetNoDataValue()
                if asset_nodata is None:
                    asset_nodata = 0.0
                options.nodata_default = max(mosaic_nodata, asset_nodata)
                del ds
            else:
                output_translate = asset

            try:
                zooms, zooms_tminmax_asset = generate_tminmax(
                    output_translate, options, connection, cursor
                )
                if zooms[0] < tminz:
                    tminz = zooms[0]
                if zooms[1] > tmaxz:
                    tmaxz = zooms[1]

                if i == 0:
                    zooms_tminmax = zooms_tminmax_asset
                else:
                    for zoom, tminmax_asset in zooms_tminmax_asset.items():
                        if zoom in zooms_tminmax:
                            # format 'tminmax_asset' => [tminx, tmaxx, tminy, tmaxy]
                            # tminx
                            if zooms_tminmax[zoom][0] > tminmax_asset[0]:
                                zooms_tminmax[zoom][0] = tminmax_asset[0]
                            # tmaxx
                            if zooms_tminmax[zoom][1] < tminmax_asset[1]:
                                zooms_tminmax[zoom][1] = tminmax_asset[1]
                            # tminy
                            if zooms_tminmax[zoom][2] > tminmax_asset[2]:
                                zooms_tminmax[zoom][2] = tminmax_asset[2]
                            # tmaxy
                            if zooms_tminmax[zoom][3] < tminmax_asset[3]:
                                zooms_tminmax[zoom][3] = tminmax_asset[3]
                        else:
                            zooms_tminmax[zoom] = tminmax_asset

            except Exception as e:
                logger.error(str(e))

            if not options.merge:
                assets.append(f"{asset_file_name}_warp_tr_ov{ext}")
            else:
                assets.append(asset)

        if options.tminz is not None:
            tminz = options.tminz
        if options.tmaxz is not None:
            tmaxz = options.tmaxz

        tile_details: Dict[int, List[TileDetail]] = {}

        for tz in range(tmaxz, tminz - 1, -1):
            # Set the bounds
            tminx: int = zooms_tminmax[tz][0]
            tmaxx: int = zooms_tminmax[tz][1]
            tminy: int = zooms_tminmax[tz][2]
            tmaxy: int = zooms_tminmax[tz][3]

            tile_detail_tz: List[TileDetail] = []

            # range include end element
            for ty in range_negative_step(tmaxy, tminy - 1, 3):
                # range include end element
                for tx in range_positive_step(tminx, tmaxx, 3):
                    tile_detail_tz.append(
                        TileDetail(
                            tx=tx,
                            ty=ty,
                            tz=tz,
                        )
                    )
                    cursor.execute(
                        "INSERT INTO tiles_detail (tz, tx, ty) values (?,?,?)",
                        (tz, tx, ty),
                    )
                connection.commit()

            if tile_detail_tz:
                tile_details[tz] = tile_detail_tz
            else:
                tile_details[tz] = []

        # get nodata from merged asset
        if options.merge:
            ds = gdal.Open(merged_asset, gdal.GA_ReadOnly)
            options.nodata_default = ds.GetRasterBand(1).GetNoDataValue()
            del ds

        tile_job: TileJob = TileJob(
            tile_details=tile_details,
            tminz=tminz,
            tmaxz=tmaxz,
            merge=options.merge,
            merged_asset=merged_asset if options.merge else None,
            nodata=options.nodata_default,
        )

        cursor.execute(
            "INSERT INTO tile_job (data_bands_count, nodata, src_file, tile_extension, tile_size, tile_driver, profile, querysize, xyz, in_file, input_file, encode_to_rgba, has_alpha_band, pixel_selection_method, resampling_method, merge) values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?);",
            (
                0,
                options.nodata_default,
                "",
                options.tileext,
                options.tile_size,
                options.tiledriver.name,
                options.profile.name,
                0,
                1 if options.XYZ else 0,
                os.path.basename(
                    os.path.normpath(
                        os.path.join(options.data_dir, options.datasource_id)
                    )
                ),
                os.path.join(options.data_dir, options.datasource_id),
                options.encode_to_rgba,
                0,
                options.pixel_selection_method.__name__,
                options.resampling.name,
                1 if options.merge else 0,
            ),
        )
        connection.commit()

        params_assets = [(a,) for a in assets]
        cursor.executemany("INSERT INTO assets (asset) values (?);", params_assets)
        connection.commit()
    except Exception as e:
        logger.error(f"'processing_input_assets': tile details generate error: {e}")
        sender.send(None)
        return None
    finally:
        connection.close()

    if options.mbtiles:
        if os.path.isfile(options.mbtiles_db):
            try:
                os.remove(options.mbtiles_db)
            except Exception:
                pass

        if not os.path.isfile(options.mbtiles_db):
            try:
                connection: sqlite3.Connection = sqlite_db_connect(options.mbtiles_db)
                cursor: sqlite3.Cursor = connection.cursor()
                mbtiles_setup(cursor)
            except sqlite3.OperationalError:
                pass
            finally:
                connection.commit()
                connection.close()
        else:
            try:
                connection: sqlite3.Connection = sqlite_db_connect(options.mbtiles_db)
                cursor: sqlite3.Cursor = connection.cursor()
                cursor.execute("DELETE FROM tiles;")
                connection.commit()
                cursor.execute("VACUUM;")
                connection.commit()
            except sqlite3.OperationalError:
                pass
            finally:
                connection.commit()
                connection.close()

    if sender:
        sender.send(tile_job)
        return None


def mp_warp_translate_addo(
    mosaic_options: MosaicOptions, in_queue: Optional[multiprocessing.Queue]
):
    warnings.filterwarnings("ignore")

    while True:
        try:
            message = in_queue.get(timeout=10)
            if message and isinstance(message, tuple):
                warp_translate_addo(
                    message[0], message[1], message[2], message[3], mosaic_options
                )
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
                f"'mp_warp_translate_addo': error get message for warping from in_queue: {e}"
            )
            break


def warp_translate_addo(
    asset: str,
    asset_file_name: str,
    ext: str,
    output_translate: str,
    options: MosaicOptions,
):
    warnings.filterwarnings("ignore")

    size: int = os.path.getsize(asset)
    if size >= options.max_file_size:
        co_big_tiff = "BIGTIFF=YES"
        compress = "COMPRESS=LZW"
    else:
        co_big_tiff = "BIGTIFF=NO"
        compress = "COMPRESS=PACKBITS"

    resampling: Any = GDAL_RESAMPLING[options.resampling]

    warp_options: Any = gdal.WarpOptions(
        format="GTiff",
        options=[
            "overwrite",
            "multi",
            "wo",
            "NUM_THREADS=ALL_CPUS",
        ],
        dstSRS=options.t_srs,
        resampleAlg=resampling,
        multithread=True,
    )

    # output_warped_raster: str = os.path.join(
    #     options.assets_dir, f"{asset_file_name}_warp{ext}"
    # )
    output_warped_raster: str = os.path.join(
        options.data_dir, options.datasource_id, f"{asset_file_name}_warp{ext}"
    )
    gdal.Warp(output_warped_raster, asset, options=warp_options)
    logger.info(f"Finish GDAL warp for '{asset}'")
    time.sleep(0.1)

    # gdal_translate
    options_translate: str = (
        f"-of GTiff -r {options.resampling.name} -co TILED=YES -co BLOCKXSIZE={options.tile_size} -co BLOCKYSIZE={options.tile_size} -co INTERLEAVE=BAND -co {compress} -co NUM_THREADS=ALL_CPUS -co {co_big_tiff}"
    )
    ds_translate = gdal.Translate(
        output_translate, output_warped_raster, options=options_translate
    )
    del ds_translate
    time.sleep(0.1)
    if os.path.isfile(output_warped_raster):
        os.remove(output_warped_raster)

    # run gdaladdo only in case MERGE==FALSE (merge assets mosaic disable)
    if not options.merge:
        input_dataset = gdal.Open(output_translate, gdal.GA_ReadOnly)
        count_levels: int = math.ceil(
            math.log2(
                max(
                    input_dataset.RasterXSize // options.tile_size,
                    input_dataset.RasterYSize // options.tile_size,
                    1,
                )
            )
        )
        del input_dataset

        levels: str = " ".join([f"{2**i}" for i in range(1, count_levels + 1)])
        cmd_gdaladdo: str = (
            f"gdaladdo -ro -r {options.resampling.name} {output_translate} {levels}"
        )
        process = subprocess.Popen(
            cmd_gdaladdo, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE
        )
        # wait for the process to terminate
        err: bytes = process.communicate()[1]
        errcode: Union[int, Any] = process.returncode
        if err:
            raise Exception(
                f"'warp_translate_addo': asset dataset '{asset}', gdaladdo terminated with code {errcode}: {err.decode()}"
            )
        logger.info(f"Overviews successfully created for asset: '{asset}'")


def gdaladdo(asset: str, options: MosaicOptions):
    input_dataset = gdal.Open(asset, gdal.GA_ReadOnly)
    count_levels: int = math.ceil(
        math.log2(
            max(
                input_dataset.RasterXSize // options.tile_size,
                input_dataset.RasterYSize // options.tile_size,
                1,
            )
        )
    )
    del input_dataset

    levels: str = " ".join([f"{2**i}" for i in range(1, count_levels + 1)])
    cmd_gdaladdo: str = f"gdaladdo -ro -r {options.resampling.name} {asset} {levels}"
    process = subprocess.Popen(
        cmd_gdaladdo, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE
    )
    # wait for the process to terminate
    err: bytes = process.communicate()[1]
    errcode: Union[int, Any] = process.returncode
    if err:
        raise Exception(
            f"'gdaladdo': asset dataset '{asset}', gdaladdo terminated with code {errcode}: {err.decode()}"
        )
    logger.info(f"Overviews successfully created for asset: '{asset}'")


def generate_tminmax(
    asset: str,
    options: MosaicOptions,
    connection: sqlite3.Connection,
    cursor: sqlite3.Cursor,
) -> Optional[Tuple[List[int], Dict[int, List[int]]]]:
    dataset = gdal.Open(asset, gdal.GA_ReadOnly)
    gt: List[float] = dataset.GetGeoTransform()
    if (gt[2], gt[4]) != (0, 0):
        raise Exception(
            f"'generate_tminmax': georeference of the raster '{asset}' contains rotation or skew"
        )
    xsize: int = dataset.RasterXSize
    ysize: int = dataset.RasterYSize
    del dataset

    # Output Bounds - coordinates in the output SRS
    ominx = gt[0]
    omaxx = gt[0] + xsize * gt[1]
    omaxy = gt[3]
    ominy = gt[3] - ysize * gt[1]

    if options.profile == Profile.mercator:
        zooms: List[int] = []
        zooms_tminmax: Dict[int, List[int]] = {}

        mercator = GlobalMercator(tileSize=options.tile_size)
        for tz in range(0, MAXZOOMLEVEL):
            tminx, tminy = mercator.MetersToTile(ominx, ominy, tz)
            tmaxx, tmaxy = mercator.MetersToTile(omaxx, omaxy, tz)
            # crop tiles extending world limits (+-180,+-90)
            tminx, tminy = max(0, tminx), max(0, tminy)
            tmaxx, tmaxy = min(2**tz - 1, tmaxx), min(2**tz - 1, tmaxy)

            if options.XYZ:
                _tminy = (2**tz - 1) - tmaxy
                _tmaxy = (2**tz - 1) - tminy
                tminy = _tminy
                tmaxy = _tmaxy

            zooms_tminmax[tz] = [tminx, tmaxx, tminy, tmaxy]

            cursor.execute(
                "INSERT INTO tminmax (tz, tminx, tmaxx, tminy, tmaxy, asset) values (?,?,?,?,?,?)",
                (tz, tminx, tmaxx, tminy, tmaxy, os.path.basename(asset)),
            )
            connection.commit()

        # Get the minimal zoom level (map covers area equivalent to one tile)
        tminz = mercator.ZoomForPixelSize(
            gt[1]
            * max(
                xsize,
                ysize,
            )
            / float(options.tile_size)
        )
        # Get the maximal zoom level
        # (closest possible zoom level up on the resolution of raster)
        tmaxz = mercator.ZoomForPixelSize(gt[1])
        zooms = [tminz, tmaxz]

        cursor.execute(
            "INSERT INTO tminz_tmaxz (tminz, tmaxz, asset) values (?,?,?)",
            (tminz, tmaxz, os.path.basename(asset)),
        )
        connection.commit()

        return (zooms, zooms_tminmax)

    return None


def mosaics_tile(
    asset: str,
    options: MosaicOptions,
    tile_detail: TileDetail,
    readers: Dict[str, Reader],
    **kwargs: Any,
) -> Optional[ImageData]:

    if options.resampling == Resampling.antialias:
        logger.error(f"Resampling method `antialias` not supported")
        return None

    # ignore warnings from rio-tiler, rasterio
    if not options.warnings:
        warnings.filterwarnings("ignore")

    try:
        reader: Reader = readers[asset]
        # If has alpha band then use his, non casting mask from rio-tiler
        if has_alpha_band(reader.dataset):
            # Need specified all bands from input raster
            indexes = reader.dataset.indexes
        else:
            indexes = None

        tz: int = tile_detail.tz
        tx: int = tile_detail.tx
        ty: int = tile_detail.ty

        nodata = reader.dataset.nodata
        # for cases when the NODATA is not specified,
        # but is necessary for masked array and correct encoding to RGBA
        if not nodata and options.encode_to_rgba:
            nodata = options.nodata_default

        try:
            image_data: ImageData = reader.tile(
                tx,
                ty,
                tz,
                tilesize=options.tile_size,
                indexes=indexes,
                nodata=nodata,
                **kwargs,
            )
        except TileOutsideBounds:
            return None
        except Exception as e:
            logger.error(f"'mosaics_tile': tile {tx}:{ty}:{tz}, error: {e}")
            return None

        # Discard empty tiles
        if nodata:
            if numpy.all(image_data.data == nodata):
                return None

        return image_data

    except Exception as e:
        logger.error(
            f"'mosaics_tile': create base tile for raster '{options.assets_dir}': {e}"
        )
        return None


def create_base_tile(
    options: MosaicOptions,
    readers: Dict[str, Reader],
    tile_detail: TileDetail,
    queue: Optional[multiprocessing.Queue],
) -> Optional[List[Tuple[bytes, int, int, int]]]:
    # ignore warnings from rio-tiler, rasterio
    if not options.warnings:
        warnings.filterwarnings("ignore")

    try:
        neighbor_tiles: List[Tile] = []
        tms_wmq = tms.get("WebMercatorQuad")
        t = Tile(tile_detail.tx, tile_detail.ty, tile_detail.tz)
        neighbor_tiles = tms_wmq.neighbors(t)
        neighbor_tiles.append(t)
        nts: List[Tuple[bytes, int, int, int]] = []

        for neighbor in neighbor_tiles:
            try:
                if not options.merge:
                    image_data, _ = mosaic_reader(
                        options.warped_assets_files,
                        reader_single_tile,
                        neighbor.x,
                        neighbor.y,
                        neighbor.z,
                        readers,
                        options.tile_size,
                        resampling_method=options.resampling.name,
                        pixel_selection=options.pixel_selection_method,
                    )
                    if options.pixel_selection_method.__name__ == "MeanMethod":
                        if len(readers) > 0:
                            r: Reader = readers.get(options.warped_assets_files[0])
                            if isinstance(r.dataset.nodatavals, tuple):
                                nd = r.dataset.nodatavals[0]
                                if nd is None:
                                    nodata_default = nodata_for_dtype(image_data)
                                    options.nodata_default = nodata_for_dtype(
                                        image_data
                                    )
                                else:
                                    nodata_default = nd
                            mask = numpy.ma.getmask(image_data.array)
                            image_data.data[mask] = nodata_default
                else:
                    image_data = reader_single_tile(
                        options.warped_assets_files[0],
                        neighbor.x,
                        neighbor.y,
                        neighbor.z,
                        readers,
                        options.tile_size,
                        resampling_method=options.resampling.name,
                    )
            except EmptyMosaicError:
                logger.info(
                    f"tile {neighbor.x}:{neighbor.y}:{neighbor.z} outside the mosaic boundaries"
                )
                continue

            if not image_data or image_data.data.size == 0:
                continue

            if options.encode_to_rgba:
                img_data = encode_raster_to_rgba(image_data, options.nodata_default)
                img_data.assets = image_data.assets
                img_data.crs = image_data.crs
                img_data.bounds = image_data.bounds
                img_data.band_names = image_data.band_names
                img_data.metadata = image_data.metadata
                img_data.dataset_statistics = image_data.dataset_statistics
            else:
                img_data = image_data

            # Tile is out of bounds raster OR all data is NODATA, exclude this tiles
            if numpy.all(img_data.data == 0):
                del img_data
                continue

            if options.encode_to_rgba == 1:
                if img_data:
                    # in this case "add_mask" always = False
                    buffer: bytes = img_data.render(
                        img_format=options.tiledriver.name, add_mask=False
                    )
            else:
                buffer = img_data.render(img_format=options.tiledriver.name)

            nts.append((buffer, neighbor.x, neighbor.y, neighbor.z))

        if len(nts) == 0:
            return None

        tileext: str = options.tileext
        output: str = options.datasource_dir

        if options.mbtiles:
            if queue:
                queue.put(nts)
            return nts
        else:
            if queue:
                queue.put(None)

            for n in nts:
                tile_file_name: str = os.path.join(
                    output, str(n[3]), str(n[1]), f"{n[2]}.{tileext}"
                )
                if not os.path.exists(os.path.dirname(tile_file_name)):
                    os.makedirs(os.path.dirname(tile_file_name), exist_ok=True)
                # Write a tile to png on disk
                with open(tile_file_name, "wb") as tile_file:
                    tile_file.write(n[0])
                    tile_file.flush()

    except Exception as e:
        logger.error(
            f"'create_base_tile': create base tile for raster '{options.assets_dir}': {e}"
        )
        return None


def nodata_for_dtype(image_data: ImageData) -> float:
    if image_data.data.dtype in [
        numpy.uint8,
        numpy.uint16,
        numpy.uint32,
        numpy.uint64,
    ]:
        return 0.0

    if image_data.data.dtype == numpy.int8:
        return -128.0
    if image_data.data.dtype == numpy.int16:
        return -32000.0
    if image_data.data.dtype == numpy.float16:
        return -65000.0

    return -99999.0
