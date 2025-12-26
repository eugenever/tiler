import asyncio
import logging
import struct
import os
import stat
import sys
import math
import numpy as np

from pathlib import Path
from typing import Any, Optional
from PIL import Image
from nptyping import NDArray, Shape, UInt8
from osgeo import gdal
from nptyping import NDArray
from bestconfig import Config

from raster_tiles.defaults import Resampling, NUMPY_DTYPE
from raster_tiles.models.tile_job import TileJob
from raster_tiles.models.tiles_options import TilesOptions
from raster_tiles.acceleration.acceleration_with_numba import (
    acceleration_encoding_with_numba,
)

from rio_tiler.models import ImageData


logger = logging.getLogger(__name__)


class GDALError(Exception):
    pass


def options_post_processing(input_file: str, options: TilesOptions) -> TilesOptions:
    if not options.title:
        options.title = os.path.basename(input_file)

    if isinstance(options.zoom, (list, tuple)) and len(options.zoom) < 2:
        raise ValueError("Invalid zoom value")

    # Supported options
    if options.resampling == Resampling.average:
        try:
            if gdal.RegenerateOverview:
                pass
        except Exception:
            raise Exception(
                "'average' resampling algorithm is not available. Please use -r 'near' argument or upgrade to newer version of GDAL."
            )

    elif options.resampling == Resampling.antialias:
        try:
            if np:  # pylint:disable=W0125
                pass
        except Exception:
            raise Exception(
                "'antialias' resampling algorithm is not available. Install PIL (Python Imaging Library) and numpy."
            )

    try:
        os.path.basename(input_file).encode("ascii")
    except UnicodeEncodeError:
        full_ascii = False
    else:
        full_ascii = True

    # LC_CTYPE check
    if not full_ascii and "UTF-8" not in os.environ.get("LC_CTYPE", ""):
        logger.info(
            "WARNING: You are running tiling with a LC_CTYPE environment variable that is not UTF-8 compatible"
        )

    # Output the results
    if options.verbose:
        logger.info(f"Options: {options}")
        logger.info(f"Input: {input_file}")
        logger.info(f"GDAL Cache: {(gdal.GetCacheMax() / 1024 / 1024)} MB")

    return options


class UseExceptions(object):
    def __enter__(self):
        self.old_used_exceptions = gdal.GetUseExceptions()
        if not self.old_used_exceptions:
            gdal.UseExceptions()

    def __exit__(self, type, value, tb):
        if not self.old_used_exceptions:
            gdal.DontUseExceptions()


class DividedCache(object):
    def __init__(self, nb_processes):
        self.nb_processes = nb_processes

    def __enter__(self):
        self.gdal_cache_max = gdal.GetCacheMax()
        # Make sure that all processes do not consume more than `gdal.GetCacheMax()`
        gdal_cache_max_per_process = max(
            1024 * 1024, math.floor(self.gdal_cache_max / self.nb_processes)
        )
        set_cache_max(gdal_cache_max_per_process)

    def __exit__(self, type, value, tb):
        # Set the maximum cache back to the original value
        set_cache_max(self.gdal_cache_max)


def set_cache_max(cache_in_bytes: int) -> None:
    # We set the maximum using `SetCacheMax` and `GDAL_CACHEMAX` to support both fork and spawn as multiprocessing start methods.
    # https://github.com/OSGeo/gdal/pull/2112
    os.environ["GDAL_CACHEMAX"] = f"{int(cache_in_bytes / 1024 / 1024)}"
    gdal.SetCacheMax(cache_in_bytes)


def isfile(path: str) -> bool:
    """Wrapper for os.path.isfile() that can work with /vsi files too"""
    if path.startswith("/vsi"):
        stat_res = gdal.VSIStatL(path)
        if stat is None:
            return False
        return stat.S_ISREG(stat_res.mode)
    else:
        return os.path.isfile(path)


def makedirs(path: str) -> None:
    """Wrapper for os.makedirs() that can work with /vsi files too"""
    if path.startswith("/vsi"):
        if gdal.MkdirRecursive(path, 0o755) != 0:
            raise Exception(f"Cannot create {path}")
    else:
        os.makedirs(path, exist_ok=True)


def count_overview_tiles(tile_job: TileJob) -> int:
    tile_number: int = 0
    for tz in range(tile_job.tmaxz - 1, tile_job.tminz - 1, -1):
        tminx, tminy, tmaxx, tmaxy = tile_job.tminmax[tz]
        tile_number += (1 + abs(tmaxx - tminx)) * (1 + abs(tmaxy - tminy))

    return tile_number


def img_to_numpy(im):
    im.load()
    # unpack data
    e = Image._getencoder(im.mode, "raw", im.mode)
    e.setimage(im.im)

    # NumPy buffer for the result
    shape, typestr = Image._conv_type_shape(im)
    data = np.empty(shape, dtype=np.dtype(typestr))
    mem = data.data.cast("B", (data.data.nbytes,))

    bufsize, s, offset = 65536, 0, 0
    while not s:
        l, s, d = e.encode(bufsize)
        mem[offset : offset + len(d)] = d
        offset += len(d)
    if s < 0:
        raise RuntimeError(f"Encoder error {s} in tobytes")
    return data


def is_bigtiff(filename: str) -> bool:
    with open(filename, "rb") as f:
        header = f.read(4)
    byteorder: str = {b"II": "<", b"MM": ">", b"EP": "<"}[header[:2]]
    version: int = struct.unpack(byteorder + "H", header[2:4])[0]
    return version == 43


# Encoding a single tile
def encode_raster_to_rgba(
    image_data: ImageData, nodata: Optional[float]
) -> Optional[ImageData]:
    # To create_base_tile
    """
    if tile_job.options.encode_to_rgba:
        image_data = encode_raster_to_rgba(image_data, tile_job.nodata)
        if image_data:
            # in this case "add_mask" always = False
            buffer: bytes = image_data.render(
                img_format=tile_job.tile_driver.name, add_mask=False
            )
    else:
        buffer = image_data.render(
            img_format=tile_job.tile_driver.name, add_mask=add_mask
        )
    """

    shape: Shape = image_data.array.data.shape
    if shape[0] > 1:
        # Only 1 band with scalar values
        return None

    if nodata is None:
        nodata = -9999999.0

    if np.all(np.abs(image_data.array.data - nodata) < 0.000001):
        rgba_array: NDArray[Any, UInt8] = np.zeros(
            (4, shape[1], shape[2]), dtype=np.uint8
        )
    else:
        rgba_array = np.empty((4, shape[1], shape[2]), dtype=np.uint8)
        acceleration_encoding_with_numba(
            image_data.array.data, nodata, rgba_array, rgba_array
        )

    if rgba_array is not None:
        return ImageData.from_array(rgba_array)

    return None


# Encoding the entire raster on the GPU
def encode_blocks_np_af_to_rgba(input_raster: str, encode_raster: str) -> None:
    """
    Optional encode One Grayscale scalar band to RGBA (4 bands)

    if self.options.encode_to_rgba:
        # ignore warnings from rio-tiler, rasterio
        if not self.options.warnings:
            warnings.filterwarnings("ignore")

        encode_raster = os.path.join(
            self.data_dir, self.in_file, f"{self.in_file}_RGBA{ext}"
        )

        logger.info(f"Run Encoding to RGBA for '{self.input_file}'")
        encode_blocks_np_af_to_rgba(self.input_file, encode_raster)
        self.input_file = encode_raster
    """

    # Local import -> Avoid a lot of memory usage at first on Windows
    import arrayfire as af
    import afnumpy

    bands = 4  # count bands for RGBA

    parent: Path = Path(__file__).parents[1]
    parent_dir: str = str(parent)
    config_path: str = os.path.join(parent_dir, "config_app.json")
    config = Config(config_path)
    x_block_size: int = config.int("tiler.encoding_to_rgba.x_block_size")
    y_block_size: int = config.int("tiler.encoding_to_rgba.y_block_size")

    ds = gdal.Open(input_raster)
    xsize, ysize = ds.RasterXSize, ds.RasterYSize

    band = ds.GetRasterBand(1)
    nodata = band.GetNoDataValue()

    interleave = ds.GetMetadata("IMAGE_STRUCTURE")["INTERLEAVE"]
    driver = gdal.GetDriverByName("GTiff")
    dst_ds = driver.Create(
        encode_raster,
        xsize,
        ysize,
        bands,
        gdal.GDT_Byte,
        options=[f"INTERLEAVE={interleave}"],
    )

    # Copy metadata from Source to Destination raster
    srs_in = ds.GetProjection()
    gt_in = ds.GetGeoTransform()
    area = ds.GetMetadataItem("AREA_OR_POINT")

    dst_ds.SetProjection(srs_in)
    dst_ds.SetGeoTransform(gt_in)
    if area:
        dst_ds.SetMetadataItem("AREA_OR_POINT", area)

    blocks = 0
    for y in range(0, ysize, y_block_size):

        if y + y_block_size < ysize:
            rows = y_block_size
        else:
            rows = ysize - y

        for x in range(0, xsize, x_block_size):

            try:
                if x + x_block_size < xsize:
                    cols = x_block_size
                else:
                    cols = xsize - x

                np_gdal_array = ds.ReadAsArray(x, y, cols, rows).astype(np.float32)
                # adapt layout of memory numpy array from GDAL to ArrayFire
                afnumpy_array = afnumpy.array(
                    np_gdal_array, dtype=np_gdal_array.dtype.char
                )
                # construct ArrayFire Array
                af_array = af.Array(
                    afnumpy_array.d_array, np_gdal_array.shape, np_gdal_array.dtype.char
                )

                # Replace NODATA to 0.0
                if nodata:
                    af.replace(af_array, af_array != nodata, 0.0)

                module = af.abs(af_array)

                # sign in AF return "1" for negative values, 0 otherwise
                sign = af.sign(af_array)
                af.replace(sign, sign != 1, -1)
                af.replace(sign, sign != 0, 1)

                norm = 1.0 - sign * sign
                exponent = af.floor(af.log2(module + norm))
                mantissa = 8388608 + sign * 8388608 * (module / af.pow2(exponent) - 1.0)

                rgba_array = af.constant(0, cols, rows, bands, 1, dtype=af.Dtype.u8)
                rgba_array[:, :, 0] = af.floor(mantissa / 65536)
                rgba_array[:, :, 1] = af.floor(af.mod(mantissa, 65536) / 256)
                rgba_array[:, :, 2] = af.floor(
                    mantissa
                    - af.floor(mantissa / 65536) * 65536
                    - af.floor(af.mod(mantissa, 65536) / 256) * 256
                )
                rgba_array[:, :, 3] = exponent + 128

                np_array_out = np.empty((cols, rows, bands), dtype=np.uint8)
                rgba_array.to_ndarray(np_array_out)
                af.sync()

                raster_array_out = np.transpose(np_array_out, [2, 1, 0])
                dst_ds.WriteArray(raster_array_out, x, y)

            except Exception as e:
                logger.error(f"Encode {input_raster}: {e}")
                logger.error(f"blocks = {blocks}")
                logger.error(f"x = {x}, xsize = {xsize}, y = {y}, ysize = {ysize}")
                logger.error(f"cols = {cols}, rows = {rows}")
                logger.error(f"np_gdal_array.shape = {np_gdal_array.shape}")
                logger.error(f"raster_array_out.shape = {raster_array_out.shape}")
                logger.error(
                    f"dst_ds.RasterXSize = {dst_ds.RasterXSize}, dst_ds.RasterYSize={dst_ds.RasterYSize}"
                )

            del np_gdal_array
            del np_array_out
            del raster_array_out

            blocks += 1

    dst_ds.FlushCache()
    del dst_ds
    del ds


def range_positive_step(start: int, end: int, step: int):
    i = start
    while i < end:
        yield i
        i += step
    yield end


def range_negative_step(start: int, end: int, step: int):
    i = start
    while i > end:
        yield i
        i -= step
    yield end


def numpy_dtype(dt_int: int) -> Any:
    """Return a numpy dtype that matches the band data type."""
    name: str = gdal.GetDataTypeName(dt_int)
    if name in NUMPY_DTYPE:
        return NUMPY_DTYPE[name]
    else:
        raise TypeError(f"GDAL data type '{dt_int}' unknown")


def gdal_type(dtype: Any) -> Any:
    """Return a GDAL type that most closely matches numpy dtype
    Notes
    -----
    Returns GDT_Int32 for np.int64, which may result in overflow.
    """
    if dtype == np.uint8:
        return gdal.GDT_Byte
    elif dtype == np.uint16:
        return gdal.GDT_UInt16
    elif dtype == np.int8:
        return gdal.GDT_Byte  # transform -127 -- 127 to 0 -- 255
    elif dtype == np.int16:
        return gdal.GDT_Int16
    elif (dtype == np.int32) or (dtype == np.int64):
        return gdal.GDT_Int32
    elif dtype == np.float32:
        return gdal.GDT_Float32
    elif dtype == np.float64:
        return gdal.GDT_Float64
    elif dtype == np.complex64:
        return gdal.GDT_CFloat64
    else:
        raise TypeError("GDAL equivalent to type {0} unknown".format(dtype))


def set_event_loop_policy() -> None:
    if sys.implementation.name == "cpython":
        if sys.platform in {"cygwin", "win32"}:
            try:
                import winloop  # type: ignore[import-not-found]
            except ImportError:
                pass
            else:
                try:
                    policy = winloop.EventLoopPolicy()
                except AttributeError:
                    policy = winloop.WinLoopPolicy()
                asyncio.set_event_loop_policy(policy)
                return
        elif sys.platform in {"darwin", "linux"}:
            try:
                import uvloop  # type: ignore[import-not-found]
            except ImportError:
                pass
            else:
                asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
                return
    if sys.platform == "win32":
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
