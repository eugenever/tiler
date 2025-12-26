import math
import time
import logging
import warnings
import numpy as np

from osgeo import gdal
from typing import Any, List, Optional
from time import perf_counter as pc
from multiprocessing import Process

from raster_tiles.mosaic.merge_options import MergeOptions


logger = logging.getLogger(__name__)


def raster_copy(
    ds: Any,
    xoff: int,
    yoff: int,
    xsize: int,
    ysize: int,
    band_n: int,
    dst_ds: Any,
    dst_xoff: int,
    dst_yoff: int,
    dst_xsize: int,
    dst_ysize: int,
    dst_band_n: int,
    nodata: Optional[List[float]],
    verbose: bool = False,
) -> None:

    if verbose:
        logger.info(
            f"Copy {xoff},{yoff},{xsize},{ysize} to {dst_xoff},{dst_yoff},{dst_xsize},{dst_ysize}"
        )

    if nodata is not None:
        return raster_copy_with_nodata(
            ds,
            xoff,
            yoff,
            xsize,
            ysize,
            band_n,
            dst_ds,
            dst_xoff,
            dst_yoff,
            dst_xsize,
            dst_ysize,
            dst_band_n,
            nodata,
        )

    rb: Any = ds.GetRasterBand(band_n)
    rb_mask: Optional[Any] = None

    # Works only in binary mode and doesn't take into account intermediate transparency values for compositing
    if rb.GetMaskFlags() != gdal.GMF_ALL_VALID:
        rb_mask = rb.GetMaskBand()
    elif rb.GetColorInterpretation() == gdal.GCI_AlphaBand:
        rb_mask = rb

    if rb_mask is not None:
        return raster_copy_with_mask(
            ds,
            xoff,
            yoff,
            xsize,
            ysize,
            band_n,
            dst_ds,
            dst_xoff,
            dst_yoff,
            dst_xsize,
            dst_ysize,
            dst_band_n,
            rb_mask,
        )

    rb = ds.GetRasterBand(band_n)
    dst_rb: Any = dst_ds.GetRasterBand(dst_band_n)

    data: np.ndarray[Any, Any] = rb.ReadRaster(
        xoff, yoff, xsize, ysize, dst_xsize, dst_ysize, dst_rb.DataType
    )
    dst_rb.WriteRaster(
        dst_xoff,
        dst_yoff,
        dst_xsize,
        dst_ysize,
        data,
        dst_xsize,
        dst_ysize,
        dst_rb.DataType,
    )


def raster_copy_with_nodata(
    ds: Any,
    xoff: int,
    yoff: int,
    xsize: int,
    ysize: int,
    band_n: int,
    dst_ds: Any,
    dst_xoff: int,
    dst_yoff: int,
    dst_xsize: int,
    dst_ysize: int,
    dst_band_n: int,
    nodata: List[float],
) -> None:

    rb: Any = ds.GetRasterBand(band_n)
    dst_rb: Any = dst_ds.GetRasterBand(dst_band_n)

    data: np.ndarray[Any, Any] = rb.ReadAsArray(
        xoff, yoff, xsize, ysize, dst_xsize, dst_ysize
    )
    dst_data: np.ndarray[Any, Any] = dst_rb.ReadAsArray(
        dst_xoff, dst_yoff, dst_xsize, dst_ysize
    )

    if not np.isnan(nodata):
        nodata_test: np.ndarray[Any, Any] = np.equal(data, nodata)
    else:
        nodata_test = np.isnan(data)

    arr: np.ndarray[Any, Any] = np.choose(nodata_test, (data, dst_data))
    dst_rb.WriteArray(arr, dst_xoff, dst_yoff)


def raster_copy_with_mask(
    ds: Any,
    xoff: int,
    yoff: int,
    xsize: int,
    ysize: int,
    band_n: int,
    dst_ds: Any,
    dst_xoff: int,
    dst_yoff: int,
    dst_xsize: int,
    dst_ysize: int,
    dst_band_n: int,
    rb_mask: Any,
) -> None:
    rb: Any = ds.GetRasterBand(band_n)
    dst_rb: Any = dst_ds.GetRasterBand(dst_band_n)

    data: np.ndarray[Any, Any] = rb.ReadAsArray(
        xoff, yoff, xsize, ysize, dst_xsize, dst_ysize
    )
    mask_data: np.ndarray[Any, Any] = rb_mask.ReadAsArray(
        xoff, yoff, xsize, ysize, dst_xsize, dst_ysize
    )
    dst_data: np.ndarray[Any, Any] = dst_rb.ReadAsArray(
        dst_xoff, dst_yoff, dst_xsize, dst_ysize
    )

    mask_test: np.ndarray[Any, Any] = np.equal(mask_data, 0)
    arr: np.ndarray[Any, Any] = np.choose(mask_test, (data, dst_data))

    dst_rb.WriteArray(arr, dst_xoff, dst_yoff)


class GDALFileInfo(object):
    """A class holding information about a GDAL file"""

    def __init__(self):
        pass

    def init_from_filename(self, filename: str) -> bool:
        """
        Initialize GDALFileInfo from filename
        filename: raster file to read
        Returns True on success or False if the file can't be opened.
        """

        try:
            ds: Any = gdal.Open(filename)
        except OSError as e:
            logger.error(f"Could not GDAL open/read file '{filename}': {e}")
        except Exception as e:
            logger.error(f"Unexpected error GDAL open file '{filename}': {e}")

        if ds is None:
            return False

        self.filename: str = filename
        self.bands: int = ds.RasterCount
        self.xsize: int = ds.RasterXSize
        self.ysize: int = ds.RasterYSize
        self.band_type: str = ds.GetRasterBand(1).DataType
        self.projection: Any = ds.GetProjection()
        self.geotransform: Any = ds.GetGeoTransform()
        self.ulx: float = self.geotransform[0]
        self.uly: float = self.geotransform[3]
        self.lrx: float = self.ulx + self.geotransform[1] * self.xsize
        self.lry: float = self.uly + self.geotransform[5] * self.ysize

        color_table = ds.GetRasterBand(1).GetRasterColorTable()
        if color_table is not None:
            self.color_table = color_table.Clone()
        else:
            self.color_table = None

        return True

    def report(self) -> None:
        logger.info(f"Filename: {self.filename}")
        logger.info(f"File Size: {self.xsize} x {self.ysize} x {self.bands}")
        logger.info(f"Pixel Size: {self.geotransform[1]:f} x {self.geotransform[5]:f}")
        logger.info(
            f"UL: ({self.ulx:f}, {self.uly:f})   LR: ({self.lrx:f}, {self.lry:f})"
        )

    def copy_into(
        self,
        dst_ds: Any,
        nodata: Optional[List[float]],
        band: int = 1,
        dst_band: int = 1,
        verbose: bool = False,
    ) -> None:
        """
        Copy this files image into target file.

        This method will compute the overlap area of the file_info objects
        file, and the target gdal.Dataset object, and copy the image data
        for the common window area.  It is assumed that the files are in
        a compatible projection ... no checking or warping is done.  However,
        if the destination file is a different resolution, or different
        image pixel type, the appropriate resampling and conversions will
        be done (using normal GDAL promotion/demotion rules).

        dst_ds -- gdal.Dataset object for the file into which some or all
        of this file may be copied.

        Returns on success (or if nothing needs to be copied), and zero one
        failure.
        """
        dst_geotransform: Any = dst_ds.GetGeoTransform()
        dst_ulx: float = dst_geotransform[0]
        dst_uly: float = dst_geotransform[3]
        dst_lrx: float = dst_geotransform[0] + dst_ds.RasterXSize * dst_geotransform[1]
        dst_lry: float = dst_geotransform[3] + dst_ds.RasterYSize * dst_geotransform[5]

        # Figure out intersection region
        tgw_ulx: float = max(dst_ulx, self.ulx)
        tgw_lrx: float = min(dst_lrx, self.lrx)
        if dst_geotransform[5] < 0:
            tgw_uly: float = min(dst_uly, self.uly)
            tgw_lry: float = max(dst_lry, self.lry)
        else:
            tgw_uly = max(dst_uly, self.uly)
            tgw_lry = min(dst_lry, self.lry)

        # Do they even intersect
        if tgw_ulx >= tgw_lrx:
            return
        if dst_geotransform[5] < 0 and tgw_uly <= tgw_lry:
            return
        if dst_geotransform[5] > 0 and tgw_uly >= tgw_lry:
            return

        # Compute target window in pixel coordinates
        tw_xoff = int((tgw_ulx - dst_geotransform[0]) / dst_geotransform[1] + 0.1)
        tw_yoff = int((tgw_uly - dst_geotransform[3]) / dst_geotransform[5] + 0.1)
        tw_xsize = (
            int((tgw_lrx - dst_geotransform[0]) / dst_geotransform[1] + 0.5) - tw_xoff
        )
        tw_ysize = (
            int((tgw_lry - dst_geotransform[3]) / dst_geotransform[5] + 0.5) - tw_yoff
        )

        if tw_xsize < 1 or tw_ysize < 1:
            return

        # Compute source window in pixel coordinates
        sw_xoff = int((tgw_ulx - self.geotransform[0]) / self.geotransform[1] + 0.1)
        sw_yoff = int((tgw_uly - self.geotransform[3]) / self.geotransform[5] + 0.1)
        sw_xsize = (
            int((tgw_lrx - self.geotransform[0]) / self.geotransform[1] + 0.5) - sw_xoff
        )
        sw_ysize = (
            int((tgw_lry - self.geotransform[3]) / self.geotransform[5] + 0.5) - sw_yoff
        )

        if sw_xsize < 1 or sw_ysize < 1:
            return

        # Open the source file, and copy the selected region
        ds = gdal.Open(self.filename)

        return raster_copy(
            ds,
            sw_xoff,
            sw_yoff,
            sw_xsize,
            sw_ysize,
            band,
            dst_ds,
            tw_xoff,
            tw_yoff,
            tw_xsize,
            tw_ysize,
            dst_band,
            nodata,
            verbose,
        )


def input_files_info(input_files: List[str]) -> List[GDALFileInfo]:
    """
    Translate a list of GDAL filenames, into GDALFileInfo objects
    input_files: list of valid GDAL dataset files
    Returns a list of GDALFileInfo objects.  There may be less GDALFileInfo objects
    than input_files if some of the files could not be opened as GDAL files.
    """

    infos: List[GDALFileInfo] = []
    for file in input_files:
        info = GDALFileInfo()
        if info.init_from_filename(file):
            infos.append(info)

    return infos


def merge(options: MergeOptions) -> None:

    # Ignore GDAL warnings
    warnings.filterwarnings("ignore")

    if not options.input_files:
        logger.error("Merge mosaic: no input files selected")
        return

    driver: Any = gdal.GetDriverByName(options.driver_name)
    if driver is None:
        logger.error(
            f"Merge mosaic: format driver '{options.driver_name}' not found, pick a supported driver"
        )
        return

    driver_metadata: Any = driver.GetMetadata()
    if "DCAP_CREATE" not in driver_metadata:
        logger.error(
            f"Merge mosaic: format driver '{options.driver_name}' does not support creation and piecewise writing"
        )
        return

    # Collect information on all the source files
    files_info: List[GDALFileInfo] = input_files_info(options.input_files)

    if options.ulx is None:
        ulx: float = files_info[0].ulx
        uly: float = files_info[0].uly
        lrx: float = files_info[0].lrx
        lry: float = files_info[0].lry

        for fi in files_info:
            ulx = min(ulx, fi.ulx)
            uly = max(uly, fi.uly)
            lrx = max(lrx, fi.lrx)
            lry = min(lry, fi.lry)

    if options.psize_x is None:
        psize_x: float = files_info[0].geotransform[1]
        psize_y: float = files_info[0].geotransform[5]

    if options.band_type is None:
        band_type: str = files_info[0].band_type
    else:
        band_type = options.band_type

    # Try opening as an existing file
    with gdal.quiet_errors(), gdal.ExceptionMgr(useExceptions=False):
        dst_ds: Any = gdal.Open(options.output, gdal.GA_Update)

    # Create output file if it does not already exist
    if dst_ds is None:

        if options.target_aligned_pixels:
            ulx = math.floor(ulx / psize_x) * psize_x
            lrx = math.ceil(lrx / psize_x) * psize_x
            lry = math.floor(lry / -psize_y) * -psize_y
            uly = math.ceil(uly / -psize_y) * -psize_y

        geotransform: List[float] = [ulx, psize_x, 0, uly, 0, psize_y]

        xsize: int = int((lrx - ulx) / geotransform[1] + 0.5)
        ysize: int = int((lry - uly) / geotransform[5] + 0.5)

        if options.separate:
            bands: int = 0
            for fi in files_info:
                bands = bands + fi.bands
        else:
            bands = files_info[0].bands

        dst_ds = driver.Create(
            options.output, xsize, ysize, bands, band_type, options.create_options
        )
        if dst_ds is None:
            logger.error(f"Merge mosaic: creation driver for '{options.output}' failed")
            return

        dst_ds.SetGeoTransform(geotransform)
        dst_ds.SetProjection(files_info[0].projection)

        if options.copy_pct:
            dst_ds.GetRasterBand(1).SetRasterColorTable(files_info[0].color_table)
    else:
        if options.separate:
            bands = 0
            for fi in files_info:
                bands = bands + fi.bands
            if dst_ds.RasterCount < bands:
                logger.error(
                    f"Merge mosaic: existing output file '{options.output}' has less bands than the input files. Should delete it before"
                )
                return
        else:
            bands = min(files_info[0].bands, dst_ds.RasterCount)

    # Set nodata value
    if options.a_nodata is not None:
        for i in range(dst_ds.RasterCount):
            dst_ds.GetRasterBand(i + 1).SetNoDataValue(options.a_nodata)

    # Pre-initialize the whole mosaic file to some value
    if options.pre_init is not None:
        if dst_ds.RasterCount <= len(options.pre_init):
            for i in range(dst_ds.RasterCount):
                dst_ds.GetRasterBand(i + 1).Fill(options.pre_init[i])
        elif len(options.pre_init) == 1:
            for i in range(dst_ds.RasterCount):
                dst_ds.GetRasterBand(i + 1).Fill(options.pre_init[0])

    # Copy data from source files into output file
    dst_band: int = 1
    processed: int = 0

    for fi in files_info:
        if options.createonly:
            continue

        if options.verbose:
            logger.info(
                f"Processing file {processed + 1:5d} of {len(files_info):5d}, {processed * 100.0 / len(files_info):6.2f}% completed in {round(time.time() - options.start_time, 5)} secs"
            )
            fi.report()

        if not options.separate:
            for band in range(1, bands + 1):
                fi.copy_into(dst_ds, options.nodata, band, band, options.verbose)
        else:
            for band in range(1, fi.bands + 1):
                fi.copy_into(dst_ds, options.nodata, band, dst_band, options.verbose)
                dst_band = dst_band + 1

        processed = processed + 1

    # Force file to be closed
    dst_ds = None


# Merge mosaic runs in a separate process
def merge_process(options: MergeOptions) -> None:

    st = pc()
    if options.verbose:
        logger.info(f"Begin merge mosaic for '{options.output}'")

    p = Process(target=merge, args=(options,))
    p.start()
    p.join()

    logger.info(f"Time merge mosaic for '{options.output}': {round(pc() - st, 4)} secs")
