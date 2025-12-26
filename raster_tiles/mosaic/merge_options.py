import json
import time
import logging
import os

from pathlib import Path
from typing import Any, List, Optional
from osgeo import gdal
from osgeo_utils.auxiliary.util import GetOutputDriverFor


class UnknownGDALBandTypeException(Exception):
    pass


class MergeOptions(object):

    def __init__(
        self,
        input: str,
        output: Optional[str] = None,
        driver_name: Optional[str] = None,
        separate: Optional[bool] = False,
        copy_pct: Optional[bool] = False,
        verbose: Optional[bool] = False,
        nodata: Optional[float] = None,
        a_nodata: Optional[float] = None,
    ):
        self.verbose: Optional[bool] = verbose

        parent_dir = Path(__file__).parents[2]
        data_dir: str = os.path.join(str(parent_dir), "data")
        mosaics_dir: str = os.path.join(data_dir, "mosaics")
        input_dir: str = os.path.join(mosaics_dir, input)

        # input: folder in data/mosaics with some raster files
        self.input_files: List[str] = [
            os.path.join(input_dir, f)
            for f in os.listdir(input_dir)
            if is_raster_asset(os.path.join(input_dir, f))
        ]

        if driver_name is None:
            if len(self.input_files) > 0:
                self.driver_name: str = GetOutputDriverFor(self.input_files[0])
            else:
                self.driver_name = "GTiff"
        else:
            self.driver_name = driver_name

        # output: raster file name with extension, for example 'coverage_5m.TIF'
        if output:
            self.output: str = os.path.join(data_dir, output)
        else:
            self.output = os.path.join(data_dir, f"{input}.TIF")

        self.ulx: Optional[float] = None
        self.uly: Optional[float] = None
        self.lrx: Optional[float] = None
        self.lry: Optional[float] = None

        # pixel sizes
        self.psize_x: Optional[float] = None
        self.psize_y: Optional[float] = None

        self.separate: Optional[bool] = separate
        self.copy_pct: Optional[bool] = copy_pct
        self.nodata: Optional[float] = nodata
        self.a_nodata: Optional[float] = a_nodata

        # Total size of input raster files in bytes
        self.total_files_size: int = sum([os.path.getsize(f) for f in self.input_files])
        # Max files size 4GB - 300 MB
        max_files_size: int = 1024 * 1024 * 1024 * 4 - 1024 * 1024 * 300
        if self.total_files_size > max_files_size:
            co_big_tiff = "BIGTIFF=YES"
            compress = "COMPRESS=LZW"
        else:
            co_big_tiff = ""
            compress = "COMPRESS=PACKBITS"

        self.create_options: List[str] = [
            "INTERLEAVE=BAND",
            "NUM_THREADS=ALL_CPUS",
            compress,
        ]

        if co_big_tiff:
            self.create_options.append(co_big_tiff)

        self.pre_init: List[float] = []

        """
        GDAL Data type

        Force the output image bands to have a specific data type supported by the driver,
        which may be one of the following: Byte, Int8, UInt16, Int16, UInt32, Int32, UInt64,
        Int64, Float32, Float64, CInt16, CInt32, CFloat32 or CFloat64
        """
        if len(self.input_files) > 0:
            ds = gdal.Open(self.input_files[0])
            rb = ds.GetRasterBand(1)
            band_type: str = gdal.GetDataTypeName(rb.DataType)
            del rb
            del ds

        self.band_type: Any = gdal.GetDataTypeByName(band_type)
        if self.band_type == gdal.GDT_Unknown:
            logging.error(f"Merge mosaic: unknown GDAL data type: {self.band_type}")
            raise UnknownGDALBandTypeException(
                f"Merge mosaic: unknown GDAL data type: {self.band_type}"
            )

        self.createonly: bool = False
        self.target_aligned_pixels: bool = False
        self.start_time: Optional[float] = time.time()

    def toJSON(self):
        return json.dumps(self.__dict__, cls=ComplexEncoder, sort_keys=True)


class ComplexEncoder(json.JSONEncoder):
    def default(self, obj):
        if hasattr(obj, "toJSON"):
            return obj.toJSON()
        else:
            return json.JSONEncoder.default(self, obj)


def is_raster_asset(asset: str) -> bool:
    if not os.path.isfile(asset):
        return False

    ext = os.path.splitext(asset)[-1].lower()
    if ext != ".ovr":
        return True
    else:
        return False
