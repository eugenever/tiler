import numpy
import enum
import json

from osgeo import gdal
from rasterio.enums import Resampling as RioResampling
from rio_tiler.mosaic.methods.defaults import (
    CountMethod,
    HighestMethod,
    LastBandHighMethod,
    LastBandLowMethod,
    LowestMethod,
    FirstMethod,
    MeanMethod,
    MedianMethod,
    StdevMethod,
)


class Resampling(enum.Enum):
    average = 1
    antialias = 2
    nearest = 3
    bilinear = 4
    cubic = 5
    cubicspline = 6
    lanczos = 7
    min = 8
    max = 9
    med = 10
    mode = 11
    gauss = 12
    rms = 13

    def toJSON(self):
        return json.dumps(self.name)


class Profile(enum.Enum):
    mercator = 1
    geodetic = 2
    raster = 3

    def toJSON(self):
        return json.dumps(self.name)


MAXZOOMLEVEL = 19

NUMPY_DTYPE = {
    "Byte": [numpy.uint8, 1],
    "UInt16": [numpy.uint16, 2],
    "Int16": [numpy.int16, 2],
    "UInt32": [numpy.uint32, 4],
    "Int32": [numpy.int32, 4],
    "Float32": [numpy.float32, 4],
    "Float64": [numpy.float64, 8],
    "CInt16": [numpy.complex64, 8],
    "CInt32": [numpy.complex64, 8],
    "CFloat32": [numpy.complex64, 8],
    "CFloat64": [numpy.complex64, 8],
}

GDAL_RESAMPLING = {
    Resampling.nearest: gdal.GRA_NearestNeighbour,
    Resampling.average: gdal.GRA_Average,
    Resampling.bilinear: gdal.GRA_Bilinear,
    Resampling.cubic: gdal.GRA_Cubic,
    Resampling.cubicspline: gdal.GRA_CubicSpline,
    Resampling.lanczos: gdal.GRA_Lanczos,
    Resampling.min: gdal.GRA_Min,
    Resampling.max: gdal.GRA_Max,
    Resampling.med: gdal.GRA_Med,
}

RIO_RESAMPLING = {
    "nearest": RioResampling.nearest,
    "average": RioResampling.average,
    "bilinear": RioResampling.bilinear,
    "cubic": RioResampling.cubic,
    "cubicspline": RioResampling.cubic_spline,
    "lanczos": RioResampling.lanczos,
    "min": RioResampling.min,
    "max": RioResampling.max,
}

PIXEL_SELECTION_METHOD = {
    "FirstMethod": FirstMethod,
    "HighestMethod": HighestMethod,
    "LowestMethod": LowestMethod,
    "MeanMethod": MeanMethod,
    "MedianMethod": MedianMethod,
    "StdevMethod": StdevMethod,
    "LastBandHighMethod": LastBandHighMethod,
    "LastBandLowMethod": LastBandLowMethod,
    "CountMethod": CountMethod,
}

PIXEL_SELECTION_METHOD_TO_MERGE_METHOD = {
    "FirstMethod": "first",
    "HighestMethod": "max",
    "LowestMethod": "min",
    "MeanMethod": "mean",
}

MERGE_MOSAIC_METHODS = ["first", "max", "min", "mean"]

RESAMPLING = {
    "nearest",
    "average",
    "bilinear",
    "cubic",
    "cubicspline",
    "lanczos",
    "min",
    "max",
    "med",
    "mode",
    "gauss",
    "rms",
}
