import multiprocessing
import os
import sqlite3
import math
import os
import subprocess
import time
import warnings
import logging
import logging.config
import numpy
import numpy.typing as npt
import osgeo.gdal_array as gdalarray

from xml.etree import ElementTree
from pathlib import Path
from multiprocessing.connection import Connection

from osgeo import gdal
from osgeo import osr
from PIL import Image

from typing import Dict, List, Optional, Any, Tuple, Union
from rio_tiler.io.rasterio import Reader
from rio_tiler.utils import has_alpha_band
from morecantile.commons import Tile

from concurrent_log_handler import ConcurrentTimedRotatingFileHandler as _

from raster_tiles.utils import (
    GDALError,
    is_bigtiff,
    isfile,
    range_negative_step,
    range_positive_step,
)
from raster_tiles.models.profiles import GlobalGeodetic, GlobalMercator
from raster_tiles.defaults import (
    GDAL_RESAMPLING,
    MAXZOOMLEVEL,
    Profile,
    Resampling,
)
from raster_tiles.models.tiles_options import TilesOptions
from raster_tiles.models.tile_details import TileDetail, tiles_detail_setup
from raster_tiles.models.tile_job import TileDriver, TileExtension, TileJob
from server.mbtiles import mbtiles_setup
from server.sqlite_db import optimize_connection, sqlite_db_connect, tiler_setup
from server.tile_utils import meta_tile


logger = logging.getLogger(__name__)
logging.config.fileConfig("log_app.ini", disable_existing_loggers=False)


class Gdal2TilesError(Exception):
    pass


class Tiles(object):

    def __init__(self, input_file: str, options: TilesOptions) -> None:

        self.warped_input_dataset: Optional[Any] = None
        self.out_srs: Optional[osr.SpatialReference] = None
        self.nativezoom: Optional[int] = None
        self.tminmax: Optional[List[tuple[int, int, int, int]]] = None
        self.tsize: Optional[List[int]] = None
        self.mercator: Optional[GlobalMercator] = None
        self.geodetic: Optional[GlobalGeodetic] = None
        self.alphaband: Optional[Any] = None
        self.data_bands_count: Optional[int] = None
        self.ominx: Optional[float] = None
        self.omaxx: Optional[float] = None
        self.omaxy: Optional[float] = None
        self.ominy: Optional[float] = None
        self.nodata: Optional[float] = None

        # Tile format
        self.tilesize: int = options.tile_size
        self.tiledriver: TileDriver = TileDriver.PNG
        self.tileext: TileExtension = TileExtension.png

        parent_dir: Path = Path(__file__).parents[1]
        self.parent_dir: str = str(parent_dir)

        temp_dir: str = os.path.join(str(parent_dir), "temp")
        if not os.path.exists(temp_dir):
            os.makedirs(temp_dir, exist_ok=True)
        self.temp_dir: str = temp_dir

        data_dir: str = os.path.join(str(parent_dir), "data")
        if not os.path.exists(data_dir):
            os.makedirs(data_dir, exist_ok=True)
        self.data_dir: str = data_dir

        # How big should be query window be for scaling down
        # Later on reset according the chosen resampling algorightm
        self.querysize: int = 4 * self.tilesize

        base_input_file: str = os.path.basename(input_file)
        self.input_file: str = os.path.join(self.data_dir, base_input_file)

        in_file: str = Path(self.input_file).stem
        self.in_file: str = in_file
        self.tmp_vrt_filename: str = os.path.join(
            self.data_dir, options.datasource_id, f"{in_file}.vrt"
        )

        self.output_folder: str = os.path.join(self.data_dir, options.datasource_id)
        if not os.path.exists(self.output_folder):
            os.makedirs(self.output_folder)

        # if option mbtiles active create mbtiles database file
        self.mbtiles: bool = options.mbtiles
        if options.mbtiles:
            self.mbtiles_db: str = options.mbtiles_db

        options.tiler_db: str = os.path.join(data_dir, "tiler.db")  # type: ignore

        self.tiles_details_db: str = os.path.join(self.output_folder, f"{in_file}.db")
        options.tiles_details_db: str = self.tiles_details_db  # type: ignore

        # create tiles detail database file
        if os.path.isfile(self.tiles_details_db):
            try:
                os.remove(self.tiles_details_db)
            except OSError:
                pass

        connection: sqlite3.Connection = sqlite_db_connect(self.tiles_details_db)
        cursor: sqlite3.Cursor = connection.cursor()
        tiles_detail_setup(cursor)
        connection.commit()
        connection.close()

        if not os.path.isfile(options.tiler_db):
            connection_db: sqlite3.Connection = sqlite_db_connect(options.tiler_db)
            cursor_db: sqlite3.Cursor = connection.cursor()
            tiler_setup(cursor_db)
            connection_db.commit()
            connection_db.close()

        self.options: TilesOptions = options

        if self.options.resampling == Resampling.nearest:
            self.querysize = self.tilesize

        elif self.options.resampling == Resampling.bilinear:
            self.querysize = self.tilesize * 2

        # User specified zoom levels
        self.tminz: Optional[int] = None
        self.tmaxz: Optional[int] = None

        if isinstance(self.options.zoom, (list, tuple)):
            self.tminz = self.options.zoom[0]
            self.tmaxz = self.options.zoom[1]
        elif isinstance(self.options.zoom, int):
            self.tminz = self.options.zoom
            self.tmaxz = self.tminz

    def processing_input(self) -> None:
        """Initialization of the input raster, reprojection if necessary"""

        warnings.filterwarnings("ignore")
        gdal.AllRegister()

        ext: str = os.path.splitext(self.input_file)[1]

        if self.options.warp:
            output: str = os.path.join(
                self.data_dir,
                self.options.datasource_id,
                f"{self.in_file}_WARP_TR_OV{ext}",
            )
        else:
            output = os.path.join(
                self.data_dir, self.options.datasource_id, f"{self.in_file}_TR_OV{ext}"
            )

        # check exist TRANSLATE + OVERVIEW Dataset for input file
        # If exist then open this ready to tiling Dataset and skip translate and generate overviews
        if isfile(output):
            self.input_file = output

        if self.input_file:
            try:
                input_dataset: Any = gdal.Open(self.input_file, gdal.GA_ReadOnly)
            except Exception as e:
                raise Exception(f"Error open input file '{self.input_file}': {e}")
        else:
            raise Exception(f"No input file '{self.input_file}' was specified")

        if self.options.verbose:
            logger.info(
                f"Input file: {input_dataset.RasterXSize}P x {input_dataset.RasterYSize}L - {input_dataset.RasterCount} bands",
            )

        if not input_dataset:
            # Note: GDAL prints the ERROR message too
            raise Exception(
                f"It is not possible to open the input file '{self.input_file}'"
            )

        # Read metadata from the input file
        if input_dataset.RasterCount == 0:
            raise Exception(f"Input file '{self.input_file}' has no raster band")

        rb1: Any = input_dataset.GetRasterBand(1)
        if rb1.GetRasterColorTable():
            raise Exception(
                f"Please convert input file '{self.input_file}' to RGB/RGBA"
            )
        nodata = rb1.GetNoDataValue()
        if nodata is not None:
            self.nodata = nodata

        """
        Preprocessing input GeoTIFF:        
        1) Optional gdalwarp for define resampling method
        2) gdal_translate for internal tiling
        3) gdaladdo for add external overviews
        """

        count_levels: int = math.ceil(
            math.log2(
                max(
                    input_dataset.RasterXSize // self.tilesize,
                    input_dataset.RasterYSize // self.tilesize,
                    1,
                )
            )
        )

        if self.input_file != output:

            if self.options.warp:
                logger.info(f"Run GDAL Warp for '{self.input_file}'")
                # !!! With -tr option big size output tiff
                # gt = input_dataset.GetGeoTransform()
                # pixel_size_x = abs(gt[1])
                # pixel_size_y = abs(gt[5])
                # resolution = (
                #     pixel_size_x if pixel_size_x < pixel_size_y else pixel_size_y
                # )
                # resampling_factor = 2
                # zoom_resolution = closest_zoom_resolution(
                #     resolution / resampling_factor, self.tilesize
                # )
                # if zoom_resolution:
                #     x_res = y_res = zoom_resolution[1]
                #     print(x_res, y_res)
                # else:
                #     x_res = pixel_size_x
                #     y_res = pixel_size_y

                # to gdal.WarpOptions
                # xRes=x_res,
                # yRes=y_res,

                resampling_alg_warp: Any = GDAL_RESAMPLING[self.options.resampling_warp]
                warp_options: Any = gdal.WarpOptions(
                    format="GTiff",
                    options=["overwrite", "multi", "wo", "NUM_THREADS=ALL_CPUS"],
                    dstSRS="EPSG:3857",
                    resampleAlg=resampling_alg_warp,
                    multithread=True,
                )
                output_warp: str = os.path.join(
                    self.data_dir,
                    self.options.datasource_id,
                    f"{self.in_file}_WARP{ext}",
                )
                gdal.Warp(output_warp, self.input_file, options=warp_options)
                self.input_file = output_warp
                logger.info(f"Finish GDAL Warp for '{self.input_file}'")

            logger.info(f"Run GDAL translate for '{self.input_file}'")

            # remove initial dataset
            del rb1
            del input_dataset

            # file size in bytes
            size_input_file: int = os.path.getsize(self.input_file)
            # 4GB - 300MB
            max_file_size: int = 1024 * 1024 * 1024 * 4 - 1024 * 1024 * 300
            co_big_tiff: str = ""
            try:
                if is_bigtiff(self.input_file) or size_input_file > max_file_size:
                    co_big_tiff = " -co BIGTIFF=YES"

                options: str = (
                    f"-of GTiff -r {self.options.resampling.name} -co TILED=YES -co BLOCKXSIZE={self.tilesize} -co BLOCKYSIZE={self.tilesize} -co INTERLEAVE=BAND -co COMPRESS=PACKBITS -co NUM_THREADS=ALL_CPUS{co_big_tiff}"
                )
                ds_translate = gdal.Translate(output, self.input_file, options=options)
                del ds_translate
                time.sleep(0.5)
            except Exception as e:
                logger.error(f"Dataset '{self.input_file}' GDAL translate error: {e}")

            logger.info(f"Finish GDAL translate for '{self.input_file}'")

            logger.info(f"Start create overviews for '{self.input_file}'")
            levels: str = " ".join([f"{2**i}" for i in range(1, count_levels + 1)])
            cmd_gdaladdo: str = f"gdaladdo -ro -r average {output} {levels}"
            process = subprocess.Popen(
                cmd_gdaladdo, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE
            )
            # wait for the process to terminate
            err: bytes = process.communicate()[1]
            errcode: Union[int, Any] = process.returncode
            if err:
                raise Exception(
                    f"Dataset '{self.input_file}' Error run gdaladdo with code {errcode}: {err.decode()}"
                )
            logger.info(f"Overviews successfully created for '{self.input_file}'")
            time.sleep(0.5)

            if (
                self.options.warp
                and ("_WARP." in self.input_file)
                and isfile(self.input_file)
            ):
                os.remove(self.input_file)

            self.input_file = output

        """
        End preprocessing
        """

        input_dataset = gdal.Open(self.input_file, gdal.GA_ReadOnly)
        in_nodata: List[float] = setup_no_data_values(input_dataset, self.options)
        if self.options.verbose:
            logger.info("Preprocessed file:")
            logger.info(
                f"( {input_dataset.RasterXSize}P x {input_dataset.RasterYSize}L - {input_dataset.RasterCount} bands)"
            )

        in_srs, self.in_srs_wkt = setup_input_srs(input_dataset, self.options)
        self.out_srs = setup_output_srs(in_srs, self.options)

        # If input and output reference systems are different, we reproject the input dataset into
        # the output reference system for easier manipulation
        self.warped_input_dataset: Optional[Any] = None

        if self.options.profile in (Profile.mercator, Profile.geodetic):
            if not in_srs:
                raise Exception(f"Input file '{self.input_file}' has unknown SRS.")

            if not has_georeference(input_dataset):
                raise Exception(
                    f"There is no georeference - neither affine transformation (worldfile) for '{self.input_file}'"
                )

            if (in_srs.ExportToProj4() != self.out_srs.ExportToProj4()) or (
                input_dataset.GetGCPCount() != 0
            ):
                self.warped_input_dataset = reproject_dataset(
                    input_dataset, in_srs, self.out_srs, None
                )

                if in_nodata:
                    self.nodata = in_nodata[0]
                    self.warped_input_dataset = update_no_data_values(
                        self.warped_input_dataset,
                        in_nodata,
                        options=self.options,
                        temp_dir=self.temp_dir,
                    )
                else:
                    self.warped_input_dataset = update_alpha_value_for_non_alpha_inputs(
                        self.warped_input_dataset,
                        options=self.options,
                        temp_dir=self.temp_dir,
                    )

            if self.warped_input_dataset and self.options.verbose:
                logger.info(
                    f"Projected file: tiles.vrt - {self.warped_input_dataset.RasterXSize}P x {self.warped_input_dataset.RasterYSize}L - {self.warped_input_dataset.RasterCount} bands",
                )

        if not self.warped_input_dataset:
            self.warped_input_dataset = input_dataset

        self.warped_input_dataset.GetDriver().CreateCopy(
            self.tmp_vrt_filename, self.warped_input_dataset
        )

        # Get alpha band (either directly or from NODATA value)
        self.alphaband = self.warped_input_dataset.GetRasterBand(1).GetMaskBand()
        self.data_bands_count = number_data_bands(self.warped_input_dataset)

        # Read the georeference
        self.out_gt: List[float] = self.warped_input_dataset.GetGeoTransform()

        # Test the size of the pixel

        # Report error in case rotation/skew is in geotransform (possible only in 'raster' profile)
        if (self.out_gt[2], self.out_gt[4]) != (0, 0):
            raise Exception(
                f"Georeference of the raster contains rotation or skew for input file '{self.input_file}'"
            )

        # Here we expect: pixel is square, no rotation on the raster

        # Output Bounds - coordinates in the output SRS
        self.ominx = self.out_gt[0]
        self.omaxx = (
            self.out_gt[0] + self.warped_input_dataset.RasterXSize * self.out_gt[1]
        )
        self.omaxy = self.out_gt[3]
        self.ominy = (
            self.out_gt[3] - self.warped_input_dataset.RasterYSize * self.out_gt[1]
        )
        # Note: maybe round(x, 14) to avoid the gdal_translate behaviour, when 0 becomes -1e-15

        if self.options.verbose:
            logger.info(
                f"Bounds (output srs): {round(self.ominx, 13)}, {self.ominy}, {self.omaxx}, {self.omaxy}"
            )

        connection: sqlite3.Connection = sqlite_db_connect(self.tiles_details_db)
        cursor: sqlite3.Cursor = connection.cursor()
        optimize_connection(cursor)
        # Calculating ranges for tiles in different zoom levels
        if self.options.profile == Profile.mercator:
            self.mercator = GlobalMercator(tileSize=self.tilesize)

            # Generate table with min max tile coordinates for all zoomlevels
            self.tminmax = [(0, 0, 0, 0)] * MAXZOOMLEVEL
            for tz in range(0, MAXZOOMLEVEL):
                tminx, tminy = self.mercator.MetersToTile(self.ominx, self.ominy, tz)
                tmaxx, tmaxy = self.mercator.MetersToTile(self.omaxx, self.omaxy, tz)
                # crop tiles extending world limits (+-180,+-90)
                tminx, tminy = max(0, tminx), max(0, tminy)
                tmaxx, tmaxy = min(2**tz - 1, tmaxx), min(2**tz - 1, tmaxy)
                self.tminmax[tz] = (tminx, tminy, tmaxx, tmaxy)  # type: ignore

                cursor.execute(
                    "INSERT INTO tminmax (tz, tminx, tmaxx, tminy, tmaxy) values (?,?,?,?,?)",
                    (tz, tminx, tmaxx, tminy, tmaxy),
                )
            # TODO: Maps crossing 180E (Alaska?)

            # Get the minimal zoom level (map covers area equivalent to one tile)
            if self.tminz is None:
                self.tminz = self.mercator.ZoomForPixelSize(
                    self.out_gt[1]
                    * max(
                        self.warped_input_dataset.RasterXSize,
                        self.warped_input_dataset.RasterYSize,
                    )
                    / float(self.tilesize)
                )

            # Get the maximal zoom level
            # (closest possible zoom level up on the resolution of raster)
            if self.tmaxz is None:
                self.tmaxz = self.mercator.ZoomForPixelSize(self.out_gt[1])

            if self.options.verbose:
                logger.info(
                    f"Bounds (latitude / longitude): {self.mercator.MetersToLatLon(self.ominx, self.ominy)}, {self.mercator.MetersToLatLon(self.omaxx, self.omaxy)}"
                )
                logger.info(f"Minimum zoom level: {self.tminz}")
                logger.info(
                    f"Maximum zoom level: {self.tmaxz} ({self.mercator.Resolution(self.tmaxz)})"
                )

        if self.options.profile == Profile.geodetic:

            self.geodetic = GlobalGeodetic(
                self.options.tmscompatible, tileSize=self.tilesize
            )

            # Generate table with min max tile coordinates for all zoomlevels
            self.tminmax = [(0, 0, 0, 0)] * MAXZOOMLEVEL
            for tz in range(0, MAXZOOMLEVEL):
                tminx, tminy = self.geodetic.LonLatToTile(self.ominx, self.ominy, tz)
                tmaxx, tmaxy = self.geodetic.LonLatToTile(self.omaxx, self.omaxy, tz)
                # crop tiles extending world limits (+-180,+-90)
                tminx, tminy = max(0, tminx), max(0, tminy)
                tmaxx, tmaxy = min(2 ** (tz + 1) - 1, tmaxx), min(2**tz - 1, tmaxy)
                self.tminmax[tz] = (tminx, tminy, tmaxx, tmaxy)  # type: ignore

                cursor.execute(
                    "INSERT INTO tminmax (tz, tminx, tmaxx, tminy, tmaxy) values (?,?,?,?,?)",
                    (tz, tminx, tmaxx, tminy, tmaxy),
                )

            # TODO: Maps crossing 180E (Alaska?)

            # Get the maximal zoom level
            # (closest possible zoom level up on the resolution of raster)
            if self.tminz is None:
                self.tminz = self.geodetic.ZoomForPixelSize(
                    self.out_gt[1]
                    * max(
                        self.warped_input_dataset.RasterXSize,
                        self.warped_input_dataset.RasterYSize,
                    )
                    / float(self.tilesize)
                )

            # Get the maximal zoom level
            # (closest possible zoom level up on the resolution of raster)
            if self.tmaxz is None:
                self.tmaxz = self.geodetic.ZoomForPixelSize(self.out_gt[1])

            if self.options.verbose:
                print(
                    "Bounds (latlong):", self.ominx, self.ominy, self.omaxx, self.omaxy
                )

        if self.options.profile == Profile.raster:

            def log2(x):
                return math.log10(x) / math.log10(2)

            self.nativezoom = int(
                max(
                    math.ceil(
                        log2(
                            self.warped_input_dataset.RasterXSize / float(self.tilesize)
                        )
                    ),
                    math.ceil(
                        log2(
                            self.warped_input_dataset.RasterYSize / float(self.tilesize)
                        )
                    ),
                )
            )

            if self.options.verbose:
                logger.info(f"Native zoom of the raster: {self.nativezoom}")

            # Get the minimal zoom level (whole raster in one tile)
            if self.tminz is None:
                self.tminz = 0

            # Get the maximal zoom level (native resolution of the raster)
            if self.tmaxz is None:
                self.tmaxz = self.nativezoom

            # Generate table with min max tile coordinates for all zoomlevels
            self.tminmax = [(0, 0, 0, 0)] * (self.tmaxz + 1)
            self.tsize = list(range(0, self.tmaxz + 1))
            for tz in range(0, self.tmaxz + 1):
                tsize = 2.0 ** (self.nativezoom - tz) * self.tilesize
                tminx, tminy = 0, 0
                tmaxx = (
                    int(math.ceil(self.warped_input_dataset.RasterXSize / tsize)) - 1
                )
                tmaxy = (
                    int(math.ceil(self.warped_input_dataset.RasterYSize / tsize)) - 1
                )
                self.tsize[tz] = math.ceil(tsize)
                self.tminmax[tz] = (tminx, tminy, tmaxx, tmaxy)  # type: ignore

                cursor.execute(
                    "INSERT INTO tminmax (tz, tminx, tmaxx, tminy, tmaxy) values (?,?,?,?,?)",
                    (tz, tminx, tmaxx, tminy, tmaxy),
                )

        connection.commit()
        connection.close()

    def generate_metadata(self, cursor: sqlite3.Cursor) -> None:

        if self.options.profile == Profile.mercator:
            south, west = self.mercator.MetersToLatLon(self.ominx, self.ominy)
            north, east = self.mercator.MetersToLatLon(self.omaxx, self.omaxy)
            south, west = max(-85.05112878, south), max(-180.0, west)
            north, east = min(85.05112878, north), min(180.0, east)
            self.swne = (south, west, north, east)

            metadata = {
                "name": self.options.title.replace('"', '\\"'),
                "description": self.options.title,
                "version": "1.0.0",
                "attribution": "",
                "type": "overlay",
                "format": self.tileext.name,
                "minzoom": self.tminz,
                "maxzoom": self.tmaxz,
                "bounds": f"{south} {west} {north} {east}",
                "scale": "1",
                "profile": Profile.mercator.name,
            }

            for n, v in metadata.items():
                cursor.execute("INSERT INTO metadata (name,value) values (?,?)", (n, v))

    def preprocessing_base_tiles(self) -> TileJob:
        """
        Generation of the base tiles (the lowest in the pyramid) directly from the input raster
        """

        connection: sqlite3.Connection = sqlite_db_connect(self.tiles_details_db)
        cursor: sqlite3.Cursor = connection.cursor()

        if self.options.verbose:
            logger.info(f"Generating Base Tiles for input file '{self.input_file}'")

        ds: Optional[Any] = self.warped_input_dataset
        tilebands: int = self.data_bands_count + 1
        querysize: int = self.querysize

        if self.options.verbose:
            logger.info(
                f"Data bands count for input file '{self.input_file}': {self.data_bands_count}"
            )
            logger.info(f"Tile bands for input file '{self.input_file}': {tilebands}")

        tile_details: Dict[int, List[TileDetail]] = {}

        for tz in range(self.tmaxz, self.tminz - 1, -1):  # type: ignore
            # Set the bounds
            tminx: int = self.tminmax[tz][0]
            tminy: int = self.tminmax[tz][1]
            tmaxx: int = self.tminmax[tz][2]
            tmaxy: int = self.tminmax[tz][3]

            tcount: int = (1 + abs(tmaxx - tminx)) * (1 + abs(tmaxy - tminy))
            ti: int = 0

            tile_detail_tz: List[TileDetail] = []

            # range include end element
            for ty in range_negative_step(tmaxy, tminy - 1, 3):
                # range include end element
                for tx in range_positive_step(tminx, tmaxx, 3):
                    ti += 1

                    if self.options.XYZ:
                        ty_xyz = (2**tz - 1) - ty
                    else:
                        ty_xyz = ty

                    if self.options.verbose:
                        tilefilename: str = os.path.join(
                            self.output_folder,
                            str(tz),
                            str(tx),
                            f"{ty_xyz}.{self.tileext}",
                        )
                        logger.info(f"{ti} / {tcount} {tilefilename}")

                    if self.options.profile == Profile.mercator:
                        # Tile bounds in EPSG:3857
                        b: tuple[float, float, float, float] = self.mercator.TileBounds(
                            tx, ty, tz
                        )
                    elif self.options.profile == Profile.geodetic:
                        b = self.geodetic.TileBounds(tx, ty, tz)

                    # Don't scale up by nearest neighbour, better change the querysize
                    # to the native resolution (and return smaller query tile) for scaling

                    if self.options.profile in (Profile.mercator, Profile.geodetic):
                        rb, wb = self.geo_query(ds, b[0], b[3], b[2], b[1])

                        # Pixel size in the raster covering query geo extent
                        nativesize: int = wb[0] + wb[2]
                        if self.options.verbose:
                            logger.info(
                                f"Native Extent (querysize {nativesize}): {rb}, {wb}"
                            )

                        # Tile bounds in raster coordinates for ReadRaster query
                        rb, wb = self.geo_query(
                            ds, b[0], b[3], b[2], b[1], querysize=querysize
                        )

                        rx, ry, rxsize, rysize = rb
                        wx, wy, wxsize, wysize = wb

                    else:  # Profile.raster profile:

                        # tilesize in raster coordinates for actual zoom
                        tsize: int = int(self.tsize[tz])
                        xsize: int = self.warped_input_dataset.RasterXSize
                        # size of the raster in pixels
                        ysize: int = self.warped_input_dataset.RasterYSize
                        if tz >= self.nativezoom:
                            querysize = self.tilesize

                        rx = tx * tsize
                        rxsize = 0
                        if tx == tmaxx:
                            rxsize = xsize % tsize
                        if rxsize == 0:
                            rxsize = tsize

                        rysize = 0
                        if ty == tmaxy:
                            rysize = ysize % tsize
                        if rysize == 0:
                            rysize = tsize
                        ry = ysize - (ty * tsize) - rysize

                        wx = 0
                        wy = 0
                        wxsize = int(rxsize / float(tsize) * self.tilesize)
                        wysize = int(rysize / float(tsize) * self.tilesize)
                        if wysize != self.tilesize:
                            wy = self.tilesize - wysize

                    # Read the source raster if anything is going inside the tile as per the computed
                    # geo_query
                    tile_detail_tz.append(
                        TileDetail(
                            tx=tx,
                            ty=ty,
                            tz=tz,
                            rx=rx,
                            ry=ry,
                            rxsize=rxsize,
                            rysize=rysize,
                            wx=wx,
                            wy=wy,
                            wxsize=wxsize,
                            wysize=wysize,
                            querysize=querysize,
                        )
                    )
                    if self.options.save_tile_detail_db:
                        cursor.execute(
                            "INSERT INTO tiles_detail (tz, tx, ty, rx, ry, rxsize, rysize, wx, wy, wxsize, wysize, querysize) values (?,?,?,?,?,?,?,?,?,?,?,?)",
                            (
                                tz,
                                tx,
                                ty,
                                rx,
                                ry,
                                rxsize,
                                rysize,
                                wx,
                                wy,
                                wxsize,
                                wysize,
                                querysize,
                            ),
                        )
            if tile_detail_tz:
                tile_details[tz] = tile_detail_tz
            else:
                tile_details[tz] = []

        is_has_alpha_band = 0
        reader: Reader = Reader(self.input_file)
        # If has alpha band then use his, non casting mask from rio-tiler
        if has_alpha_band(reader.dataset):
            is_has_alpha_band = 1
        del reader

        tile_job: TileJob = TileJob(
            src_file=self.tmp_vrt_filename,
            nb_data_bands=self.data_bands_count,
            nodata=self.nodata,
            output_folder=os.path.join(
                str(Path(__file__).parents[1]), "tiles", self.options.datasource_id
            ),
            tile_extension=self.tileext,
            tile_driver=self.tiledriver,
            tile_size=self.tilesize,
            tminmax=self.tminmax,
            tminz=self.tminz,
            tmaxz=self.tmaxz,
            in_srs_wkt=self.in_srs_wkt,
            out_geo_trans=self.out_gt,
            ominy=self.ominy,
            options=self.options,
            tile_details=tile_details,
            in_file=self.in_file,
            input_file=self.input_file,
            encode_to_rgba=self.options.encode_to_rgba,
            has_alpha_band=is_has_alpha_band,
        )

        xyz: int = 1
        if not self.options.XYZ:
            xyz = 0

        cursor.execute(
            "INSERT INTO tile_job (data_bands_count, nodata, src_file, tile_extension, tile_size, tile_driver, profile, querysize, xyz, in_file, input_file, encode_to_rgba, has_alpha_band, resampling_method) values (?,?,?,?,?,?,?,?,?,?,?,?,?,?);",
            (
                self.data_bands_count,
                self.nodata,
                self.tmp_vrt_filename,
                self.tileext.name,
                self.tilesize,
                self.tiledriver.name,
                self.options.profile.name,
                querysize,
                xyz,
                self.in_file,
                self.input_file,
                self.options.encode_to_rgba,
                is_has_alpha_band,
                self.options.resampling.name,
            ),
        )
        connection.commit()
        connection.close()

        return tile_job

    def geo_query(
        self,
        ds: Any,
        ulx: float,
        uly: float,
        lrx: float,
        lry: float,
        querysize: int = 0,
    ) -> tuple[tuple[int, int, int, int], tuple[int, int, int, int]]:
        """
        For given dataset and query in cartographic coordinates returns parameters for ReadRaster()
        in raster coordinates and x/y shifts (for border tiles). If the querysize is not given, the
        extent is returned in the native resolution of dataset ds.

        raises Gdal2TilesError if the dataset does not contain anything inside this geo_query
        """
        geotran: Any = ds.GetGeoTransform()
        rx: int = int((ulx - geotran[0]) / geotran[1] + 0.001)
        ry: int = int((uly - geotran[3]) / geotran[5] + 0.001)
        rxsize: int = int((lrx - ulx) / geotran[1] + 0.5)
        rysize: int = int((lry - uly) / geotran[5] + 0.5)

        if not querysize:
            wxsize, wysize = rxsize, rysize
        else:
            wxsize, wysize = querysize, querysize

        # Coordinates should not go out of the bounds of the raster
        wx: int = 0
        if rx < 0:
            rxshift = abs(rx)
            wx = int(wxsize * (float(rxshift) / rxsize))
            wxsize = wxsize - wx
            rxsize = rxsize - int(rxsize * (float(rxshift) / rxsize))
            rx = 0
        if rx + rxsize > ds.RasterXSize:
            wxsize = int(wxsize * (float(ds.RasterXSize - rx) / rxsize))
            rxsize = ds.RasterXSize - rx

        wy: int = 0
        if ry < 0:
            ryshift = abs(ry)
            wy = int(wysize * (float(ryshift) / rysize))
            wysize = wysize - wy
            rysize = rysize - int(rysize * (float(ryshift) / rysize))
            ry = 0
        if ry + rysize > ds.RasterYSize:
            wysize = int(wysize * (float(ds.RasterYSize - ry) / rysize))
            rysize = ds.RasterYSize - ry

        return (rx, ry, rxsize, rysize), (wx, wy, wxsize, wysize)


def closest_zoom_resolution(
    resolution: float, tilesize: int, max_zoom: int = 20
) -> Optional[tuple[int, float]]:

    if resolution < 0:
        logger.info(f"Resolution is less than 0: {resolution}")
        return None

    equator_length = 40075.016686 * 1000

    min_resolution = equator_length / tilesize
    latitude = 0
    zoom = 0

    zoom_resolution = (min_resolution * math.cos(latitude)) / (2**zoom)
    if resolution > zoom_resolution:
        print(
            f"Resolution must be less than {zoom_resolution}. Now it is equal {resolution}"
        )
        return None

    while zoom <= max_zoom:
        zoom_resolution = (min_resolution * math.cos(latitude)) / (2**zoom)
        if zoom_resolution <= resolution:
            return (zoom, zoom_resolution)
        zoom += 1

    return (zoom - 1, zoom_resolution)


def setup_no_data_values(input_dataset: Any, options: Any) -> List[float]:
    """
    Extract the NODATA values from the dataset or use the passed arguments as override if any
    """
    nodata: List[float] = []
    if options.nodata:
        nds = list(map(float, options.nodata.split(",")))
        if len(nds) < input_dataset.RasterCount:
            nodata = (nds * input_dataset.RasterCount)[: input_dataset.RasterCount]
        else:
            nodata = nds
    else:
        for i in range(1, input_dataset.RasterCount + 1):
            raster_no_data = input_dataset.GetRasterBand(i).GetNoDataValue()
            if raster_no_data is not None:
                nodata.append(raster_no_data)

    if options.verbose:
        logger.info(f"NODATA: {nodata}")

    return nodata


def setup_input_srs(input_dataset: Any, options: Any) -> Any:
    """
    Determines and returns the Input Spatial Reference System (SRS) as an osr object and as a
    WKT representation

    Uses in priority the one passed in the command line arguments. If None, tries to extract them
    from the input dataset
    """

    input_srs = None
    input_srs_wkt = None

    if options.s_srs:
        input_srs = osr.SpatialReference()
        input_srs.SetFromUserInput(options.s_srs)
        input_srs_wkt = input_srs.ExportToWkt()
    else:
        input_srs_wkt = input_dataset.GetProjection()

        if not input_srs_wkt and input_dataset.GetGCPCount() != 0:
            input_srs_wkt = input_dataset.GetGCPProjection()
        if input_srs_wkt:
            input_srs = osr.SpatialReference()
            input_srs.ImportFromWkt(input_srs_wkt)

    return input_srs, input_srs_wkt


def setup_output_srs(
    input_srs: osr.SpatialReference, options: Any
) -> osr.SpatialReference:
    """
    Setup the desired SRS (based on options)
    """
    output_srs = osr.SpatialReference()

    if options.profile == Profile.mercator:
        output_srs.ImportFromEPSG(3857)
    elif options.profile == Profile.geodetic:
        output_srs.ImportFromEPSG(4326)
    else:
        output_srs = input_srs

    return output_srs


def has_georeference(dataset: Any) -> bool:
    return (  # type: ignore
        dataset.GetGeoTransform() != (0.0, 1.0, 0.0, 0.0, 0.0, 1.0)
        or dataset.GetGCPCount() != 0
    )


def reproject_dataset(
    from_dataset: Any, from_srs: Any, to_srs: Any, options: Any
) -> Any:
    """
    Returns the input dataset in the expected "destination" SRS.
    If the dataset is already in the correct SRS, returns it unmodified
    """
    if not from_srs or not to_srs:
        raise GDALError(f"From and to SRS must be defined to reproject the dataset")

    if (from_srs.ExportToProj4() != to_srs.ExportToProj4()) or (
        from_dataset.GetGCPCount() != 0
    ):
        to_dataset = gdal.AutoCreateWarpedVRT(
            from_dataset, from_srs.ExportToWkt(), to_srs.ExportToWkt()
        )

        if options and options.verbose:
            logger.info(
                "Warping of the raster by AutoCreateWarpedVRT (result saved into 'tiles.vrt')"
            )
            to_dataset.GetDriver().CreateCopy("tiles.vrt", to_dataset)

        return to_dataset

    return from_dataset


def add_gdal_warp_options_to_string(
    vrt_string: str, warp_options: dict[str, str]
) -> str:
    if not warp_options:
        return vrt_string

    vrt_root = ElementTree.fromstring(vrt_string)
    options = vrt_root.find("GDALWarpOptions")

    if options is None:
        return vrt_string

    for key, value in warp_options.items():
        tb = ElementTree.TreeBuilder()
        tb.start("Option", {"name": key})
        tb.data(value)
        tb.end("Option")
        elem = tb.close()
        options.insert(0, elem)

    return ElementTree.tostring(vrt_root).decode()


def update_no_data_values(
    warped_vrt_dataset: Any,
    nodata_values: List[float],
    options: TilesOptions,
    temp_dir: str,
) -> Any:
    """
    Takes an array of NODATA values and forces them on the WarpedVRT file dataset passed
    """
    # TODO: gbataille - Seems that I forgot tests there
    if nodata_values != []:
        temp_file: str = temp_dir + "/" + options.title + "-no_data_update.vrt"
        warped_vrt_dataset.GetDriver().CreateCopy(temp_file, warped_vrt_dataset)
        with open(temp_file, "r") as f:
            vrt_string: str = f.read()

        vrt_string = add_gdal_warp_options_to_string(
            vrt_string, {"INIT_DEST": "NO_DATA", "UNIFIED_SRC_NODATA": "YES"}
        )

        # save the corrected VRT
        with open(temp_file, "w") as f:
            f.write(vrt_string)

        corrected_dataset: Any = gdal.Open(temp_file)
        os.unlink(temp_file)

        # set NODATA_VALUE metadata
        corrected_dataset.SetMetadataItem(
            "NODATA_VALUES", " ".join([str(i) for i in nodata_values])
        )

        if options and options.verbose:
            logger.info("Modified warping result saved into 'tiles1.vrt'")
            # TODO: gbataille - test replacing that with a gdal write of the dataset (more
            # accurately what's used, even if should be the same
            with open("tiles1.vrt", "w") as f:
                f.write(vrt_string)

        return corrected_dataset


def add_alpha_band_to_string_vrt(vrt_string: str) -> str:
    # TODO: gbataille - Old code speak of this being equivalent to gdalwarp -dstalpha
    # To be checked

    vrt_root: ElementTree.Element = ElementTree.fromstring(vrt_string)

    index: int = 0
    nb_bands: int = 0
    for subelem in list(vrt_root):
        if subelem.tag == "VRTRasterBand":
            nb_bands += 1
            color_node: Optional[ElementTree.Element] = subelem.find("./ColorInterp")
            if color_node is not None and color_node.text == "Alpha":
                raise Exception("Alpha band already present")
        else:
            if nb_bands:
                # This means that we are one element after the Band definitions
                break

        index += 1

    tb: ElementTree.TreeBuilder = ElementTree.TreeBuilder()
    tb.start(
        "VRTRasterBand",
        {
            "dataType": "Byte",
            "band": str(nb_bands + 1),
            "subClass": "VRTWarpedRasterBand",
        },
    )
    tb.start("ColorInterp", {})
    tb.data("Alpha")
    tb.end("ColorInterp")
    tb.end("VRTRasterBand")
    elem: ElementTree.Element = tb.close()

    vrt_root.insert(index, elem)

    warp_options: Optional[ElementTree.Element] = vrt_root.find(".//GDALWarpOptions")
    tb = ElementTree.TreeBuilder()
    tb.start("DstAlphaBand", {})
    tb.data(str(nb_bands + 1))
    tb.end("DstAlphaBand")
    elem = tb.close()
    if warp_options:
        warp_options.append(elem)

    # TODO: gbataille - this is a GDALWarpOptions. Why put it in a specific place?
    tb = ElementTree.TreeBuilder()
    tb.start("Option", {"name": "INIT_DEST"})
    tb.data("0")
    tb.end("Option")
    elem = tb.close()
    if warp_options:
        warp_options.append(elem)

    return ElementTree.tostring(vrt_root).decode()


def update_alpha_value_for_non_alpha_inputs(
    warped_vrt_dataset: Any, options: TilesOptions, temp_dir: str
) -> Any:
    """
    Handles dataset with 1 or 3 bands, i.e. without alpha channel, in the case the nodata value has
    not been forced by options
    """
    if warped_vrt_dataset.RasterCount in [1, 3]:
        tempfilename: str = temp_dir + "/" + options.title + "_alpha_value.vrt"
        warped_vrt_dataset.GetDriver().CreateCopy(tempfilename, warped_vrt_dataset)
        with open(tempfilename) as f:
            orig_data = f.read()
        alpha_data: str = add_alpha_band_to_string_vrt(orig_data)
        with open(tempfilename, "w") as f:
            f.write(alpha_data)

        warped_vrt_dataset = gdal.Open(tempfilename)
        os.unlink(tempfilename)

        if options and options.verbose:
            logger.info("Modified -dstalpha warping result saved into 'tiles1.vrt'")
            # TODO: gbataille - test replacing that with a gdal write of the dataset (more
            # accurately what's used, even if should be the same
            with open("tiles1.vrt", "w") as f:
                f.write(alpha_data)

    return warped_vrt_dataset


def number_data_bands(dataset: Any) -> int:
    """
    Return the number of data (non-alpha) bands of a gdal dataset
    """
    alphaband = dataset.GetRasterBand(1).GetMaskBand()
    if (
        (alphaband.GetMaskFlags() & gdal.GMF_ALPHA)
        or dataset.RasterCount == 4
        or dataset.RasterCount == 2
    ):
        return dataset.RasterCount - 1  # type: ignore
    else:
        return dataset.RasterCount  # type: ignore


def scale_query_to_tile(
    dsquery: Any,
    dstile: Any,
    tiledriver: TileDriver,
    options: TilesOptions,
    tilefilename: str = "",
) -> None:
    """Scales down query dataset to the tile dataset"""

    querysize: int = dsquery.RasterXSize
    tilesize: int = dstile.RasterXSize
    tilebands: int = dstile.RasterCount

    if options.resampling == Resampling.average:
        # Function: gdal.RegenerateOverview()
        for i in range(1, tilebands + 1):
            # Black border around NODATA
            res = gdal.RegenerateOverview(
                dsquery.GetRasterBand(i),
                dstile.GetRasterBand(i),
                options.resampling.name,
            )

            if res != 0:
                raise Exception(
                    f"RegenerateOverview() failed on {tilefilename}, error {res}"
                )

    elif options.resampling == Resampling.antialias:
        # Scaling by PIL (Python Imaging Library) - improved Lanczos
        array: npt.NDArray[numpy.uint8] = numpy.zeros(
            (querysize, querysize, tilebands), numpy.uint8
        )
        for i in range(tilebands):
            array[:, :, i] = gdalarray.BandReadAsArray(
                dsquery.GetRasterBand(i + 1), 0, 0, querysize, querysize
            )

        image_array: Image.Image = Image.fromarray(array, "RGBA")  # Always four bands
        img = image_array.resize((tilesize, tilesize), Image.ANTIALIAS)

        if os.path.exists(tilefilename):
            im0: Image.Image = Image.open(tilefilename)
            im1: Image.Image = Image.composite(img, im0, img)
            im1.save(tilefilename, tiledriver.name)

    else:

        if options.resampling == Resampling.nearest:
            gdal_resampling = gdal.GRA_NearestNeighbour
        elif options.resampling == Resampling.bilinear:
            gdal_resampling = gdal.GRA_Bilinear
        elif options.resampling == Resampling.cubic:
            gdal_resampling = gdal.GRA_Cubic
        elif options.resampling == Resampling.cubicspline:
            gdal_resampling = gdal.GRA_CubicSpline
        elif options.resampling == Resampling.lanczos:
            gdal_resampling = gdal.GRA_Lanczos
        else:
            gdal_resampling = gdal.GRA_NearestNeighbour

        # Other algorithms are implemented by gdal.ReprojectImage().
        dsquery.SetGeoTransform(
            (
                0.0,
                tilesize / float(querysize),
                0.0,
                0.0,
                0.0,
                tilesize / float(querysize),
            )
        )
        dstile.SetGeoTransform((0.0, 1.0, 0.0, 0.0, 0.0, 1.0))

        res = gdal.ReprojectImage(dsquery, dstile, None, None, gdal_resampling)
        if res != 0:
            raise Exception(f"ReprojectImage() failed on {tilefilename}, error {res}")


def tile_job_details(
    input_file: str, options: TilesOptions, sender: Optional[Connection] = None
) -> Optional[TileJob]:
    try:
        tiles: Tiles = Tiles(input_file, options)
        tiles.processing_input()
    except Exception as e:
        logger.error(f"Error of processing input raster '{input_file}': {e}")
        return None

    if tiles.mbtiles:
        if os.path.isfile(tiles.mbtiles_db):
            try:
                os.remove(tiles.mbtiles_db)
            except OSError:
                pass

        if not os.path.isfile(tiles.mbtiles_db):
            try:
                connection: sqlite3.Connection = sqlite_db_connect(tiles.mbtiles_db)
                cursor: sqlite3.Cursor = connection.cursor()
                mbtiles_setup(cursor)
            except sqlite3.OperationalError:
                pass
            finally:
                tiles.generate_metadata(cursor)
                connection.commit()
                connection.close()
        else:
            try:
                connection: sqlite3.Connection = sqlite_db_connect(tiles.mbtiles_db)
                cursor: sqlite3.Cursor = connection.cursor()
                cursor.execute("DELETE FROM tiles;")
                connection.commit()
                cursor.execute("DELETE FROM metadata;")
                connection.commit()
                cursor.execute("VACUUM;")
                connection.commit()
            except sqlite3.OperationalError:
                pass
            finally:
                tiles.generate_metadata(cursor)
                connection.commit()
                connection.close()

    try:
        tile_job: TileJob = tiles.preprocessing_base_tiles()
    except Exception as e:
        logger.error(
            f"Error of preprocessing base tiles for raster '{input_file}': {e}"
        )
        return None

    if sender:
        sender.send(tile_job)
        return None

    return tile_job


def create_base_tile(
    tile_job: TileJob, tile_detail: TileDetail, queue: Optional[multiprocessing.Queue]
) -> Optional[List[Tuple[bytes, int, int, int]]]:

    if tile_job.options.resampling == Resampling.antialias:
        logger.error(f"Resampling method `antialias` not supported")
        return None

    # ignore warnings from rio-tiler, rasterio
    if not tile_job.options.warnings:
        warnings.filterwarnings("ignore")

    try:
        reader: Reader = Reader(tile_job.input_file)
        # If has alpha band then use his, non casting mask from rio-tiler
        if tile_job.has_alpha_band == 1:
            # Need specified all bands from input raster
            indexes = reader.dataset.indexes
        else:
            indexes = None

        tz: int = tile_detail.tz
        tx: int = tile_detail.tx
        ty: int = tile_detail.ty
        if tile_job.options.XYZ:
            ty_xyz: int = (2**tz - 1) - ty
        else:
            ty_xyz = ty

        if not reader.tile_exists(tx, ty_xyz, tz):
            tiles: List[Tile] = reader.tms.neighbors(Tile(tx, ty_xyz, tz))
            is_exists = False
            for n in tiles:
                if reader.tile_exists(n.x, n.y, n.z):
                    is_exists = True
                    break
            if not is_exists:
                return None

        tileext: TileExtension = tile_job.tile_extension
        output: str = tile_job.output_folder
        tilesize: int = tile_job.tile_size
        tile_driver: str = tile_job.tile_driver.name

        # for cases when the NODATA is not specified,
        # but is necessary for masked array and correct encoding to RGBA
        if not tile_job.nodata and tile_job.options.encode_to_rgba:
            tile_job.nodata = -999999.0

        neighbor_tiles: Optional[List[Tuple[bytes, int, int, int]]] = meta_tile(
            reader,
            tx,
            ty_xyz,
            tz,
            tilesize,
            tile_driver,
            indexes,
            tile_job.nodata,
            tile_job.options.encode_to_rgba,
            tile_job.options.resampling.name,
            tile_job.has_alpha_band,
            True,
        )

        if neighbor_tiles is None:
            return None

        if tile_job.options.mbtiles:
            if queue:
                queue.put(neighbor_tiles)
            return neighbor_tiles
        else:
            if queue:
                queue.put(None)

            for n in neighbor_tiles:
                tilefilename: str = os.path.join(
                    output, str(n[3]), str(n[1]), f"{n[2]}.{tileext.name}"
                )
                if not os.path.exists(os.path.dirname(tilefilename)):
                    os.makedirs(os.path.dirname(tilefilename), exist_ok=True)
                # Write a tile to png/jpg on disk
                with open(tilefilename, "wb") as tile_file:
                    tile_file.write(n[0])
                    tile_file.flush()

    except Exception as e:
        logger.error(f"Error create base tile for raster '{tile_job.input_file}': {e}")
        return None
