import json
import multiprocessing

from typing import Any, List, Optional, Union, Tuple
from raster_tiles.mosaic.merge_options import ComplexEncoder, MergeOptions

from raster_tiles.defaults import Profile, Resampling
from raster_tiles.models.tile_driver import TileDriver


class TilesOptions(object):

    def __init__(self, **kwargs):

        self.datasource_id: str = ""
        self.verbose: bool = False
        self.title: str = ""

        # Default profile, resampling method, driver and SRS
        self.profile: Profile = Profile.mercator
        self.resampling: Resampling = Resampling.nearest
        self.tiledriver: TileDriver = TileDriver.PNG
        self.s_srs: Optional[Any] = None

        self.zoom: Optional[Union[List[int], Tuple[int, int], int]] = None
        self.tile_size: int = 256
        self.nodata: Optional[float] = None
        self.tmscompatible: bool = False

        # Default pyramid use all cpus, need redefine in requests
        self.count_processes: int = multiprocessing.cpu_count()

        # Default XYZ tiles
        self.XYZ: bool = True

        # Default save tiles on disk
        self.mbtiles: bool = False
        self.mbtiles_db: str = ""

        self.datasource_dir: str = ""

        self.tiles_details_db: str = ""
        self.tiler_db: str = ""

        self.webp_lossless: bool = False
        self.webp_quality: int = 75  # default 75%

        # Remove warp/translate/overviews GeoTIFF after build pyramid
        self.remove_processing_raster_files: bool = True

        # disable warnings of GDAL, rasterio, rio-tiler...
        self.warnings: bool = False

        # save tile details for all zooms to sqlite db
        self.save_tile_detail_db: bool = False

        # Include gdalwarp at first step of preprocessing
        self.warp: bool = False
        self.resampling_warp: Resampling = Resampling.average
        self.encode_to_rgba: bool = True

        # mosaic
        self.mosaic_merge: bool = False
        self.mosaic_merge_options: Optional[MergeOptions] = None

        for key in kwargs:
            if hasattr(self, key):
                setattr(self, key, kwargs[key])

    def __unicode__(self):
        return f"TilesOptions verbose: {self.verbose}, profile: {self.profile.name},\
            resampling: {self.resampling.name}, tile size: {self.tile_size}, count_processes: {self.count_processes}\
            XYZ: {self.XYZ}, mbtiles: {self.mbtiles}, mbtiles_db: {self.mbtiles_db}\n"

    def __str__(self):
        return f"TilesOptions verbose: {self.verbose}, profile: {self.profile.name},\
            resampling: {self.resampling.name}, tile size: {self.tile_size}, count_processes: {self.count_processes}\
            XYZ: {self.XYZ}, mbtiles: {self.mbtiles}, mbtiles_db: {self.mbtiles_db}\n"

    def __repr__(self):
        return f"TilesOptions verbose: {self.verbose}, profile: {self.profile.name},\
            resampling: {self.resampling.name}, tile size: {self.tile_size}, count_processes: {self.count_processes}\
            XYZ: {self.XYZ}, mbtiles: {self.mbtiles}, mbtiles_db: {self.mbtiles_db}\n"

    def toJSON(self):
        return json.dumps(self.__dict__, cls=ComplexEncoder, sort_keys=True)
