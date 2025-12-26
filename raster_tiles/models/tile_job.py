import enum
import json

from typing import Dict, List, Optional, Tuple
from raster_tiles.models.tile_details import TileDetail
from raster_tiles.models.tile_driver import TileDriver
from raster_tiles.models.tiles_options import TilesOptions


class TileExtension(enum.Enum):
    png = 1
    jpg = 2
    jpeg = 3

    def toJSON(self):
        return json.dumps(self.name)


class TileJob(object):
    """
    Plain object to hold tile job configuration for a dataset
    """

    def __init__(self, **kwargs):

        self.src_file: str = ""
        self.in_file: str = ""
        self.input_file: str = ""
        self.nb_data_bands: int = 0
        self.nodata: Optional[float] = None
        self.output_folder: str = ""
        self.tile_extension: TileExtension = TileExtension.png
        self.tile_size: int = 0
        self.tile_driver: TileDriver = TileDriver.PNG
        self.tminmax: List[Tuple[int, int, int, int]] = []
        self.tminz: int = 0
        self.tmaxz: int = 0
        self.in_srs_wkt = 0
        self.out_geo_trans: List[float] = []
        self.ominy = 0
        self.options: Optional[TilesOptions] = None
        self.tile_details: Dict[int, List[TileDetail]] = {}
        self.encode_to_rgba: int = 0
        self.has_alpha_band: int = 0
        self.merge: bool = True
        self.merged_asset: Optional[str] = None

        for key in kwargs:
            if hasattr(self, key):
                setattr(self, key, kwargs[key])

    def __unicode__(self):
        return f"TileJob source file: {self.src_file}, tile size:{self.tile_size}, tminz: {self.tminz}, tmaxz: {self.tmaxz},\
            options: {self.options}, output folder: {self.output_folder}\n"

    def __str__(self):
        return f"TileJob source file: {self.src_file}, tile size:{self.tile_size}, tminz: {self.tminz}, tmaxz: {self.tmaxz},\
            options: {self.options}, output folder: {self.output_folder}\n"

    def __repr__(self):
        return f"TileJob source file: {self.src_file}, tile size:{self.tile_size}, tminz: {self.tminz}, tmaxz: {self.tmaxz},\
            options: {self.options}, output folder: {self.output_folder}\n"
