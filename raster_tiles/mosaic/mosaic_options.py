import os
import multiprocessing
import sqlite3
import json

from pathlib import Path
from typing import Optional, Type, Union, List, Tuple

from raster_tiles.defaults import MERGE_MOSAIC_METHODS, Profile, Resampling
from raster_tiles.mosaic.merge_options import ComplexEncoder
from raster_tiles.models.tile_driver import TileDriver
from server.sqlite_db import sqlite_db_connect, tiler_setup
from server.datasources import EXTENSIONS
from raster_tiles.models.tile_details import tiles_detail_setup

from rio_tiler.mosaic.methods.base import MosaicMethodBase
from rio_tiler.mosaic.methods.defaults import LowestMethod


class MosaicOptions(object):
    def __init__(
        self,
        assets_dir: str,
        datasource_id: str,
        zoom: Optional[Union[List[int], Tuple[int, int], int]] = None,
        **kwargs,
    ):

        self.datasource_id: str = datasource_id
        self.verbose: bool = False

        parent_dir: Path = Path(__file__).parents[2]
        self.data_dir: str = os.path.join(str(parent_dir), "data")
        self.mosaics_dir: str = os.path.join(self.data_dir, "mosaics")
        self.assets_dir: str = os.path.join(self.mosaics_dir, assets_dir)
        self.mosaic: str = assets_dir

        # input: folder in data/mosaics with some raster files
        self.assets_files: List[str] = [
            os.path.join(self.assets_dir, asset)
            for asset in os.listdir(self.assets_dir)
            if is_raster_asset(os.path.join(self.assets_dir, asset))
        ]

        self.warped_assets_files: Optional[List[str]] = None

        # Max files size 2GB
        self.max_file_size: int = 1024 * 1024 * 1024 * 2

        # Default profile, resampling method, driver and SRS
        self.profile: Profile = Profile.mercator
        self.resampling: Resampling = Resampling.nearest
        self.tiledriver: TileDriver = TileDriver.PNG

        # User specified zoom levels
        self.zoom: Optional[Union[List[int], Tuple[int, int], int]] = zoom
        self.tminz: Optional[int] = None
        self.tmaxz: Optional[int] = None
        if isinstance(self.zoom, (list, tuple)):
            self.tminz = self.zoom[0]
            self.tmaxz = self.zoom[1]
        elif isinstance(self.zoom, int):
            self.tminz = self.zoom
            self.tmaxz = self.tminz

        self.tile_size: int = 256
        self.tileext: str = "png"
        self.datasource_dir: str = ""
        # Default XYZ tiles
        self.XYZ: bool = True

        # Default pyramid use all cpus, need redefine in requests
        cpus = multiprocessing.cpu_count()
        self.count_processes: int = cpus
        self.threads: int = cpus

        # Default save tiles on disk
        self.mbtiles: bool = False
        self.mbtiles_db: str = ""

        self.tiles_details_db: str = os.path.join(
            self.data_dir, datasource_id, f"{assets_dir}.db"
        )
        ds_path: str = os.path.join(
            self.data_dir,
            self.datasource_id,
        )
        if not os.path.exists(ds_path):
            os.makedirs(ds_path, exist_ok=True)

        self.tiler_db: str = os.path.join(self.data_dir, "tiler.db")

        # create tiles detail database file
        if os.path.isfile(self.tiles_details_db):
            try:
                os.remove(self.tiles_details_db)
            except Exception:
                pass

        connection: sqlite3.Connection = sqlite_db_connect(self.tiles_details_db)
        cursor: sqlite3.Cursor = connection.cursor()
        tiles_detail_setup(cursor)
        connection.commit()
        connection.close()

        if not os.path.isfile(self.tiler_db):
            connection_db: sqlite3.Connection = sqlite_db_connect(self.tiler_db)
            cursor_db: sqlite3.Cursor = connection.cursor()
            tiler_setup(cursor_db)
            connection_db.commit()
            connection_db.close()

        # Remove warp/translate/overviews GeoTIFF after build pyramid
        self.remove_processing_raster_files: bool = True

        # disable warnings of GDAL, rasterio, rio-tiler...
        self.warnings: bool = False

        self.encode_to_rgba: bool = True

        # In metres - 2D projection
        self.t_srs: str = "EPSG:3857"
        self.ot: str = "Float64"
        self.of: str = "GTiff"

        self.pixel_selection_method: Union[Type[MosaicMethodBase], MosaicMethodBase] = (
            LowestMethod
        )

        self.nodata_default: float = -999999.0

        # merge mosaic assets into one asset to optimize the tiling process
        self.merge: bool = True
        self.merged_asset: Optional[str] = None

        for key in kwargs:
            if hasattr(self, key):
                setattr(self, key, kwargs[key])

    def toJSON(self):
        return json.dumps(self.__dict__, cls=ComplexEncoder, sort_keys=True)


def is_raster_asset(asset: str) -> bool:
    if not os.path.isfile(asset):
        return False

    ext = os.path.splitext(asset)[-1].lower()
    extend_methods = ["sum", "count"]
    methods = MERGE_MOSAIC_METHODS.copy()
    methods.extend(extend_methods)
    if (
        ext != ".ovr"
        and ("_warp_tr_ov." not in asset.lower())
        and (ext in EXTENSIONS)
        and not any([f"_{method.upper()}" in asset for method in methods])
    ):
        return True
    else:
        return False
