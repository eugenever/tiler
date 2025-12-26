import multiprocessing
import os
import time

from osgeo import gdal

from raster_tiles.singleprocess import single_threaded_tiling
from raster_tiles.models.tiles_options import TilesOptions
from raster_tiles.multiprocess import (
    multi_process_tiling,
    process_tiling_in_separate_processes,
)
from raster_tiles.utils import (
    DividedCache,
    isfile,
    options_post_processing,
    set_cache_max,
)
from raster_tiles.models.tile_job import TileJob

gdal.UseExceptions()


def generate_tiles(input_file: str, options: TilesOptions) -> None:
    """Generate tiles from input file.
    Arguments:
        ``input_file`` (str): name of input file from folder ./data
        ``options``: Tile generation options (TilesOptions).
    Options:
        ``profile`` (str): Tile cutting profile (mercator,geodetic,raster) - default
            'mercator' (Google Maps compatible)
        ``resampling`` (str): Resampling method (average,near,bilinear,cubic,cubicsp
            line,lanczos,antialias) - default 'average'
        ``s_srs``: The spatial reference system used for the source input data
        ``zoom``: Zoom levels to render; format: `[int min, int max]`,
            `'min-max'` or `int/str zoomlevel`.
        ``tile_size`` (int): Size of tiles to render - default 256
        ``nodata``: NODATA transparency value to assign to the input data
        ``tmscompatible`` (bool): When using the geodetic profile, specifies the base
            resolution as 0.703125 or 2 tiles at zoom level 0.
        ``verbose`` (bool): Print status messages to stdout
        ``count_processes``: Number of processes to use for tiling.
    """

    options = options_post_processing(input_file, options)

    if options.count_processes == 1:
        with DividedCache(options.count_processes), multiprocessing.get_context(
            "spawn"
        ).Pool(processes=1) as pool:
            tile_job = single_threaded_tiling(input_file, options, pool)
    else:
        with DividedCache(options.count_processes), multiprocessing.get_context(
            "spawn"
        ).Pool(processes=options.count_processes) as pool:
            tile_job = multi_process_tiling(input_file, options, pool)

    # Remove source raster files (gdaltranslate + gdaladdo) after build pyramid of tiles
    # You can't delete it right away
    # It is necessary to wait for the mbtiles or tile files to be completely saved to disk
    # Otherwise in tile handler can may be exception since there is no place to get the tile from
    time.sleep(5)
    if options.remove_processing_raster_files and tile_job is not None:
        clean_warping_rasters(tile_job)


def generate_tiles_in_separate_processes(
    input_file: str, options: TilesOptions
) -> None:

    options = options_post_processing(input_file, options)

    set_cache_max(1024 * 1024 * 1024 * 2)
    tile_job = process_tiling_in_separate_processes(input_file, options)

    # Remove source raster files (gdaltranslate + gdaladdo) after build pyramid of tiles
    # You can't delete it right away
    # It is necessary to wait for the mbtiles or tile files to be completely saved to disk
    # Otherwise in tile handler can may be exception since there is no place to get the tile from
    time.sleep(5)
    if options.remove_processing_raster_files and tile_job is not None:
        clean_warping_rasters(tile_job)


def clean_warping_rasters(tile_job: TileJob):
    if isfile(tile_job.input_file):
        os.remove(tile_job.input_file)

    overviews = tile_job.input_file + ".ovr"
    if isfile(overviews):
        os.remove(overviews)

    vrt = os.path.join(tile_job.output_folder, tile_job.in_file + ".vrt")
    if isfile(vrt):
        os.remove(vrt)

    ext: str = os.path.splitext(tile_job.input_file)[1]
    raster_rgba = os.path.join(tile_job.output_folder, f"{tile_job.in_file}_RGBA{ext}")
    if isfile(raster_rgba):
        os.remove(raster_rgba)
