import logging
import warnings
import rasterio
import math
import numpy as np
import os

from server.tile_utils import is_warped_raster_asset
from raster_tiles.mosaic.mosaic_options import MosaicOptions
from raster_tiles.defaults import (
    PIXEL_SELECTION_METHOD_TO_MERGE_METHOD,
    RIO_RESAMPLING,
)

from typing import Any, Dict, List, Optional, Tuple, Union
from nptyping import NDArray, Bool

from rio_tiler.utils import resize_array
from rasterio import Affine, windows
from rasterio.io import DatasetReader, DatasetWriter, BufferedDatasetWriter

logger = logging.getLogger(__name__)

"""
copy_count and copy_sum necessary for calculation MEAN raster
"""


def copy_count(
    merged_data: NDArray[Any, np.float32],
    new_data: NDArray[Any, np.float32],
    merged_mask: NDArray[Any, Bool],
    new_mask: NDArray[Any, Bool],
) -> None:
    """Returns the count of valid pixels."""
    mask: NDArray[Any, Bool] = np.empty_like(merged_mask, dtype="bool")
    np.logical_or(merged_mask, new_mask, out=mask)
    np.logical_not(mask, out=mask)
    np.add(merged_data, mask, out=merged_data, where=mask, casting="unsafe")
    np.logical_not(new_mask, out=mask)
    np.logical_and(merged_mask, mask, out=mask)
    np.copyto(merged_data, mask, where=mask, casting="unsafe")


def copy_sum(
    merged_data: NDArray[Any, np.float32],
    new_data: NDArray[Any, np.float32],
    merged_mask: NDArray[Any, Bool],
    new_mask: NDArray[Any, Bool],
) -> None:
    """Returns the sum of all pixel values."""
    mask: NDArray[Any, Bool] = np.empty_like(merged_mask, dtype="bool")
    np.logical_or(merged_mask, new_mask, out=mask)
    np.logical_not(mask, out=mask)
    np.add(merged_data, new_data, out=merged_data, where=mask, casting="unsafe")
    np.logical_not(new_mask, out=mask)
    np.logical_and(merged_mask, mask, out=mask)
    np.copyto(merged_data, new_data, where=mask, casting="unsafe")


def merge_with_mean_method(options: MosaicOptions) -> None:
    warnings.filterwarnings("ignore")

    rasters: List[Union[DatasetReader, DatasetWriter, BufferedDatasetWriter]] = [
        rasterio.open(raster, dtype=np.float32)
        for raster in options.warped_assets_files
    ]

    # adapted from https://github.com/mapbox/rasterio/blob/master/rasterio/merge.py
    max_resolution: Tuple[float, float] = max([r.res for r in rasters])
    # Determine output band count = 1
    band_count: int = 1
    # Resolution/pixel size
    res = max_resolution

    # General extent of all inputs
    xs: List[float] = []
    ys: List[float] = []
    for r in rasters:
        left, bottom, right, top = r.bounds
        xs.extend([left, right])
        ys.extend([bottom, top])

    dst_w, dst_s, dst_e, dst_n = min(xs), min(ys), max(xs), max(ys)
    out_transform: Affine = Affine.translation(dst_w, dst_n)
    out_transform *= Affine.scale(res[0], -res[1])
    # Compute output array shape
    # We guarantee it will cover the output bounds completely
    output_width: int = int(math.ceil((dst_e - dst_w) / res[0]))
    output_height: int = int(math.ceil((dst_n - dst_s) / res[1]))
    # Adjust bounds to fit
    dst_e, dst_s = out_transform * (output_width, output_height)

    # destination array shape
    shape: Tuple[int, int] = (output_height, output_width)
    dest_profile: Dict[str, Any] = {
        "driver": "GTiff",
        "height": shape[0],
        "width": shape[1],
        "count": band_count,
        "dtype": np.float32,
        "crs": "EPSG:3857",
        "transform": out_transform,
        "tiled": True,
        "BIGTIFF": "YES",
        "nodata": -9999999.0,
    }

    # open output file in write/read mode and fill with destination mosaics array
    output_raster: str = os.path.join(
        options.data_dir, options.datasource_id, f"{options.mosaic}_SUM.tif"
    )
    output_raster_count: str = os.path.join(
        options.data_dir, options.datasource_id, f"{options.mosaic}_COUNT.tif"
    )

    resampling_method = RIO_RESAMPLING[options.resampling.name]

    with rasterio.open(
        output_raster,
        "w+",
        **dest_profile,
    ) as mosaic_raster, rasterio.open(
        output_raster_count,
        "w+",
        **dest_profile,
    ) as mosaic_count:
        mosaic_raster_nodata: Optional[float] = mosaic_raster.nodatavals[0]
        if mosaic_raster_nodata is None:
            mosaic_raster_nodata = 0.0

        for src in rasters:
            # store raster nodata value
            nodata: Optional[float] = src.nodatavals[0]
            if nodata is None:
                nodata = 0.0

            for ij, src_window in src.block_windows(1):
                src_block: NDArray[Any, np.float32] = src.read(
                    1, window=src_window, resampling=resampling_method
                ).astype("float")
                # Replace NODATA NAN
                mask: NDArray[Any, Bool] = src_block == nodata
                src_block[mask] = np.nan

                # convert relative input window location to relative output window location
                # using real world coordinates (bounds)
                src_bounds: Tuple[float, float, float, float] = windows.bounds(
                    src_window, transform=src.profile["transform"]
                )
                dst_window: windows.Window = windows.from_bounds(
                    *src_bounds, transform=mosaic_raster.profile["transform"]
                )
                # round the values of dest_window as they can be float
                dst_window: windows.Window = windows.Window(
                    round(dst_window.col_off),
                    round(dst_window.row_off),
                    round(dst_window.width),
                    round(dst_window.height),
                )

                try:
                    # before writing the window, replace source nodata with dest nodata as it can already have been written
                    # (e.g. another adjacent country) https://stackoverflow.com/a/43590909/1979665
                    mosaic_raster_block: NDArray[Any, np.float32] = mosaic_raster.read(
                        1, window=dst_window, resampling=resampling_method
                    ).astype("float")
                    mosaic_raster_block[mosaic_raster_block == mosaic_raster_nodata] = (
                        np.nan
                    )
                    mask_rb: NDArray[Any, Bool] = np.isnan(mosaic_raster_block)

                    mosaic_counting_block: NDArray[Any, np.float32] = mosaic_count.read(
                        1, window=dst_window, resampling=resampling_method
                    ).astype("float")
                    mosaic_counting_block[mask_rb] = np.nan

                    src_block_mod: NDArray[Any, np.float32] = np.copy(src_block)
                    if (
                        mosaic_raster_block.shape[0] != src_block_mod.shape[0]
                        or mosaic_raster_block.shape[1] != src_block_mod.shape[1]
                    ):
                        mosaic_raster_block = resize_array(
                            mosaic_raster_block,
                            src_block_mod.shape[0],
                            src_block_mod.shape[1],
                        )
                        mask_rb = np.isnan(mosaic_raster_block)
                        mosaic_counting_block = resize_array(
                            mosaic_counting_block,
                            src_block_mod.shape[0],
                            src_block_mod.shape[1],
                        )

                    mask_src: NDArray[Any, Bool] = mosaic_counting_block >= len(rasters)
                    np.logical_or(mask, mask_src, out=mask)

                    copy_sum(src_block_mod, mosaic_raster_block, mask, mask_rb)
                    mosaic_raster.write(src_block_mod, 1, window=dst_window)

                    counting: NDArray[Any, np.float32] = np.ones_like(src_block_mod)
                    counting[mask] = np.nan
                    copy_sum(counting, mosaic_counting_block, mask, mask_rb)
                    mosaic_count.write(counting, 1, window=dst_window)

                except Exception as e:
                    logger.error(f"Read block_windows {ij}, raster {src.name}: {e}")
                    raise Exception(e)

    # Only to calculating 'mean' raster
    raster_mean: str = os.path.join(
        options.data_dir, options.datasource_id, f"{options.mosaic}_MEAN.tif"
    )
    with rasterio.open(raster_mean, "w+", **dest_profile) as mosaic_mean:
        # Open only in read mode
        open_options: Dict[str, Any] = {
            "driver": "GTiff",
            "height": shape[0],
            "width": shape[1],
            "count": band_count,
            "dtype": np.float32,
            "crs": "EPSG:3857",
            "transform": out_transform,
            "nodata": -9999999.0,
        }
        source_sum: Union[DatasetReader, DatasetWriter, BufferedDatasetWriter] = (
            rasterio.open(output_raster, "r", **open_options)
        )
        source_count: Union[DatasetReader, DatasetWriter, BufferedDatasetWriter] = (
            rasterio.open(output_raster_count, "r", **open_options)
        )

        nodata: Optional[float] = source_sum.nodatavals[0]
        if nodata is None:
            nodata = 0.0

        for _, src_window in source_sum.block_windows(1):
            r_sum: NDArray[Any, np.float32] = source_sum.read(
                1, window=src_window, resampling=resampling_method
            ).astype("float")
            r_count: NDArray[Any, np.float32] = source_count.read(
                1, window=src_window, resampling=resampling_method
            ).astype("float")

            # replace zeros with nan
            mask: NDArray[Any, Bool] = r_sum == nodata
            r_sum[mask] = np.nan
            r_count[mask] = np.nan

            src_bounds: Tuple[float, float, float, float] = windows.bounds(
                src_window, transform=source_sum.profile["transform"]
            )
            dst_window: windows.Window = windows.from_bounds(
                *src_bounds, transform=mosaic_mean.profile["transform"]
            )
            # round the values of dest_window as they can be float
            dst_window: windows.Window = windows.Window(
                round(dst_window.col_off),
                round(dst_window.row_off),
                round(dst_window.width),
                round(dst_window.height),
            )
            r_div: NDArray[Any, np.float32] = np.divide(
                r_sum, r_count, casting="unsafe"
            )
            mosaic_mean.write(r_div, 1, window=dst_window)


def merge_with_rio_methods(options: MosaicOptions, method: str):
    warnings.filterwarnings("ignore")

    rasters: List[Union[DatasetReader, DatasetWriter, BufferedDatasetWriter]] = [
        rasterio.open(raster, dtype=np.float32)
        for raster in options.warped_assets_files
    ]

    max_resolution: Tuple[float, float] = max([r.res for r in rasters])
    # Determine output band count = 1
    band_count: int = 1
    # Resolution/pixel size
    res = max_resolution

    # General extent of all inputs
    xs: List[float] = []
    ys: List[float] = []
    for r in rasters:
        left, bottom, right, top = r.bounds
        xs.extend([left, right])
        ys.extend([bottom, top])

    dst_w, dst_s, dst_e, dst_n = min(xs), min(ys), max(xs), max(ys)
    out_transform: Affine = Affine.translation(dst_w, dst_n)
    out_transform *= Affine.scale(res[0], -res[1])
    # Compute output array shape
    # We guarantee it will cover the output bounds completely
    output_width: int = int(math.ceil((dst_e - dst_w) / res[0]))
    output_height: int = int(math.ceil((dst_n - dst_s) / res[1]))
    # Adjust bounds to fit
    dst_e, dst_s = out_transform * (output_width, output_height)

    # destination array shape
    shape: Tuple[int, int] = (output_height, output_width)

    if method == "first":

        def copyto(
            old_data: NDArray[Any, np.float32],
            new_data: NDArray[Any, np.float32],
            old_nodata: float,
            new_nodata: float,
        ):
            mask: NDArray[Any, Bool] = np.logical_and(old_nodata, ~new_nodata)
            old_data[mask] = new_data[mask]

    elif method == "last":

        def copyto(
            old_data: NDArray[Any, np.float32],
            new_data: NDArray[Any, np.float32],
            old_nodata: float,
            new_nodata: float,
        ):
            mask: NDArray[Any, Bool] = ~new_nodata
            old_data[mask] = new_data[mask]

    elif method == "min":

        def copyto(
            old_data: NDArray[Any, np.float32],
            new_data: NDArray[Any, np.float32],
            old_nodata: float,
            new_nodata: float,
        ):
            mask: NDArray[Any, Bool] = np.logical_and(~old_nodata, ~new_nodata)
            old_data[mask] = np.minimum(old_data[mask], new_data[mask])
            mask = np.logical_and(old_nodata, ~new_nodata)
            old_data[mask] = new_data[mask]

    elif method == "max":

        def copyto(
            old_data: NDArray[Any, np.float32],
            new_data: NDArray[Any, np.float32],
            old_nodata: float,
            new_nodata: float,
        ):
            mask: NDArray[Any, Bool] = np.logical_and(~old_nodata, ~new_nodata)
            old_data[mask] = np.maximum(old_data[mask], new_data[mask])
            mask = np.logical_and(old_nodata, ~new_nodata)
            old_data[mask] = new_data[mask]

    dest_profile: Dict[str, Any] = {
        "driver": "GTiff",
        "height": shape[0],
        "width": shape[1],
        "count": band_count,
        "dtype": np.float32,
        "crs": "EPSG:3857",
        "transform": out_transform,
        "tiled": True,
        "BIGTIFF": "YES",
        "nodata": -9999999.0,
    }

    # open output file in write/read mode and fill with destination mosaick array
    output_raster: str = os.path.join(
        options.data_dir,
        options.datasource_id,
        f"{options.mosaic}_{method.upper()}.tif",
    )

    resampling_method = RIO_RESAMPLING[options.resampling.name]

    with rasterio.open(
        output_raster,
        "w+",
        **dest_profile,
    ) as mosaic_raster:

        mosaic_raster_nodata: Optional[float] = mosaic_raster.nodatavals[0]
        if mosaic_raster_nodata is None:
            mosaic_raster_nodata = 0.0

        for src in rasters:
            # store raster nodata value
            nodata: Optional[float] = src.nodatavals[0]
            if nodata is None:
                nodata = 0.0

            for ij, src_window in src.block_windows(1):
                src_block: NDArray[Any, np.float32] = src.read(
                    1, window=src_window, resampling=resampling_method
                ).astype("float")

                # Replace NODATA to NAN
                mask: NDArray[Any, Bool] = src_block == nodata
                src_block[mask] = np.nan

                # convert relative input window location to relative output window location
                # using real world coordinates (bounds)
                src_bounds: Tuple[float, float, float, float] = windows.bounds(
                    src_window, transform=src.profile["transform"]
                )
                dst_window: windows.Window = windows.from_bounds(
                    *src_bounds, transform=mosaic_raster.profile["transform"]
                )
                # round the values of dest_window as they can be float
                dst_window = windows.Window(
                    round(dst_window.col_off),
                    round(dst_window.row_off),
                    round(dst_window.width),
                    round(dst_window.height),
                )

                try:
                    # before writing the window, replace source nodata with dest nodata as it can already have been written (e.g. another adjacent country)
                    # https://stackoverflow.com/a/43590909/1979665
                    mosaic_raster_block: NDArray[Any, np.float32] = mosaic_raster.read(
                        1, window=dst_window, resampling=resampling_method
                    ).astype("float")
                    mosaic_raster_block[mosaic_raster_block == mosaic_raster_nodata] = (
                        np.nan
                    )
                    mask_rb: NDArray[Any, Bool] = np.isnan(mosaic_raster_block)
                    src_block_mod: NDArray[Any, np.float32] = np.copy(src_block)
                    if (
                        mosaic_raster_block.shape[0] != src_block_mod.shape[0]
                        or mosaic_raster_block.shape[1] != src_block_mod.shape[1]
                    ):
                        mosaic_raster_block = resize_array(
                            mosaic_raster_block,
                            src_block_mod.shape[0],
                            src_block_mod.shape[1],
                        )
                        mask_rb = np.isnan(mosaic_raster_block)

                    copyto(src_block_mod, mosaic_raster_block, mask, mask_rb)
                    mosaic_raster.write(src_block_mod, 1, window=dst_window)
                except Exception as e:
                    logger.error(f"Read block_windows {ij}, raster {src.name}: {e}")
                    raise Exception(e)


def mosaics_merge(options: MosaicOptions) -> str:
    warnings.filterwarnings("ignore")

    warped_assets_files: List[str] = [
        os.path.join(options.data_dir, options.datasource_id, asset)
        for asset in os.listdir(os.path.join(options.data_dir, options.datasource_id))
        if is_warped_raster_asset(
            os.path.join(options.data_dir, options.datasource_id, asset)
        )
    ]
    options.warped_assets_files = warped_assets_files

    method: str = PIXEL_SELECTION_METHOD_TO_MERGE_METHOD[
        options.pixel_selection_method.__name__
    ]

    if method == "mean":
        merge_with_mean_method(options)
    elif method in ["first", "min", "max"]:
        merge_with_rio_methods(options, method)
    else:
        raise Exception(f"Undefined merge method: {method}")

    asset: str = os.path.join(
        options.data_dir,
        options.datasource_id,
        f"{options.mosaic}_{method.upper()}.tif",
    )
    return asset
