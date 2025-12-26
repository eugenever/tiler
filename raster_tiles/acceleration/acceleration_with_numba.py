import numpy as np

from typing import Any
from nptyping import Float, Int, NDArray, Bool
from numba import guvectorize, float64, uint8, void


@guvectorize(
    [void(float64[:, :, :], float64, uint8[:, :, :], uint8[:, :, :])],
    "(x,y,y),(),(z,y,y)->(z,y,y)",
)
def acceleration_encoding_with_numba(
    data: np.ndarray, nodata: float, dummy: np.ndarray, rgba_array: np.ndarray
):
    # Hiding the NODATA under a mask
    mask: NDArray[Any, Bool] = data <= nodata
    array: NDArray[Any, Float] = np.where(mask, np.nan, data)

    module: NDArray[Any, Float] = np.abs(array)
    sign: NDArray[Any, Int] = np.sign(array)
    norm: NDArray[Any, Int] = 1.0 - sign * sign
    exponent: NDArray[Any, Int] = np.floor(np.log2(module + norm))
    mantissa: NDArray[Any, Float] = (
        8388608 + sign + sign * 8388608 * (module / np.exp2(exponent) - 1.0)
    )

    mantissa = np.where(mask, 0, mantissa)

    rgba_array[0, :, :] = np.floor(mantissa / 65536)
    rgba_array[1, :, :] = np.floor(np.mod(mantissa, 65536) / 256)
    rgba_array[2, :, :] = np.floor(
        mantissa - rgba_array[0, :, :] * 65536 - rgba_array[1, :, :] * 256
    )
    rgba_array[3, :, :] = np.where(mask, 0, exponent + 128)
