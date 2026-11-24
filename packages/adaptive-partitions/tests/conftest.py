"""Synthetic population rasters, generated per test session (no downloads)."""

import numpy as np
import pytest
import rasterio
from rasterio.transform import from_origin

NODATA = -200.0  # as in GHSL POP

# (lon, lat, population, sigma in degrees): a few dense "cities" incl. one
# on the antimeridian and one near the north pole
CITIES = [
    (77.2, 28.6, 3.0e7, 2.0),
    (121.5, 31.2, 2.5e7, 1.5),
    (139.7, 35.7, 2.0e7, 1.0),
    (-74.0, 40.7, 1.5e7, 1.5),
    (3.4, 6.5, 1.5e7, 1.5),
    (-46.6, -23.5, 1.2e7, 1.5),
    (179.5, -17.0, 2.0e6, 1.0),
    (30.0, 85.0, 1.0e5, 2.0),
]


def synthetic_population(res=0.25):
    nx, ny = int(360 / res), int(180 / res)
    lon = -180 + (np.arange(nx) + 0.5) * res
    lat = 90 - (np.arange(ny) + 0.5) * res
    LON, LAT = np.meshgrid(lon, lat)
    rng = np.random.default_rng(42)
    pop = rng.gamma(0.3, 2.0, size=(ny, nx))  # rural background
    for x0, y0, total, s in CITIES:
        dx = (LON - x0 + 180) % 360 - 180
        g = np.exp(-(dx**2 + (LAT - y0) ** 2) / (2 * s**2))
        pop += total * g / g.sum()
    # "ocean": a nodata band and zeros
    pop[(LAT < -60)] = NODATA
    pop[(LON > -40) & (LON < -20) & (np.abs(LAT) < 30)] = 0
    return pop.astype("float32"), from_origin(-180, 90, res, res)


def _write(path, arr, transform, crs):
    with rasterio.open(
        path, "w", driver="GTiff", width=arr.shape[1], height=arr.shape[0],
        count=1, dtype="float32", crs=crs, transform=transform, nodata=NODATA,
        tiled=True, blockxsize=256, blockysize=256,
    ) as dst:
        dst.write(arr, 1)
    return path


@pytest.fixture(scope="session")
def pop_raster(tmp_path_factory):
    arr, transform = synthetic_population()
    path = tmp_path_factory.mktemp("rasters") / "pop_4326.tif"
    _write(path, arr, transform, "EPSG:4326")
    total = float(arr[arr > 0].astype("float64").sum())
    return path, total


@pytest.fixture(scope="session")
def pop_raster_mollweide(tmp_path_factory):
    """Same synthetic surface sampled onto a coarse Mollweide grid."""
    from pyproj import Transformer

    arr, _ = synthetic_population(res=0.25)
    res_m = 50_000.0
    xmax, ymax = 18_040_000.0, 9_020_000.0
    nx, ny = int(2 * xmax / res_m), int(2 * ymax / res_m)
    xs = -xmax + (np.arange(nx) + 0.5) * res_m
    ys = ymax - (np.arange(ny) + 0.5) * res_m
    X, Y = np.meshgrid(xs, ys)
    t = Transformer.from_crs("ESRI:54009", "EPSG:4326", always_xy=True)
    lon, lat = t.transform(X, Y)
    out = np.full((ny, nx), NODATA, dtype="float32")
    ok = np.isfinite(lon) & np.isfinite(lat) & (np.abs(lon) <= 180)
    ix = np.clip(((lon[ok] + 180) / 0.25).astype(int), 0, arr.shape[1] - 1)
    iy = np.clip(((90 - lat[ok]) / 0.25).astype(int), 0, arr.shape[0] - 1)
    out[ok] = arr[iy, ix]
    path = tmp_path_factory.mktemp("rasters") / "pop_mollweide.tif"
    _write(path, out, from_origin(-xmax, ymax, res_m, res_m), "ESRI:54009")
    total = float(out[out > 0].astype("float64").sum())
    return path, total
