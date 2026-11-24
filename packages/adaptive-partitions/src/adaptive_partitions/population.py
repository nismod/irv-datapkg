"""Stream a population raster onto a global geographic quadtree grid.

The global 30 arcsec GHSL raster is 43200 x 21600 pixels (~3.7 GB as
float32), so it is never read in one go. It is read in strips of rows; the
value of each valid pixel is added to the grid cell that contains the pixel
centre. The grid is the level-``L`` layer of the geographic quadtree used by
the quadkey scheme (see :mod:`adaptive_partitions.quadkey`): ``2**L`` rows and
``2**(L+1)`` columns of ``180 / 2**L`` degree squares.

Rasters in any projected CRS (for example GHSL's 1 km World Mollweide
product) are handled by transforming the pixel centres of valid pixels to
longitude/latitude with pyproj.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path

import numpy as np

__all__ = ["PopulationGrid", "aggregate_population", "grid_indices"]


def grid_indices(lon, lat, level: int):
    """Column and row of the level-``level`` grid cell containing each point.

    Cells are closed on their west and north edges: a cell covers
    ``xmin <= lon < xmax`` and ``ymin < lat <= ymax``. Longitudes are wrapped
    into ``[-180, 180)`` (so 180 is treated as -180) and latitudes are clipped
    to ``[-90, 90]`` (so the south pole falls in the bottom row).
    """
    lon = np.asarray(lon, dtype="float64")
    lat = np.asarray(lat, dtype="float64")
    size = 180.0 / 2**level
    lon = np.mod(lon + 180.0, 360.0)
    lat = np.clip(lat, -90.0, 90.0)
    ix = np.floor(lon / size).astype("int64")
    iy = np.floor((90.0 - lat) / size).astype("int64")
    np.clip(ix, 0, 2 ** (level + 1) - 1, out=ix)
    np.clip(iy, 0, 2**level - 1, out=iy)
    return ix, iy


@dataclass
class PopulationGrid:
    """Population summed onto the level-``level`` geographic quadtree grid.

    ``values[iy, ix]`` is the population whose pixel centre lies in the cell
    in row ``iy`` (from the north) and column ``ix`` (from 180 W).
    """

    values: np.ndarray
    level: int
    source: str | None = None
    _pyramid: dict = field(default_factory=dict, repr=False, compare=False)

    def __post_init__(self):
        expected = (2**self.level, 2 ** (self.level + 1))
        if self.values.shape != expected:
            raise ValueError(
                f"grid at level {self.level} must have shape {expected}, "
                f"got {self.values.shape}"
            )

    @property
    def cell_size(self) -> float:
        return 180.0 / 2**self.level

    @property
    def total(self) -> float:
        return float(self.values.sum())

    def at_level(self, level: int) -> np.ndarray:
        """Population summed to a coarser quadtree level (exact)."""
        if level > self.level:
            raise ValueError(
                f"grid level {self.level} is coarser than requested level {level}"
            )
        if level not in self._pyramid:
            arr = self.values
            for _ in range(self.level - level):
                ny, nx = arr.shape
                arr = arr.reshape(ny // 2, 2, nx // 2, 2).sum(axis=(1, 3))
            self._pyramid[level] = arr
        return self._pyramid[level]

    def nonzero_centres(self):
        """Longitude, latitude and population of every non-empty grid cell."""
        iy, ix = np.nonzero(self.values)
        size = self.cell_size
        lon = -180.0 + (ix + 0.5) * size
        lat = 90.0 - (iy + 0.5) * size
        return lon, lat, self.values[iy, ix]


def aggregate_population(
    path: str | Path,
    level: int = 10,
    band: int = 1,
    block_rows: int = 512,
) -> PopulationGrid:
    """Sum a population raster onto the geographic quadtree grid at ``level``.

    Reads the raster in strips of ``block_rows`` rows, so peak memory is one
    strip plus the output grid (``2**(2*level+1)`` float64 values; 16 MB at
    level 10, 64 MB at level 11). Pixels that are nodata, masked, non-finite
    or ``<= 0`` are ignored.

    The raster may be in EPSG:4326 (or any geographic CRS, treated as
    longitude/latitude) or in a projected CRS such as World Mollweide
    (ESRI:54009), in which case valid pixel centres are transformed to
    EPSG:4326.
    """
    import rasterio
    from rasterio.windows import Window

    ny, nx = 2**level, 2 ** (level + 1)
    values = np.zeros((ny, nx), dtype="float64")

    with rasterio.open(path) as src:
        transformer = None
        if src.crs is not None and not src.crs.is_geographic:
            from pyproj import Transformer

            transformer = Transformer.from_crs(src.crs, "EPSG:4326", always_xy=True)
        affine = src.transform
        cols_all = np.arange(src.width, dtype="float64") + 0.5

        for row_off in range(0, src.height, block_rows):
            nrows = min(block_rows, src.height - row_off)
            window = Window(0, row_off, src.width, nrows)
            block = src.read(band, window=window, masked=True)
            data = np.ma.filled(block.astype("float64"), np.nan)
            valid = np.isfinite(data) & (data > 0)
            if not valid.any():
                continue
            rr, cc = np.nonzero(valid)
            weights = data[rr, cc]
            # pixel centres in raster CRS (affine handles rotation too)
            col = cols_all[cc]
            row = rr + row_off + 0.5
            xs = affine.c + affine.a * col + affine.b * row
            ys = affine.f + affine.d * col + affine.e * row
            if transformer is not None:
                xs, ys = transformer.transform(xs, ys)
                ok = np.isfinite(xs) & np.isfinite(ys)
                xs, ys, weights = xs[ok], ys[ok], weights[ok]
                if xs.size == 0:
                    continue
            ix, iy = grid_indices(xs, ys, level)
            # accumulate only into the band of grid rows this strip touches
            y0, y1 = int(iy.min()), int(iy.max()) + 1
            local = (iy - y0) * nx + ix
            sums = np.bincount(local, weights=weights, minlength=(y1 - y0) * nx)
            values[y0:y1] += sums.reshape(y1 - y0, nx)

    return PopulationGrid(values=values, level=level, source=str(path))
