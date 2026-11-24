"""Geographic (EPSG:4326) quadtree with quadkey-style names.

Grid
----
The tree is the OGC *WorldCRS84Quad* tile matrix set (OGC 17-083r4): level 0
is two 180 x 180 degree squares (west and east hemispheres) and each level
halves the cell size, so level ``z`` has ``2**(z+1)`` columns and ``2**z``
rows of ``180 / 2**z`` degree cells. Unlike Web Mercator tiles, it covers the
whole globe, poles included, and every cell edge is a meridian or parallel,
so a cell is exactly its bbox in the lon/lat coordinates GeoParquet uses.

Keys
----
``"q" + h + d1 d2 ... dz`` where ``h`` is the level-0 cell (``0`` west,
``1`` east) and each digit ``d`` picks a quadrant as in Bing Maps quadkeys
(``0`` NW, ``1`` NE, ``2`` SW, ``3`` SE). So level = ``len(key) - 2`` and a
key is a prefix of the keys of all its descendants. The leading ``q`` keeps
Hive partition values from being parsed as integers (``0213`` -> ``213``).
"""

from __future__ import annotations

import heapq

import numpy as np

from .population import PopulationGrid, grid_indices

__all__ = [
    "tile_to_key",
    "key_to_tile",
    "key_bounds",
    "children",
    "build_quadkey",
    "assign_points",
]

PREFIX = "q"


def tile_to_key(level: int, x: int, y: int) -> str:
    """Quadkey for WorldCRS84Quad tile (level, column x from 180W, row y from 90N)."""
    if not (0 <= x < 2 ** (level + 1) and 0 <= y < 2**level):
        raise ValueError(f"tile ({level}, {x}, {y}) is out of range")
    digits = [str(x >> level)]
    for i in range(level, 0, -1):
        mask = 1 << (i - 1)
        digits.append(str((2 if y & mask else 0) + (1 if x & mask else 0)))
    return PREFIX + "".join(digits)


def key_to_tile(key: str) -> tuple[int, int, int]:
    """Inverse of :func:`tile_to_key`: returns (level, x, y)."""
    if not key.startswith(PREFIX) or len(key) < 2 or key[1] not in "01":
        raise ValueError(f"not a geographic quadkey: {key!r}")
    x = int(key[1])
    y = 0
    for ch in key[2:]:
        d = int(ch)
        if d > 3:
            raise ValueError(f"not a geographic quadkey: {key!r}")
        x = (x << 1) | (d & 1)
        y = (y << 1) | (d >> 1)
    return len(key) - 2, x, y


def key_bounds(key: str) -> tuple[float, float, float, float]:
    """(xmin, ymin, xmax, ymax) of a quadkey cell in degrees."""
    level, x, y = key_to_tile(key)
    size = 180.0 / 2**level
    xmin = -180.0 + x * size
    ymax = 90.0 - y * size
    return (xmin, ymax - size, xmin + size, ymax)


def children(key: str) -> list[str]:
    return [key + d for d in "0123"]


def _population(grid: PopulationGrid, key: str) -> float:
    level, x, y = key_to_tile(key)
    return float(grid.at_level(level)[y, x])


def build_quadkey(
    grid: PopulationGrid,
    target: int | None = 200,
    start_level: int = 1,
    max_level: int | None = None,
    max_population: float | None = None,
    allow_overshoot: bool = False,
) -> list[tuple[str, float]]:
    """Adaptive quadtree: split the most populous cell until ``target`` cells.

    Starts from all ``2 * 4**start_level`` cells at ``start_level``. Each step
    removes the most populous splittable cell and adds its 4 children (net +3
    cells). Ties are broken by key so the result is deterministic. Stops when

    * the next split would make more than ``target`` cells (unless
      ``allow_overshoot``, in which case the split that reaches or passes
      ``target`` is the last one), or
    * ``max_population`` is given and no cell exceeds it, or
    * no splittable cell has any population.

    Cells at ``max_level`` (default: the grid level) are never split.
    Returns ``[(key, population), ...]`` sorted by key.
    """
    if max_level is None:
        max_level = grid.level
    if max_level > grid.level:
        raise ValueError(
            f"max_level {max_level} is finer than the population grid "
            f"(level {grid.level}); aggregate at a finer grid level"
        )
    if not 0 <= start_level <= max_level:
        raise ValueError("need 0 <= start_level <= max_level")
    if target is None and max_population is None:
        raise ValueError("give target, max_population or both")
    n_start = 2 * 4**start_level
    if target is not None and n_start > target:
        raise ValueError(
            f"start_level {start_level} already has {n_start} cells, "
            f"more than target {target}"
        )

    final: list[tuple[str, float]] = []
    heap: list[tuple[float, str]] = []
    for y in range(2**start_level):
        for x in range(2 ** (start_level + 1)):
            k = tile_to_key(start_level, x, y)
            heapq.heappush(heap, (-_population(grid, k), k))

    count = n_start
    while heap:
        neg_pop, key = heap[0]
        pop = -neg_pop
        if pop <= 0:
            break
        if max_population is not None and pop <= max_population:
            break
        if len(key) - 2 >= max_level:
            heapq.heappop(heap)
            final.append((key, pop))
            continue
        if _target_reached(count, 3, target, allow_overshoot):
            break
        heapq.heappop(heap)
        for c in children(key):
            heapq.heappush(heap, (-_population(grid, c), c))
        count += 3

    final.extend((k, -p) for p, k in heap)
    final.sort()
    return final


def _target_reached(count, growth, target, allow_overshoot) -> bool:
    if target is None:
        return False
    if allow_overshoot:
        return count >= target
    return count + growth > target


def assign_points(keys, lon, lat) -> np.ndarray:
    """Index into ``keys`` of the cell containing each point (-1 if none).

    Uses the closed-west, closed-north rule of
    :func:`adaptive_partitions.population.grid_indices`. Because quadtree
    cells nest exactly, a point lies in exactly one cell of a valid
    partition.
    """
    keys = list(keys)
    tiles = [key_to_tile(k) for k in keys]
    lon = np.asarray(lon, dtype="float64")
    lat = np.asarray(lat, dtype="float64")
    out = np.full(lon.shape, -1, dtype="int64")
    if not tiles:
        return out
    finest = max(t[0] for t in tiles)
    ok = np.isfinite(lon) & np.isfinite(lat)
    ix, iy = grid_indices(np.where(ok, lon, 0.0), np.where(ok, lat, 0.0), finest)
    by_level: dict[int, list[tuple[int, int]]] = {}
    for i, (z, x, y) in enumerate(tiles):
        by_level.setdefault(z, []).append((y * 2 ** (z + 1) + x, i))
    for z, entries in by_level.items():
        entries.sort()
        codes = np.array([c for c, _ in entries], dtype="int64")
        idx = np.array([i for _, i in entries], dtype="int64")
        shift = finest - z
        code = (iy >> shift) * 2 ** (z + 1) + (ix >> shift)
        pos = np.searchsorted(codes, code)
        pos = np.minimum(pos, len(codes) - 1)
        hit = ok & (codes[pos] == code)
        out[hit] = idx[pos[hit]]
    return out
