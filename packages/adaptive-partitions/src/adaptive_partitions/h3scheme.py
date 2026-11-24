"""Adaptive H3 partition.

H3 specifics
------------
* Resolution 0 already has 122 cells (110 hexagons, 12 pentagons) and there
  is nothing coarser, so the default start is resolution 0 and a target of
  ~200 means only ~13 splits. Splitting a hexagon adds 6 cells net, a
  pentagon 5. ``target`` below 122 is rejected.
* H3 children do not tile their parent exactly (aperture 7 is not
  congruent), so a mixed-resolution set of cells is *not* a partition when
  you use the cell polygons. It is a partition in index space, which is what
  this module uses. The **reference resolution** ``R`` of a cell list is its
  finest resolution. A point belongs to key ``k`` when the point's
  resolution-``R`` cell is ``k`` or a descendant of ``k``. The region of a
  key is therefore the union of its resolution-``R`` descendants. That
  region is the published geometry, so geometries tile the globe with no
  gaps or overlaps and agree with :func:`assign_points`. ``R`` comes from
  the key list alone, so it needs no extra metadata.
* Antimeridian: cells that cross it are split into a MultiPolygon at
  +/-180. Poles: the two cells that contain a pole are closed along the
  pole line (lat +/-90), so they are valid lon/lat polygons. Edges are
  densified along great circles for coarse resolutions.
"""

from __future__ import annotations

import heapq
from collections import defaultdict

import h3
import numpy as np
import shapely
from shapely import affinity
from shapely.geometry import MultiPolygon, Polygon, box

from .population import PopulationGrid

__all__ = [
    "build_h3",
    "reference_resolution",
    "assign_points",
    "key_geometries",
    "cell_polygon",
    "population_by_key",
]

WORLD = box(-180.0, -90.0, 180.0, 90.0)


def _cells_at(res: int) -> list[str]:
    base = sorted(h3.get_res0_cells())
    if res == 0:
        return base
    out = []
    for c in base:
        out.extend(h3.cell_to_children(c, res))
    return sorted(out)


def _fine_population(grid: PopulationGrid, res: int) -> dict[str, float]:
    """Population per resolution-``res`` cell, by grid-cell centre."""
    lon, lat, pop = grid.nonzero_centres()
    cells = [h3.latlng_to_cell(float(y), float(x), res) for x, y in zip(lon, lat)]
    if not cells:
        return {}
    uniq, inv = np.unique(np.array(cells), return_inverse=True)
    sums = np.bincount(inv, weights=pop)
    return dict(zip(uniq.tolist(), sums.tolist()))


def _rollup(fine: dict[str, float], fine_res: int, res: int) -> dict[str, float]:
    if res == fine_res:
        return dict(fine)
    out: dict[str, float] = defaultdict(float)
    for c, p in fine.items():
        out[h3.cell_to_parent(c, res)] += p
    return dict(out)


def reference_resolution(keys) -> int:
    return max(h3.get_resolution(k) for k in keys)


def build_h3(
    grid: PopulationGrid,
    target: int | None = 200,
    start_res: int = 0,
    max_res: int = 4,
    max_population: float | None = None,
    allow_overshoot: bool = False,
) -> list[tuple[str, float]]:
    """Adaptive H3: split the most populous cell until ``target`` cells.

    Populations used to decide splits are estimated at ``max_res`` (grid-cell
    centres to resolution-``max_res`` cells, rolled up by H3 parent). Stopping
    rules match :func:`adaptive_partitions.quadkey.build_quadkey`. Returns
    ``[(key, population), ...]`` sorted by key. The returned population is
    recomputed at the final reference resolution, so it matches
    :func:`assign_points`.
    """
    from .quadkey import _target_reached

    if target is None and max_population is None:
        raise ValueError("give target, max_population or both")
    if not 0 <= start_res <= max_res <= 15:
        raise ValueError("need 0 <= start_res <= max_res <= 15")
    start = _cells_at(start_res)
    if target is not None and len(start) > target:
        raise ValueError(
            f"H3 resolution {start_res} already has {len(start)} cells, "
            f"more than target {target} (resolution 0 has 122)"
        )

    fine = _fine_population(grid, max_res)
    pops = {r: _rollup(fine, max_res, r) for r in range(start_res, max_res + 1)}

    def pop_of(c):
        return pops[h3.get_resolution(c)].get(c, 0.0)

    heap = [(-pop_of(c), c) for c in start]
    heapq.heapify(heap)
    final: list[str] = []
    count = len(start)
    while heap:
        neg_pop, key = heap[0]
        pop = -neg_pop
        if pop <= 0:
            break
        if max_population is not None and pop <= max_population:
            break
        res = h3.get_resolution(key)
        if res >= max_res:
            heapq.heappop(heap)
            final.append(key)
            continue
        kids = sorted(h3.cell_to_children(key, res + 1))
        if _target_reached(count, len(kids) - 1, target, allow_overshoot):
            break
        heapq.heappop(heap)
        for c in kids:
            heapq.heappush(heap, (-pop_of(c), c))
        count += len(kids) - 1

    final.extend(k for _, k in heap)
    final.sort()
    by_key = population_by_key(grid, final)
    return [(k, by_key.get(k, 0.0)) for k in final]


def population_by_key(grid: PopulationGrid, keys) -> dict[str, float]:
    """Population per key under the reference-resolution rule."""
    keys = list(keys)
    lon, lat, pop = grid.nonzero_centres()
    idx = assign_points(keys, lon, lat)
    sums = np.bincount(idx[idx >= 0], weights=pop[idx >= 0], minlength=len(keys))
    return dict(zip(keys, sums.tolist()))


def assign_points(keys, lon, lat) -> np.ndarray:
    """Index into ``keys`` of the key each point belongs to (-1 if none).

    Rule: take the point's cell at the reference resolution ``R`` (the
    finest resolution in ``keys``), then walk up its parents until one is in
    ``keys``.
    """
    keys = list(keys)
    lookup = {k: i for i, k in enumerate(keys)}
    lon = np.asarray(lon, dtype="float64")
    lat = np.asarray(lat, dtype="float64")
    out = np.full(lon.shape, -1, dtype="int64")
    if not keys:
        return out
    ref = reference_resolution(keys)
    present = sorted({h3.get_resolution(k) for k in keys}, reverse=True)
    cache: dict[str, int] = {}
    flat_out = out.reshape(-1)
    for j, (x, y) in enumerate(zip(lon.reshape(-1), lat.reshape(-1))):
        if not (np.isfinite(x) and np.isfinite(y)):
            continue
        c = h3.latlng_to_cell(float(np.clip(y, -90, 90)), float(x), ref)
        hit = cache.get(c)
        if hit is None:
            hit = -1
            for r in present:
                p = c if r == ref else h3.cell_to_parent(c, r)
                if p in lookup:
                    hit = lookup[p]
                    break
            cache[c] = hit
        flat_out[j] = hit
    return out


# --- geometry -------------------------------------------------------------


def _slerp_ring(latlng, max_step: float):
    """Densify a closed ring of (lat, lng) vertices along great circles.

    H3 edges are great-circle arcs; in lon/lat they curve, strongly near the
    poles. Each edge gets enough points that no step exceeds ``max_step``
    degrees of arc, or a tenth of that within 10 degrees of a pole.
    """
    pts = np.radians(np.asarray(latlng, dtype="float64"))
    lat, lng = pts[:, 0], pts[:, 1]
    xyz = np.stack(
        [np.cos(lat) * np.cos(lng), np.cos(lat) * np.sin(lng), np.sin(lat)], axis=1
    )
    a, b = xyz, np.roll(xyz, -1, axis=0)
    omega = np.arccos(np.clip(np.einsum("ij,ij->i", a, b), -1.0, 1.0))
    polar = np.maximum(np.abs(lat), np.roll(np.abs(lat), -1)) > np.radians(80)
    step = np.radians(max_step) * np.where(polar, 0.1, 1.0)
    nseg = np.maximum(1, np.ceil(omega / step).astype(int))
    out = []
    for ai, bi, om, n in zip(a, b, omega, nseg):
        t = np.arange(n)[:, None] / n
        if om < 1e-12:
            out.append(ai[None, :] * np.ones_like(t))
        else:
            out.append(
                (np.sin((1 - t) * om) * ai + np.sin(t * om) * bi) / np.sin(om)
            )
    xyz = np.concatenate(out)
    lon = np.degrees(np.arctan2(xyz[:, 1], xyz[:, 0]))
    lat = np.degrees(np.arcsin(np.clip(xyz[:, 2], -1.0, 1.0)))
    return lon, lat


def _split_world(poly: Polygon):
    """Fold a polygon in unwrapped longitudes back into [-180, 180]."""
    parts = []
    for k in (-360.0, 0.0, 360.0):
        piece = affinity.translate(poly, xoff=k).intersection(WORLD)
        if piece.is_empty:
            continue
        for g in getattr(piece, "geoms", [piece]):
            if isinstance(g, Polygon) and g.area > 0:
                parts.append(g)
    if len(parts) == 1:
        return parts[0]
    # pieces of a pole cell meet along the seam where the ring was opened
    merged = shapely.union_all(parts)
    return merged if merged.geom_type == "Polygon" else MultiPolygon(list(merged.geoms))


def cell_polygon(cell: str, max_step: float = 1.0):
    """Lon/lat (Multi)Polygon of an H3 cell, antimeridian- and pole-safe.

    Edges are densified along great circles (``max_step`` degrees of arc,
    finer near the poles).
    """
    res = h3.get_resolution(cell)
    lon, lat = _slerp_ring(h3.cell_to_boundary(cell), max_step)
    lon = np.degrees(np.unwrap(np.radians(lon)))
    pole = None
    if h3.latlng_to_cell(90.0, 0.0, res) == cell:
        pole = 90.0
    elif h3.latlng_to_cell(-90.0, 0.0, res) == cell:
        pole = -90.0
    if pole is not None:
        if lon[-1] < lon[0]:
            lon, lat = lon[::-1], lat[::-1]
        lon0 = lon[0]
        coords = list(zip(lon, lat))
        coords += [(lon0 + 360.0, lat[0]), (lon0 + 360.0, pole), (lon0, pole)]
        return _split_world(Polygon(coords))
    poly = Polygon(list(zip(lon, lat)))
    if lon.min() < -180.0 or lon.max() > 180.0:
        return _split_world(poly)
    return poly


def key_geometries(keys, grid_size: float = 1e-9) -> list:
    """Region of each key: union of its reference-resolution descendants."""
    keys = list(keys)
    ref = reference_resolution(keys)
    out = []
    for k in keys:
        r = h3.get_resolution(k)
        cells = [k] if r == ref else h3.cell_to_children(k, ref)
        polys = shapely.set_precision(
            np.array([cell_polygon(c) for c in sorted(cells)], dtype=object),
            grid_size,
        )
        # descendants share snapped edges exactly, so a coverage union (fast)
        # suffices; fall back to a full overlay union if it is not clean
        geom = shapely.coverage_union_all(polys)
        if not geom.is_valid:
            geom = shapely.union_all(polys, grid_size=grid_size)
        if not geom.is_valid:
            geom = shapely.make_valid(geom)
        out.append(geom)
    return out
