"""Scheme-independent API: build, assign, query, read and write cell lists."""

from __future__ import annotations

import json
from pathlib import Path

import numpy as np
import pandas as pd
import shapely
from shapely.geometry import box

from . import h3scheme, quadkey
from .population import PopulationGrid, aggregate_population

__all__ = [
    "SCHEMES",
    "build_cells",
    "cells_from_keys",
    "assign",
    "keys_for_bbox",
    "write_cells",
    "read_cells",
]

SCHEMES = ("quadkey", "h3")
METADATA_KEY = b"adaptive_partitions"
COLUMNS = ["key", "scheme", "level", "population", "xmin", "ymin", "xmax", "ymax"]


def _version() -> str:
    from . import __version__

    return __version__


def build_cells(
    population: str | Path | PopulationGrid,
    scheme: str = "quadkey",
    target: int | None = 200,
    start_level: int | None = None,
    max_level: int | None = None,
    max_population: float | None = None,
    allow_overshoot: bool = False,
    grid_level: int | None = None,
    block_rows: int = 512,
):
    """Build an adaptive partition and return it as a GeoDataFrame.

    Parameters
    ----------
    population
        Path to a population raster (streamed, see
        :func:`aggregate_population`) or a pre-computed :class:`PopulationGrid`.
    scheme
        ``"quadkey"`` (geographic quadtree, default) or ``"h3"``.
    target
        Maximum number of cells (approximate target). The cell count ends at
        most ``target`` and within one split's growth of it (3 for quadkey,
        6 for H3), unless another stopping rule fires first.
    start_level
        Level (quadkey, default 1: 8 cells) or resolution (H3, default 0: 122
        cells) to start from.
    max_level
        Finest level/resolution a cell may be split to. Quadkey default: the
        grid level. H3 default: 4.
    max_population
        Optional: stop when no cell has more than this many people (with or
        without ``target``).
    allow_overshoot
        Let the last split pass ``target`` instead of stopping just below it.
    grid_level
        Level of the geographic grid the raster is first summed onto.
        Default 10 (~0.18 degrees) for quadkey, 11 for H3.
    """
    if scheme not in SCHEMES:
        raise ValueError(f"scheme must be one of {SCHEMES}")
    if isinstance(population, PopulationGrid):
        grid = population
    else:
        if grid_level is None:
            grid_level = max(10 if scheme == "quadkey" else 11, max_level or 0)
        grid = aggregate_population(population, level=grid_level, block_rows=block_rows)

    params = dict(
        scheme=scheme,
        target=target,
        max_population=max_population,
        allow_overshoot=allow_overshoot,
        grid_level=grid.level,
        source=grid.source and Path(grid.source).name,
        total_population=grid.total,
        version=_version(),
    )
    if scheme == "quadkey":
        start = 1 if start_level is None else start_level
        result = quadkey.build_quadkey(
            grid,
            target=target,
            start_level=start,
            max_level=max_level,
            max_population=max_population,
            allow_overshoot=allow_overshoot,
        )
        params.update(start_level=start, max_level=max_level or grid.level)
    else:
        start = 0 if start_level is None else start_level
        mx = 4 if max_level is None else max_level
        result = h3scheme.build_h3(
            grid,
            target=target,
            start_res=start,
            max_res=mx,
            max_population=max_population,
            allow_overshoot=allow_overshoot,
        )
        params.update(start_level=start, max_level=mx)

    keys = [k for k, _ in result]
    cells = cells_from_keys(keys, scheme, population=[p for _, p in result])
    cells.attrs["adaptive_partitions"] = params
    return cells


def cells_from_keys(keys, scheme: str, population=None):
    """GeoDataFrame of cells (geometry, bbox, level) for a list of keys."""
    import geopandas as gpd
    from pyproj import Geod

    keys = sorted(keys) if population is None else list(keys)
    if scheme == "quadkey":
        levels = [len(k) - 2 for k in keys]
        geoms = [box(*quadkey.key_bounds(k)) for k in keys]
    elif scheme == "h3":
        import h3

        levels = [h3.get_resolution(k) for k in keys]
        geoms = h3scheme.key_geometries(keys)
    else:
        raise ValueError(f"scheme must be one of {SCHEMES}")
    bounds = shapely.bounds(np.array(geoms, dtype=object))
    geod = Geod(ellps="WGS84")
    # densify so that straight lon/lat edges (parallels) are not taken as
    # geodesics when computing ellipsoidal area
    area = [
        abs(geod.geometry_area_perimeter(shapely.segmentize(g, 0.1))[0]) / 1e6
        for g in geoms
    ]
    df = pd.DataFrame(
        {
            "key": keys,
            "scheme": scheme,
            "level": np.asarray(levels, dtype="int8"),
            "population": (
                np.asarray(population, dtype="float64")
                if population is not None
                else np.nan
            ),
            "xmin": bounds[:, 0],
            "ymin": bounds[:, 1],
            "xmax": bounds[:, 2],
            "ymax": bounds[:, 3],
            "area_km2": area,
        }
    )
    if scheme == "quadkey":
        # OGC WorldCRS84Quad tile matrix column/row of each cell
        tiles = np.array([quadkey.key_to_tile(k)[1:] for k in keys], dtype="int64")
        df["tile_x"] = tiles[:, 0] if len(keys) else []
        df["tile_y"] = tiles[:, 1] if len(keys) else []
    if scheme == "h3":
        df["crosses_antimeridian"] = [
            g.geom_type == "MultiPolygon" and b[0] == -180 and b[2] == 180
            for g, b in zip(geoms, bounds)
        ]
    return gpd.GeoDataFrame(df, geometry=geoms, crs="EPSG:4326")


def _scheme_of(cells) -> str:
    schemes = set(cells["scheme"])
    if len(schemes) != 1:
        raise ValueError(f"cells must have exactly one scheme, got {schemes}")
    return schemes.pop()


def _points(data, how: str):
    """Representative lon/lat for geometries, points or bboxes."""
    import geopandas as gpd

    if isinstance(data, (gpd.GeoDataFrame, gpd.GeoSeries)):
        if data.crs is not None and not data.crs.equals("EPSG:4326"):
            data = data.to_crs("EPSG:4326")
        geoms = np.asarray(data.geometry.values, dtype=object)
    elif isinstance(data, np.ndarray) and data.dtype != object:
        arr = np.asarray(data, dtype="float64")
        if arr.ndim == 2 and arr.shape[1] == 2:
            return arr[:, 0], arr[:, 1]
        if arr.ndim == 2 and arr.shape[1] == 4:
            xmin, ymin, xmax, ymax = arr.T
            # bbox crossing the antimeridian has xmin > xmax
            xmax = np.where(xmin > xmax, xmax + 360.0, xmax)
            return (xmin + xmax) / 2.0, (ymin + ymax) / 2.0
        raise ValueError("arrays must be (N, 2) points or (N, 4) bboxes")
    else:
        geoms = np.asarray(list(data), dtype=object)

    missing = shapely.is_missing(geoms) | shapely.is_empty(geoms)
    geoms = np.where(missing, None, geoms)
    if how == "representative_point":
        pts = shapely.point_on_surface(geoms)
    elif how == "centroid":
        pts = shapely.centroid(geoms)
    elif how == "bbox_centre":
        b = shapely.bounds(geoms)
        return (b[:, 0] + b[:, 2]) / 2.0, (b[:, 1] + b[:, 3]) / 2.0
    else:
        raise ValueError(
            "how must be 'representative_point', 'centroid' or 'bbox_centre'"
        )
    return shapely.get_x(pts), shapely.get_y(pts)


def assign(cells, data, how: str = "representative_point"):
    """Partition key for each feature, point or bbox.

    ``data`` may be a GeoDataFrame/GeoSeries (reprojected to EPSG:4326 if
    needed), a sequence of shapely geometries, an ``(N, 2)`` array of
    lon/lat points or an ``(N, 4)`` array of ``xmin, ymin, xmax, ymax``
    bboxes (``xmin > xmax`` means the box crosses the antimeridian).

    A feature crossing cell boundaries goes to exactly one partition: the
    cell containing its **representative point** (``shapely``
    ``point_on_surface``, which always lies on the geometry; ``how`` can
    also be ``"centroid"`` or ``"bbox_centre"``; bboxes always use their
    centre). Points on a quadkey cell edge go to the cell to the east/south
    (cells are closed on their west and north edges). Missing or empty
    geometries get ``None``.

    Returns a pandas Series aligned with a GeoDataFrame/GeoSeries input,
    else a numpy object array.
    """
    scheme = _scheme_of(cells)
    keys = list(cells["key"])
    lon, lat = _points(data, how)
    if scheme == "quadkey":
        idx = quadkey.assign_points(keys, lon, lat)
    else:
        idx = h3scheme.assign_points(keys, lon, lat)
    lookup = np.array(keys + [None], dtype=object)
    out = lookup[np.where(idx < 0, len(keys), idx)]
    if hasattr(data, "index") and not isinstance(data, np.ndarray):
        return pd.Series(out, index=data.index, name="partition_key", dtype=object)
    return out


def keys_for_bbox(cells, bbox, buffer: float = 0.0) -> list[str]:
    """Keys of cells whose region intersects a query bbox (edges included).

    ``bbox`` is ``(xmin, ymin, xmax, ymax)`` in degrees. ``xmin > xmax``
    means the query crosses the antimeridian. ``buffer`` (degrees) widens
    the query. Features are partitioned by representative point, so a
    feature can reach beyond its cell. To find every feature that
    *intersects* the query, either buffer by your largest feature extent or
    read the partitions listed here and also filter on the per-row bbox
    covering column.
    """
    xmin, ymin, xmax, ymax = (float(v) for v in bbox)
    ymin, ymax = max(ymin - buffer, -90.0), min(ymax + buffer, 90.0)
    xmin, xmax = xmin - buffer, xmax + buffer
    if xmax - xmin >= 360.0 or (xmin <= -180.0 and xmax >= 180.0):
        queries = [box(-180.0, ymin, 180.0, ymax)]
    elif xmin > xmax:
        queries = [box(xmin, ymin, 180.0, ymax), box(-180.0, ymin, xmax, ymax)]
    else:
        queries = []
        for k in (-360.0, 0.0, 360.0):
            a, b = max(xmin + k, -180.0), min(xmax + k, 180.0)
            if a <= b:
                queries.append(box(a, ymin, b, ymax))
    geoms = np.asarray(cells.geometry.values, dtype=object)
    hit = np.zeros(len(cells), dtype=bool)
    for q in queries:
        hit |= shapely.intersects(geoms, q)
    return sorted(np.asarray(cells["key"])[hit].tolist())


def write_cells(cells, path: str | Path) -> Path:
    """Write cells as GeoParquet 1.1 (bbox covering column, build metadata)."""
    import pyarrow.parquet as pq

    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    cells = cells.sort_values("key").reset_index(drop=True)
    cells.to_parquet(path, index=False, write_covering_bbox=True)
    params = cells.attrs.get("adaptive_partitions")
    if params:
        table = pq.read_table(path)
        meta = dict(table.schema.metadata or {})
        meta[METADATA_KEY] = json.dumps(params, sort_keys=True).encode()
        pq.write_table(table.replace_schema_metadata(meta), path)
    return path


def read_cells(path: str | Path):
    """Read a cell list written by :func:`write_cells`."""
    import geopandas as gpd
    import pyarrow.parquet as pq

    cells = gpd.read_parquet(path)
    if "bbox" in cells.columns:
        cells = cells.drop(columns="bbox")
    meta = pq.read_schema(path).metadata or {}
    if METADATA_KEY in meta:
        cells.attrs["adaptive_partitions"] = json.loads(meta[METADATA_KEY])
    return cells
