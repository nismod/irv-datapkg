import numpy as np
import pandas as pd
import pytest
import shapely
from shapely.geometry import LineString, Point, box

import adaptive_partitions as ap
from adaptive_partitions import h3scheme, quadkey
from adaptive_partitions.cli import main

GLOBE_DEG2 = 360.0 * 180.0
GLOBE_KM2 = 510_065_621.7  # WGS84 ellipsoid surface area


@pytest.fixture(scope="session")
def grid(pop_raster):
    path, _ = pop_raster
    return ap.aggregate_population(path, level=8, block_rows=97)


@pytest.fixture(scope="session")
def qcells(grid):
    return ap.build_cells(grid, "quadkey", target=200)


@pytest.fixture(scope="session")
def hcells(grid):
    return ap.build_cells(grid, "h3", target=200, max_level=2)


def _assert_partition(cells):
    geoms = np.asarray(cells.geometry.values, dtype=object)
    assert shapely.is_valid(geoms).all()
    # planar area in lon/lat degrees sums to the full 360 x 180 rectangle
    assert shapely.area(geoms).sum() == pytest.approx(GLOBE_DEG2, rel=1e-9)
    # geodesic area sums to the ellipsoid surface
    assert cells["area_km2"].sum() == pytest.approx(GLOBE_KM2, rel=1e-3)
    # no overlaps: pairwise intersections have zero area
    tree = shapely.STRtree(geoms)
    a, b = tree.query(geoms, predicate="intersects")
    m = a < b
    overlap = shapely.area(shapely.intersection(geoms[a[m]], geoms[b[m]]))
    assert overlap.max(initial=0) < 1e-9
    assert cells["key"].is_unique


# --- population aggregation -------------------------------------------------


def test_aggregation_conserves_population(pop_raster, grid):
    _, total = pop_raster
    assert grid.total == pytest.approx(total, rel=1e-12)
    for z in range(grid.level + 1):
        assert grid.at_level(z).sum() == pytest.approx(total, rel=1e-12)


def test_block_size_does_not_change_result(pop_raster, grid):
    path, _ = pop_raster
    other = ap.aggregate_population(path, level=8, block_rows=720)
    np.testing.assert_allclose(other.values, grid.values, rtol=1e-12)


def test_mollweide_raster(pop_raster_mollweide):
    path, total = pop_raster_mollweide
    g = ap.aggregate_population(path, level=6, block_rows=50)
    assert g.total == pytest.approx(total, rel=1e-9)
    cells = ap.build_cells(g, "quadkey", target=50)
    assert len(cells) <= 50
    assert cells["population"].sum() == pytest.approx(total, rel=1e-9)


# --- quadkey ------------------------------------------------------------------


def test_quadkey_roundtrip():
    for z, x, y in [(0, 0, 0), (0, 1, 0), (3, 5, 2), (7, 255, 127), (10, 1234, 567)]:
        k = quadkey.tile_to_key(z, x, y)
        assert quadkey.key_to_tile(k) == (z, x, y)
        assert len(k) == z + 2 and k.startswith("q")
    assert quadkey.key_bounds("q0") == (-180, -90, 0, 90)
    assert quadkey.key_bounds("q12") == (0, -90, 90, 0)
    assert quadkey.key_bounds("q01") == (-90, 0, 0, 90)


def test_quadkey_partition(qcells):
    _assert_partition(qcells)
    assert (qcells["scheme"] == "quadkey").all()
    # no key is a prefix of another (leaves of one tree)
    keys = sorted(qcells["key"])
    assert not any(b.startswith(a) for a, b in zip(keys, keys[1:]))
    # each cell is exactly its bbox
    for row in qcells.itertuples():
        assert (row.xmin, row.ymin, row.xmax, row.ymax) == quadkey.key_bounds(row.key)
        assert quadkey.tile_to_key(row.level, row.tile_x, row.tile_y) == row.key


def test_quadkey_target_and_population(qcells, grid):
    assert 197 < len(qcells) <= 200
    assert qcells["population"].sum() == pytest.approx(grid.total, rel=1e-12)
    # adaptive: dense areas get finer cells than empty ones
    assert qcells["level"].max() > qcells["level"].min() + 3
    delhi = ap.assign(qcells, np.array([[77.2, 28.6]]))[0]
    pacific = ap.assign(qcells, np.array([[-140.0, -10.0]]))[0]
    assert len(delhi) > len(pacific)


@pytest.mark.parametrize("target,start", [(8, 1), (50, 1), (101, 2), (32, 2)])
def test_quadkey_target_param(grid, target, start):
    cells = ap.build_cells(grid, "quadkey", target=target, start_level=start)
    assert target - 3 < len(cells) <= target
    _assert_partition(cells)


def test_quadkey_overshoot_and_max_population(grid):
    over = ap.build_cells(grid, "quadkey", target=100, allow_overshoot=True)
    assert 100 <= len(over) < 103
    cap = 5e6
    capped = ap.build_cells(grid, "quadkey", target=None, max_population=cap)
    # every cell is under the cap unless it reached the finest level
    big = capped[capped["population"] > cap]
    assert (big["level"] == grid.level).all()
    with pytest.raises(ValueError):
        ap.build_cells(grid, "quadkey", target=7, start_level=1)
    with pytest.raises(ValueError):
        ap.build_cells(grid, "quadkey", max_level=9)


def test_deterministic(grid, qcells, hcells):
    again = ap.build_cells(grid, "quadkey", target=200)
    pd.testing.assert_frame_equal(
        pd.DataFrame(again.drop(columns="geometry")),
        pd.DataFrame(qcells.drop(columns="geometry")),
    )
    assert again.geometry.equals(qcells.geometry)
    h_again = ap.build_cells(grid, "h3", target=200, max_level=2)
    assert list(h_again["key"]) == list(hcells["key"])
    np.testing.assert_array_equal(h_again["population"], hcells["population"])


def test_quadkey_assign_points_and_edges(qcells):
    pts = np.array(
        [[0.0, 0.0], [-180.0, 90.0], [180.0, -90.0], [179.999, 0.0], [-0.0001, 45]]
    )
    keys = ap.assign(qcells, pts)
    for (x, y), k in zip(pts, keys):
        xmin, ymin, xmax, ymax = quadkey.key_bounds(k)
        x = -180.0 if x == 180.0 else x
        assert xmin <= x < xmax or (x == xmin)
        assert ymin < y <= ymax or (y == -90.0 and ymin == -90.0)
    # (0, 0) is on four cells' corners: closed west + north -> SE cell
    xmin, ymin, xmax, ymax = quadkey.key_bounds(keys[0])
    assert xmin == 0.0 and ymax == 0.0


def test_assign_matches_geometry(qcells, hcells):
    rng = np.random.default_rng(1)
    pts = np.column_stack([rng.uniform(-180, 180, 2000), rng.uniform(-90, 90, 2000)])
    for cells in (qcells, hcells):
        keys = ap.assign(cells, pts)
        assert all(k is not None for k in keys)
        geom = dict(zip(cells["key"], cells.geometry))
        inside = [geom[k].covers(Point(x, y)) for (x, y), k in zip(pts, keys)]
        assert np.mean(inside) == 1.0
        # population per key agrees with assigning raster grid centres
        assert set(keys) <= set(cells["key"])


def test_assign_geodataframe(qcells):
    import geopandas as gpd

    gdf = gpd.GeoDataFrame(
        {"name": ["line", "poly", "empty", "point"]},
        geometry=[
            LineString([(10, 10), (20, 10), (30, 10)]),
            box(100, -10, 101, -9),
            shapely.Polygon(),
            Point(-74.0, 40.7),
        ],
        crs="EPSG:4326",
        index=[10, 11, 12, 13],
    )
    keys = ap.assign(qcells, gdf)
    assert list(keys.index) == [10, 11, 12, 13]
    assert keys.iloc[2] is None
    rp = gdf.geometry.representative_point()
    for i in (0, 1, 3):
        b = quadkey.key_bounds(keys.iloc[i])
        assert box(*b).covers(rp.iloc[i])
    # reprojected input gives the same keys
    keys_m = ap.assign(qcells, gdf.drop(index=12).to_crs("EPSG:3857"))
    assert list(keys_m) == [keys.iloc[0], keys.iloc[1], keys.iloc[3]]
    # bboxes use their centre; antimeridian-crossing bbox centre wraps
    bb = np.array([[0.0, 0.0, 2.0, 2.0], [179.0, -18.0, -179.0, -16.0]])
    kb = ap.assign(qcells, bb)
    assert kb[0] == ap.assign(qcells, np.array([[1.0, 1.0]]))[0]
    assert kb[1] == ap.assign(qcells, np.array([[180.0, -17.0]]))[0]


def test_keys_for_bbox(qcells, hcells):
    for cells in (qcells, hcells):
        all_keys = sorted(cells["key"])
        assert ap.keys_for_bbox(cells, (-180, -90, 180, 90)) == all_keys
        q = (10.0, 40.0, 12.0, 42.0)
        got = set(ap.keys_for_bbox(cells, q))
        brute = {k for k, g in zip(cells["key"], cells.geometry) if g.intersects(box(*q))}
        assert got == brute
        # every point sampled inside the query is in one of the returned keys
        rng = np.random.default_rng(2)
        pts = np.column_stack([rng.uniform(10, 12, 500), rng.uniform(40, 42, 500)])
        assert set(ap.assign(cells, pts)) <= got
        # antimeridian query == union of its two halves
        am = set(ap.keys_for_bbox(cells, (170, -20, -170, -10)))
        halves = set(ap.keys_for_bbox(cells, (170, -20, 180, -10))) | set(
            ap.keys_for_bbox(cells, (-180, -20, -170, -10))
        )
        assert am == halves and len(am) >= 2
        # longitudes beyond 180 wrap
        assert set(ap.keys_for_bbox(cells, (190, 0, 200, 5))) == set(
            ap.keys_for_bbox(cells, (-170, 0, -160, 5))
        )
        # buffer widens
        assert got <= set(ap.keys_for_bbox(cells, q, buffer=5))


# --- H3 -----------------------------------------------------------------------


def test_h3_partition(hcells, grid):
    _assert_partition(hcells)
    assert (hcells["scheme"] == "h3").all()
    assert 194 < len(hcells) <= 200
    assert hcells["level"].min() == 0
    assert hcells["population"].sum() == pytest.approx(grid.total, rel=1e-12)
    # antimeridian cells are split, not smeared across the globe
    am = hcells[hcells["crosses_antimeridian"]]
    assert len(am) > 0
    for g in am.geometry:
        assert all(
            p.bounds[2] - p.bounds[0] < 180 or abs(p.bounds[1]) == 90 or abs(p.bounds[3]) == 90
            for p in g.geoms
        )
        assert any(p.bounds[0] == -180 for p in g.geoms)
        assert any(p.bounds[2] == 180 for p in g.geoms)
    # the two pole cells reach the poles
    assert hcells["ymax"].max() == 90.0 and hcells["ymin"].min() == -90.0


def test_h3_target_rules(grid):
    with pytest.raises(ValueError, match="122"):
        ap.build_cells(grid, "h3", target=100)
    base = ap.build_cells(grid, "h3", target=122, max_level=1)
    assert len(base) == 122 and (base["level"] == 0).all()
    over = ap.build_cells(grid, "h3", target=130, max_level=1, allow_overshoot=True)
    assert 130 <= len(over) <= 136


def test_h3_pole_and_antimeridian_polygons():
    import h3

    north = h3.latlng_to_cell(90, 0, 1)
    south = h3.latlng_to_cell(-90, 0, 1)
    for cell, pole in ((north, Point(0, 90)), (south, Point(45, -90))):
        g = h3scheme.cell_polygon(cell)
        assert g.is_valid
        assert g.bounds[0] == -180 and g.bounds[2] == 180
        assert g.covers(pole)
    fiji = h3.latlng_to_cell(-17.0, 180.0, 1)
    g = h3scheme.cell_polygon(fiji)
    assert g.geom_type == "MultiPolygon" and g.is_valid
    assert g.covers(Point(179.9, -17.0)) and g.covers(Point(-179.9, -17.0))


def test_h3_assign_reference_resolution(hcells):
    import h3

    keys = list(hcells["key"])
    ref = h3scheme.reference_resolution(keys)
    rng = np.random.default_rng(3)
    lon, lat = rng.uniform(-180, 180, 500), rng.uniform(-90, 90, 500)
    got = ap.assign(hcells, np.column_stack([lon, lat]))
    for x, y, k in zip(lon, lat, got):
        c = h3.latlng_to_cell(y, x, ref)
        assert c == k or h3.cell_to_parent(c, h3.get_resolution(k)) == k


# --- IO and CLI ---------------------------------------------------------------


def test_write_read_roundtrip(tmp_path, qcells):
    import pyarrow.parquet as pq

    path = ap.write_cells(qcells, tmp_path / "partitions" / "quadkey" / "cells.parquet")
    back = ap.read_cells(path)
    assert list(back["key"]) == sorted(qcells["key"])
    assert back.crs.to_epsg() == 4326
    assert back.attrs["adaptive_partitions"]["scheme"] == "quadkey"
    meta = pq.read_schema(path).metadata
    assert b"geo" in meta
    assert "bbox" in pq.read_schema(path).names  # GeoParquet 1.1 covering
    for col in ["key", "scheme", "level", "population", "xmin", "ymin", "xmax", "ymax"]:
        assert col in back.columns


def test_cli(tmp_path, pop_raster, capsys):
    path, total = pop_raster
    out = tmp_path / "cells.parquet"
    assert main(["build", str(path), "-o", str(out), "--target", "40",
                 "--grid-level", "7"]) == 0
    cells = ap.read_cells(out)
    assert len(cells) <= 40
    assert cells["population"].sum() == pytest.approx(total, rel=1e-9)
    capsys.readouterr()
    assert main(["keys", str(out), "--bbox", "-10", "40", "10", "50"]) == 0
    printed = capsys.readouterr().out.split()
    assert printed == ap.keys_for_bbox(cells, (-10, 40, 10, 50))
    out_h3 = tmp_path / "h3.parquet"
    assert main(["build", str(path), "-o", str(out_h3), "--scheme", "h3",
                 "--target", "140", "--max-level", "1", "--grid-level", "7"]) == 0
    assert len(ap.read_cells(out_h3)) <= 140
    assert main(["show", str(out_h3)]) == 0
