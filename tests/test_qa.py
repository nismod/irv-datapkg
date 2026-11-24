"""Deep validation (STAC schemas, COG and GeoParquet profiles) and checksum audit."""

import csv
import json
import shutil

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
import rasterio
from click.testing import CliRunner

from irv_datapkg import recipes
from irv_datapkg.catalog import Catalog, qa, validate_catalog
from irv_datapkg.cli import main

from test_recipes import make_gdf, make_tif


@pytest.fixture
def cat(catalogue, tmp_path):
    """A writable copy of the fixture catalogue."""
    base, private = catalogue
    shutil.copytree(base, tmp_path / "base")
    shutil.copytree(private, tmp_path / "base-private")
    return tmp_path / "base"


def _item(base, dataset, version, item_id):
    path = base / dataset / version / f"{item_id}.json"
    return path, json.loads(path.read_text())


def _rewrite_same_size(path):
    """Change bytes without changing size (so only the sha256 catches it)."""
    data = bytearray(path.read_bytes())
    data[-1] ^= 0xFF
    path.write_bytes(bytes(data))


# -- deep validation of the whole catalogue ---------------------------------


def test_fixture_catalogue_is_deep_valid(catalogue):
    base, _ = catalogue
    warnings = []
    assert validate_catalog(str(base), deep=True, warnings=warnings) == []
    # the fixture COGs are DEFLATE without a predictor: a warning, not an error
    assert warnings and all("predictor" in w for w in warnings)


def test_deep_reports_stac_schema_errors(cat):
    path, item = _item(cat, "flood", "v1-r1", "river_rcp45_rp10")
    item["properties"]["proj:epsg"] = "EPSG:4326"  # projection extension: integer
    path.write_text(json.dumps(item))
    assert validate_catalog(str(cat)) == []  # the irv schema does not cover proj:*
    errors = validate_catalog(str(cat), deep=True)
    assert any("projection" in e and "proj:epsg" in e for e in errors), errors


def test_deep_reports_bad_files(cat):
    path, item = _item(cat, "flood", "v1-r1", "river_rcp45_rp10")
    tif = path.parent / "flood_v1-r1_rp10.tif"
    item["assets"]["data"]["raster:bands"][0]["nodata"] = -1.0
    path.write_text(json.dumps(item))
    pq_path = cat / "points" / "1.0-r1" / "points.parquet"
    item_path, pitem = _item(cat, "points", "1.0-r1", "points")
    pitem["assets"]["data"]["table:row_count"] = 999
    item_path.write_text(json.dumps(pitem))
    errors = validate_catalog(str(cat), deep=True)
    assert any(str(tif) in e and "nodata is -9999.0 but STAC declares -1.0" in e for e in errors)
    assert any(str(pq_path) in e and "table:row_count 999" in e for e in errors)


def test_sample_limits_file_checks(cat):
    tif = cat / "flood" / "v1-r1" / "flood_v1-r1_rp10.tif"
    _rewrite_same_size(tif)
    full = validate_catalog(str(cat), check_files=True)
    assert full == [f"{tif}: file:checksum does not match"]
    seen, warnings = set(), []
    for seed in range(30):
        warnings.clear()
        errors = validate_catalog(
            str(cat), check_files=True, sample=1, seed=seed, warnings=warnings
        )
        seen.add(bool(errors))
    assert seen == {True, False}  # sometimes the bad file is the one sampled
    assert any("sampled 1 of 6 assets" in w for w in warnings)


def test_datasets_filter(cat):
    tif = cat / "flood" / "v1-r1" / "flood_v1-r1_rp10.tif"
    _rewrite_same_size(tif)
    assert validate_catalog(str(cat), check_files=True, datasets=["points"]) == []
    assert validate_catalog(str(cat), check_files=True, datasets=["flood"]) != []


def test_cli_validate_deep(cat):
    runner = CliRunner()
    r = runner.invoke(main, ["validate", str(cat), "--deep"])
    assert r.exit_code == 0, r.output
    assert "valid (4 warnings)" in r.output
    r = runner.invoke(main, ["validate", str(cat), "--deep", "--strict"])
    assert r.exit_code == 1
    r = runner.invoke(main, ["validate", str(cat), "--deep", "--sample", "2", "--seed", "1"])
    assert r.exit_code == 0 and "sampled 2 of 6" in r.output


# -- STAC schemas -------------------------------------------------------------


def test_stac_schemas_are_bundled(catalogue, monkeypatch):
    """Core and every extension build_item/build_collection use resolve offline."""
    from irv_datapkg.catalog.producer import EXT

    bundled = qa._bundled()
    for uri in EXT.values():
        assert uri in bundled
    for kind in qa.STAC_CORE.values():
        assert kind.format(v="1.0.0") in bundled

    def offline(*args, **kwargs):
        raise AssertionError("tried the network")

    monkeypatch.setattr("requests.get", offline)
    base, _ = catalogue
    item = json.loads((base / "points" / "1.0-r1" / "points.json").read_text())
    coll = json.loads((base / "flood" / "collection.json").read_text())
    root = json.loads((base / "catalog.json").read_text())
    release = json.loads((base / "releases" / "2026.1" / "catalog.json").read_text())
    for doc in (item, coll, root, release):
        assert qa.stac_problems(doc) == ([], [])


def test_stac_core_errors():
    errors, _ = qa.stac_problems({"type": "Feature", "stac_version": "1.0.0", "id": "x"})
    assert errors and all(e.startswith("STAC 1.0.0 Feature") for e in errors)
    errors, _ = qa.stac_problems({"type": "Thing"})
    assert errors == ["not a STAC Catalog/Collection/Item (type='Thing')"]


def test_unreachable_extension_is_a_warning(catalogue, monkeypatch):
    def offline(*args, **kwargs):
        raise ConnectionError("proxy refused")

    monkeypatch.setattr("requests.get", offline)
    qa._remote_cache.clear()
    base, _ = catalogue
    item = json.loads((base / "points" / "1.0-r1" / "points.json").read_text())
    item["stac_extensions"].append("https://stac-extensions.github.io/eo/v9.9.9/schema.json")
    errors, warnings = qa.stac_problems(item)
    assert errors == []
    assert len(warnings) == 1 and "eo/v9.9.9" in warnings[0] and "proxy refused" in warnings[0]


# -- COG ----------------------------------------------------------------------


def test_cog_good(tmp_path):
    src = make_tif(tmp_path / "src.tif", nodata=-9999)
    cog = recipes.to_cog(src, tmp_path / "cog.tif", units="m")
    asset = {"raster:bands": [{"data_type": "float32", "nodata": -9999}]}
    item = {"properties": {"proj:shape": [600, 1100]}}
    assert qa.cog_problems(str(cog), asset, item) == ([], [])


def test_cog_bad_striped_uncompressed(tmp_path):
    tif = make_tif(tmp_path / "striped.tif")  # 1100x600, strips, no compression, no nodata
    errors, warnings = qa.cog_problems(str(tif), {"raster:bands": [{"nodata": 0}]})
    assert "not compressed (ADR 0004: ZSTD or DEFLATE)" in errors
    assert "no overviews (ADR 0004)" in errors
    assert any(e.startswith("not tiled") for e in errors)
    assert any(e.startswith("COG: ") for e in errors)  # rio-cogeo's own findings
    assert "band 1: nodata 0 declared but not set" in errors


def test_cog_nodata_and_dtype_mismatch(tmp_path):
    src = make_tif(tmp_path / "src.tif", width=64, height=32, nodata=np.nan)
    cog = recipes.to_cog(src, tmp_path / "cog.tif")
    nan_ok = {"raster:bands": [{"data_type": "float32", "nodata": float("nan")}]}
    assert qa.cog_problems(str(cog), nan_ok)[0] == []
    bad = {"raster:bands": [{"data_type": "uint8", "nodata": 0}, {"nodata": 1}]}
    errors, _ = qa.cog_problems(str(cog), bad, {"properties": {"proj:shape": [1, 1]}})
    assert "raster:bands lists 2 bands but the file has 1" in errors
    assert "band 1: nodata is nan but STAC declares 0" in errors
    assert "band 1: data type float32 but STAC declares uint8" in errors
    assert "proj:shape [1, 1] but the file is [32, 64]" in errors


def test_cog_without_declared_nodata_warns(tmp_path):
    src = make_tif(tmp_path / "src.tif", width=64, height=32)
    cog = recipes.to_cog(src, tmp_path / "cog.tif")
    with rasterio.open(cog) as ds:
        assert ds.nodata is None
    errors, warnings = qa.cog_problems(str(cog), {"raster:bands": [{"nodata": None}]})
    assert errors == []
    assert "band 1: no nodata (ADR 0004: explicit nodata)" in warnings


# -- GeoParquet -----------------------------------------------------------------


def test_geoparquet_good(tmp_path):
    path = recipes.write_geoparquet(make_gdf(), tmp_path / "good.parquet")
    assert qa.geoparquet_problems(str(path), {"table:row_count": 50}) == ([], [])


def test_geoparquet_plain_parquet(tmp_path):
    path = tmp_path / "plain.parquet"
    pq.write_table(pa.table({"a": [1, 2]}), path)
    assert qa.geoparquet_problems(str(path)) == (["no GeoParquet 'geo' metadata"], [])


def test_geoparquet_no_covering_wrong_crs(tmp_path):
    path = tmp_path / "bad.parquet"
    make_gdf(crs="EPSG:3857").to_parquet(path, schema_version="1.0.0")
    errors, warnings = qa.geoparquet_problems(str(path))
    assert "column geometry: no covering.bbox (ADR 0004: bbox covering column)" in errors
    assert "column geometry: crs is not EPSG:4326" in errors
    assert "GeoParquet 1.0.0 (ADR 0004: 1.1)" in warnings


def test_geoparquet_metadata_errors(tmp_path):
    path = recipes.write_geoparquet(make_gdf(), tmp_path / "good.parquet")
    table = pq.read_table(path)
    geo = json.loads(table.schema.metadata[b"geo"])
    geo["version"] = "0.4.0"
    geo["columns"]["geometry"]["geometry_types"] = ["Pointy"]
    geo["columns"]["geometry"]["crs"] = None
    geo["columns"]["geometry"]["covering"]["bbox"]["xmin"] = ["nope", "xmin"]
    meta = {**table.schema.metadata, b"geo": json.dumps(geo).encode()}
    bad = tmp_path / "bad.parquet"
    pq.write_table(table.replace_schema_metadata(meta), bad)
    errors, _ = qa.geoparquet_problems(str(bad), {"table:primary_geometry": "geom"})
    assert "GeoParquet version '0.4.0' (expected 1.1.0)" in errors
    assert "column geometry: unknown geometry_types ['Pointy']" in errors
    assert "column geometry: crs is null (unknown); ADR 0004 needs EPSG:4326" in errors
    assert "column geometry: covering.bbox.xmin ['nope', 'xmin'] is not a column" in errors
    assert "table:primary_geometry 'geom' but the file says 'geometry'" in errors


def test_geoparquet_row_groups(tmp_path):
    path = recipes.write_geoparquet(make_gdf(n=50), tmp_path / "small.parquet", row_group_size=10)
    _, warnings = qa.geoparquet_problems(str(path))
    assert warnings == ["4 of 5 row groups have under 10000 rows (ADR 0004: ~100000 per group)"]


# -- audit ------------------------------------------------------------------


def test_audit_finds_mismatch_and_missing(cat, tmp_path):
    tif = cat / "flood" / "v2-r1" / "flood_v2-r1_rp10.tif"
    _rewrite_same_size(tif)
    gone = cat / "flood" / "v2-r1" / "flood_v2-r1_rp100.tif"
    gone.unlink()
    results = qa.audit(qa.iter_assets(str(cat)))
    status = {r.href: r.status for r in results}
    assert len(results) == 6  # 4 flood + points (public) + tracks (private)
    assert status[str(tif)] == "mismatch"
    assert status[str(gone)] == "missing"
    assert sum(s == "ok" for s in status.values()) == 4
    assert any(r.access == "private" and r.status == "ok" for r in results)

    report = tmp_path / "audit.csv"
    r = CliRunner().invoke(main, ["audit", str(cat), "-o", str(report), "-j", "1"])
    assert r.exit_code == 1
    assert "MISMATCH" in r.output and "MISSING" in r.output
    assert "audited 6 of 6 assets: 1 mismatch, 1 missing, 4 ok" in r.output
    rows = list(csv.DictReader(open(report)))
    assert {row["status"] for row in rows} == {"ok", "missing", "mismatch"}


def test_audit_release_dataset_and_sample(cat):
    runner = CliRunner()
    r = runner.invoke(main, ["audit", str(cat), "--release", "2026.1"])
    # flood v1-r1 (2 files), points and secret_tracks (latest at release time)
    assert r.exit_code == 0 and "audited 4 of 4 assets: 4 ok" in r.output
    r = runner.invoke(main, ["audit", str(cat), "-d", "points", "-d", "secret_tracks"])
    assert r.exit_code == 0 and "audited 2 of 2" in r.output
    r = runner.invoke(main, ["audit", str(cat), "--sample", "3", "--seed", "4"])
    assert r.exit_code == 0 and "audited 3 of 6 assets: 3 ok" in r.output


def test_audit_retired_files(cat):
    from irv_datapkg.catalog import CatalogWriter

    CatalogWriter(cat).retire("flood", "v2-r1", "on tape", force=True)
    for tif in (cat / "flood" / "v2-r1").glob("*.tif"):
        tif.unlink()  # item JSON stays, files are on tape
    results = qa.audit(qa.iter_assets(str(cat), datasets=["flood"]))
    assert sorted(r.status for r in results) == ["ok", "ok", "retired", "retired"]
    r = CliRunner().invoke(main, ["audit", str(cat), "-d", "flood"])
    assert r.exit_code == 0 and "2 ok, 2 retired" in r.output


def test_audit_lock_file(cat, tmp_path):
    lock = tmp_path / "datapkg.lock.yaml"
    Catalog.open(str(cat), release="2026.1").write_lock(lock)
    r = CliRunner().invoke(main, ["audit", str(lock)])
    assert r.exit_code == 0 and "audited 4 of 4 assets: 4 ok" in r.output
    mirror = tmp_path / "mirror"
    shutil.copytree(cat, mirror)
    _rewrite_same_size(mirror / "flood" / "v1-r1" / "flood_v1-r1_rp10.tif")
    r = CliRunner().invoke(main, ["audit", str(lock), "--base", str(mirror)])
    assert r.exit_code == 1 and "1 mismatch" in r.output
