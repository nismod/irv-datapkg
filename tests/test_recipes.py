"""Recipe helpers (irv_datapkg.recipes) on tiny synthetic rasters and vectors."""

import json
import tarfile
import zipfile
from pathlib import Path

import geopandas
import numpy as np
import pyarrow.parquet as pq
import pytest
import rasterio
from click.testing import CliRunner
from rasterio.transform import from_origin
from shapely.geometry import LineString, Point, box

from irv_datapkg import recipes
from irv_datapkg.catalog import Catalog, check, validate_catalog
from irv_datapkg.cli import main
from irv_datapkg.recipes import T1Item

from conftest import COMMIT

REPO = Path(__file__).parent.parent


def make_tif(path, width=1100, height=600, dtype="float32", crs="EPSG:4326", nodata=None):
    """Plain striped (non-COG) GeoTIFF over lon -180..180, lat -90..90."""
    data = (np.arange(width * height).reshape(1, height, width) % 97).astype(dtype)
    profile = dict(
        driver="GTiff",
        width=width,
        height=height,
        count=1,
        dtype=dtype,
        transform=from_origin(-180, 90, 360 / width, 180 / height),
    )
    if crs:
        profile["crs"] = crs
    if nodata is not None:
        profile["nodata"] = nodata
    with rasterio.open(path, "w", **profile) as ds:
        ds.write(data)
    return path


def make_gdf(n=50, crs="EPSG:4326"):
    return geopandas.GeoDataFrame(
        {"name": [f"f{i}" for i in range(n)], "value": list(range(n))},
        geometry=[Point(-170 + 7 * i, -80 + 3 * i) for i in range(n)],
        crs="EPSG:4326",
    ).to_crs(crs)


#
# COG
#
def test_to_cog_keeps_grid_and_sets_metadata(tmp_path):
    src = make_tif(tmp_path / "src.tif")
    dst = recipes.to_cog(
        src, tmp_path / "out" / "cog.tif", nodata=-1, units="m", descriptions="depth"
    )
    assert recipes.is_cog(dst) == []
    with rasterio.open(src) as a, rasterio.open(dst) as b:
        assert a.transform == b.transform and a.crs == b.crs and a.shape == b.shape
        assert np.array_equal(a.read(), b.read())
        assert b.nodata == -1
        assert b.units == ("m",) and b.descriptions == ("depth",)
        assert b.tags(ns="IMAGE_STRUCTURE")["COMPRESSION"] == "ZSTD"
        assert b.block_shapes[0] == (512, 512)
        assert b.overviews(1)


def test_to_cog_deflate_assigns_crs_and_tags(tmp_path):
    src = make_tif(tmp_path / "src.tif", dtype="uint8", crs=None)
    dst = recipes.to_cog(
        src,
        tmp_path / "cog.tif",
        crs="EPSG:4326",
        nodata=0,
        compress="DEFLATE",
        tags={"source": "synthetic"},
    )
    with rasterio.open(dst) as ds:
        assert ds.crs.to_epsg() == 4326
        assert ds.tags()["source"] == "synthetic"
        assert ds.tags(ns="IMAGE_STRUCTURE")["COMPRESSION"] == "DEFLATE"
        assert ds.tags(ns="IMAGE_STRUCTURE")["PREDICTOR"] == "2"
    assert recipes.is_cog(dst) == []


def test_is_cog_flags_plain_tiff(tmp_path):
    problems = recipes.is_cog(make_tif(tmp_path / "plain.tif"))
    assert "not COG layout" in problems and "no nodata" in problems


def test_cog_from_zip_member(tmp_path):
    src = make_tif(tmp_path / "inner.tif", width=40, height=20, nodata=-9999)
    archive = tmp_path / "a.zip"
    with zipfile.ZipFile(archive, "w") as zf:
        zf.write(src, "nested/dir/inner.tif")
    path = recipes.vsizip(archive, basename="inner.tif")
    assert path.endswith("a.zip/nested/dir/inner.tif")
    dst = recipes.to_cog(path, tmp_path / "out.tif")
    with rasterio.open(dst) as ds:
        assert ds.nodata == -9999 and ds.shape == (20, 40)


def test_netcdf_to_cog(tmp_path):
    with rasterio.Env() as env:
        if "netCDF" not in env.drivers():
            pytest.skip("GDAL netCDF driver not available")
    src = make_tif(tmp_path / "src.tif", width=60, height=30, dtype="uint8")
    nc = tmp_path / "lc.nc"
    with rasterio.open(src) as ds:
        rasterio.shutil.copy(ds, nc, driver="netCDF")
    with rasterio.open(nc) as ds:
        variable = ds.subdatasets[0].rsplit(":", 1)[-1] if ds.subdatasets else "Band1"
    dst = recipes.netcdf_to_cog(nc, variable, tmp_path / "lc.tif", crs="EPSG:4326", nodata=0)
    with rasterio.open(src) as a, rasterio.open(dst) as b:
        assert b.shape == a.shape and b.nodata == 0 and b.crs.to_epsg() == 4326
        assert np.allclose(a.transform, b.transform)
        assert np.array_equal(a.read(), b.read())


def test_cog_options_and_gdal_args():
    opts = recipes.cog_options(compress="deflate", overview_resampling="average")
    assert opts["COMPRESS"] == "DEFLATE" and opts["RESAMPLING"] == "AVERAGE"
    assert opts["BLOCKSIZE"] == "512" and opts["PREDICTOR"] == "YES"
    assert "-co BLOCKSIZE=512" in recipes.gdal_translate_cog_args()
    with pytest.raises(ValueError):
        recipes.cog_options(compress="JPEG")


#
# GeoParquet
#
def _geo(path):
    return json.loads(pq.ParquetFile(path).metadata.metadata[b"geo"])


def test_to_geoparquet_from_gpkg_reprojects_and_covers(tmp_path):
    gpkg = tmp_path / "in.gpkg"
    make_gdf(crs="EPSG:3857").to_file(gpkg, layer="things", engine="pyogrio")
    out = recipes.to_geoparquet(gpkg, tmp_path / "out.parquet", layer="things")
    geo = _geo(out)
    assert geo["version"] == "1.1.0"
    assert "covering" in geo["columns"]["geometry"]
    gdf = geopandas.read_parquet(out)
    assert gdf.crs.to_epsg() == 4326 and len(gdf) == 50
    # Hilbert-sorted: row order follows hilbert_distance
    dist = gdf.geometry.hilbert_distance().to_numpy()
    assert (np.diff(dist) >= 0).all()
    # bbox filter on the covering column works (used by the client extract)
    sub = geopandas.read_parquet(out, bbox=(-180, -90, -150, -60))
    assert 0 < len(sub) < 50


def test_points_to_geoparquet_from_csv(tmp_path):
    csv = tmp_path / "plants.csv"
    csv.write_text("name,longitude,latitude\na,1.0,2.0\nb,-3.5,4.5\n")
    out = recipes.points_to_geoparquet(csv, tmp_path / "plants.parquet")
    gdf = geopandas.read_parquet(out)
    assert sorted(gdf.name) == ["a", "b"] and gdf.crs.to_epsg() == 4326


def test_ogr2ogr_geoparquet_cmd():
    cmd = recipes.ogr2ogr_geoparquet_cmd("in.pbf", "out dir/x.parquet", "lines")
    assert cmd.startswith("ogr2ogr -f Parquet")
    assert "WRITE_COVERING_BBOX=YES" in cmd and cmd.endswith("in.pbf lines")
    assert "'out dir/x.parquet'" in cmd


#
# Archives
#
def test_unzip_zip_and_tar(tmp_path):
    (tmp_path / "src").mkdir()
    for name in ("a.tif", "b.txt"):
        (tmp_path / "src" / name).write_text(name)
    zpath = tmp_path / "x.zip"
    with zipfile.ZipFile(zpath, "w") as zf:
        zf.write(tmp_path / "src" / "a.tif", "deep/a.tif")
        zf.write(tmp_path / "src" / "b.txt", "b.txt")
    out = recipes.unzip(zpath, tmp_path / "z")
    assert sorted(p.relative_to(tmp_path / "z").as_posix() for p in out) == ["b.txt", "deep/a.tif"]
    out = recipes.unzip(zpath, tmp_path / "flat", members=["a.tif"], flatten=True)
    assert [p.name for p in out] == ["a.tif"] and out[0].parent == tmp_path / "flat"
    tpath = tmp_path / "x.tgz"
    with tarfile.open(tpath, "w:gz") as tf:
        tf.add(tmp_path / "src" / "a.tif", "T1/DEM/T1_DEM.tif")
    out = recipes.unzip(tpath, tmp_path / "t", members=["T1_DEM.tif"], flatten=True)
    assert out == [tmp_path / "t" / "T1_DEM.tif"]
    assert recipes.archive_member(zpath, "a.tif") == "deep/a.tif"
    with pytest.raises(FileNotFoundError):
        recipes.unzip(zpath, tmp_path / "m", members=["missing.tif"])


def test_unzip_refuses_path_traversal(tmp_path):
    zpath = tmp_path / "evil.zip"
    with zipfile.ZipFile(zpath, "w") as zf:
        zf.writestr("../escape.txt", "x")
    with pytest.raises(ValueError):
        recipes.unzip(zpath, tmp_path / "out")
    assert not (tmp_path / "escape.txt").exists()


#
# Provenance
#
def test_provenance_roundtrip(tmp_path):
    raw = tmp_path / "raw"
    raw.mkdir()
    (raw / "a.tif").write_bytes(b"aaaa")
    (raw / "sub").mkdir()
    (raw / "sub" / "b.tif").write_bytes(b"bb")
    sidecar = recipes.write_provenance(
        raw / "provenance.json",
        [raw / "a.tif", raw / "sub" / "b.tif"],
        tool="aws s3 sync",
        url_prefix="s3://bucket/v2/",
        retrieved="2026-01-02",
    )
    doc = recipes.read_provenance(sidecar)
    assert doc["retrieved"] == "2026-01-02"
    assert [f["source_url"] for f in doc["files"]] == [
        "s3://bucket/v2/a.tif",
        "s3://bucket/v2/sub/b.tif",
    ]
    _, entry, _ = recipes.find_provenance(raw / "sub" / "b.tif")
    assert entry["size"] == 2 and len(entry["sha256"]) == 64
    assert recipes.verify_provenance(sidecar) == []
    (raw / "a.tif").write_bytes(b"changed")
    assert recipes.verify_provenance(sidecar) == ["a.tif"]
    # single-file sidecars take precedence
    recipes.write_provenance(
        recipes.sidecar_for(raw / "a.tif"),
        [raw / "a.tif"],
        tool="wget",
        source_url="https://x/a.tif",
    )
    assert recipes.find_provenance(raw / "a.tif")[1]["source_url"] == "https://x/a.tif"
    with pytest.raises(FileNotFoundError):
        recipes.find_provenance(tmp_path / "elsewhere.tif")


def test_provenance_cli(tmp_path):
    (tmp_path / "t1.tar").write_bytes(b"1")
    (tmp_path / "t2.tar").write_bytes(b"22")
    (tmp_path / "urls.txt").write_text("https://h/a/t1.tar?x=1\nhttps://h/b/t2.tar\n")
    result = CliRunner().invoke(
        main,
        [
            "provenance",
            "-o",
            str(tmp_path / "provenance.json"),
            "--tool",
            "definitely-not-a-tool --flag",
            "--urls-from",
            str(tmp_path / "urls.txt"),
            str(tmp_path / "t1.tar"),
            str(tmp_path / "t2.tar"),
        ],
    )
    assert result.exit_code == 0, result.output
    doc = recipes.read_provenance(tmp_path / "provenance.json")
    assert [f["source_url"] for f in doc["files"]] == [
        "https://h/a/t1.tar?x=1",
        "https://h/b/t2.tar",
    ]
    assert doc["tool"].startswith("definitely-not-a-tool")


def test_to_cog_cli(tmp_path):
    src = make_tif(tmp_path / "src.tif", width=30, height=20)
    result = CliRunner().invoke(
        main, ["to-cog", str(src), str(tmp_path / "c.tif"), "--nodata", "-5"]
    )
    assert result.exit_code == 0, result.output
    with rasterio.open(tmp_path / "c.tif") as ds:
        assert ds.nodata == -5


#
# Metadata
#
def test_repository_metadata_is_valid():
    assert recipes.validate_metadata(REPO / "metadata", REPO / "docs" / "datasets.csv") == []
    packages = recipes.load_metadata(REPO / "metadata")
    colls = recipes.collections_by_id(packages)
    assert {"aqueduct_flood", "storm_fixed_rp", "ghsl_pop", "ghsl_built_s"} <= set(colls)
    assert packages["jrc_ghsl"].in_t2 and not packages["gem_earthquake"].in_t2
    for coll in colls.values():
        assert check(coll.stac(), "collection") == [], coll.id


#
# Registration and T2 extracts (synthetic dataset end to end)
#
def _collection(access="public", dims=("hazard", "return_period")):
    return recipes.CollectionMeta(
        id="synthetic_flood",
        version="v1-r1",
        package="synthetic",
        title="Synthetic flood",
        description="Tiny synthetic data",
        license="CC-BY-4.0",
        citation="Nobody (2026)",
        upstream_url="https://example.org/flood",
        upstream_version="v1",
        access=access,
        processing=["convert to COG"],
        dimensions=list(dims),
        units="m",
        license_url="https://creativecommons.org/licenses/by/4.0/",
    )


@pytest.fixture
def workspace(tmp_path):
    """A raw mirror (with provenance) and a local catalogue holding 2 rasters and 1 vector."""
    raw = tmp_path / "raw"
    raw.mkdir()
    sources = []
    for rp in (10, 100):
        src = make_tif(raw / f"flood_rp{rp}.tif", width=720, height=360, nodata=-9999)
        sources.append(src)
    recipes.write_provenance(
        raw / "provenance.json", sources, tool="test", url_prefix="https://example.org/flood"
    )
    base = tmp_path / "catalogue"
    coll = _collection()
    for rp, src in zip((10, 100), sources):
        spec = T1Item("tif", {"hazard": "river", "return_period": rp}, sources=[str(src)])
        recipes.to_cog(src, recipes.item_data_path(base, coll, f"river_rp{rp}", "tif"))
        recipes.register_item(base, coll, f"river_rp{rp}", spec, commit=COMMIT)
    lines = _collection(dims=("layer",))
    lines.id = "synthetic_lines"
    gdf = geopandas.GeoDataFrame(
        {"kind": ["a", "b", "c"]},
        geometry=[LineString([(0, 0), (1, 1)]), LineString([(50, 10), (51, 11)]), Point(0.5, 0.2)],
        crs="EPSG:4326",
    )
    (raw / "lines.gpkg").write_bytes(b"placeholder")
    recipes.write_provenance(
        recipes.sidecar_for(raw / "lines.gpkg"),
        [raw / "lines.gpkg"],
        tool="test",
        source_url="https://example.org/lines.gpkg",
    )
    for layer in ("lines", "points"):
        spec = T1Item(
            "parquet", {"layer": layer, "sector": "demo"}, sources=[str(raw / "lines.gpkg")]
        )
        recipes.write_geoparquet(
            gdf, recipes.item_data_path(base, lines, f"demo_{layer}", "parquet")
        )
        recipes.register_item(base, lines, f"demo_{layer}", spec, commit=COMMIT)
    return base, coll, lines


def test_register_items_make_a_valid_catalogue(workspace):
    base, coll, _ = workspace
    assert validate_catalog(str(base), check_files=True) == []
    cat = Catalog.open(str(base))
    assert cat.datasets() == ["synthetic_flood", "synthetic_lines"]
    item = cat.items("synthetic_flood", return_period=100)[0]
    assert item.properties["hazard"] == "river"
    asset = item.asset.fields
    assert asset["irv:source_url"] == "https://example.org/flood/flood_rp100.tif"
    assert asset["irv:commit"] == COMMIT
    assert asset["raster:bands"][0]["unit"] == "m"
    links = cat.collection("synthetic_flood")["links"]
    assert any(lk["rel"] == "license" for lk in links)
    # re-registering identical content is a no-op
    spec = T1Item(
        "tif",
        {"hazard": "river", "return_period": 10},
        sources=[str(Path(base).parent / "raw" / "flood_rp10.tif")],
    )
    recipes.register_item(base, coll, "river_rp10", spec, commit=COMMIT)


def test_register_requires_dimensions(workspace):
    base, coll, _ = workspace
    with pytest.raises(ValueError, match="dimension"):
        recipes.register_item(base, coll, "river_rp10", T1Item("tif", {"hazard": "river"}))


def test_private_collection_paths(tmp_path):
    coll = _collection(access="private")
    assert (
        recipes.item_dir(tmp_path / "cat", coll) == f"{tmp_path}/cat-private/synthetic_flood/v1-r1"
    )


def test_t2_plan_selection_and_names():
    items = [
        ("c", "inunriver_a", T1Item("tif", {"hazard": "river", "upstream_name": "inunriver_A"})),
        ("c", "inuncoast_nosub", T1Item("tif", {"hazard": "coastal", "upstream_name": "x_nosub"})),
        ("v", "gbr_adm1", T1Item("parquet", {"iso3": "GBR", "adm_level": "ADM1"})),
        ("v", "fra_adm1", T1Item("parquet", {"iso3": "FRA", "adm_level": "ADM1"})),
    ]
    t2 = dict(recipes.metadata.T2_DEFAULTS, stem="{upstream_name}", exclude_patterns=["nosub"])
    plan = recipes.plan("pkg", t2, items[:2], "JAM", "JAM")
    assert plan == {"pkg/inunriver_A__JAM.tif": [("c", "inunriver_a", "inunriver_A")]}
    t2 = dict(
        recipes.metadata.T2_DEFAULTS,
        stem="geoBoundaries-{iso3}-{adm_level}",
        filter={"iso3": "{ADM0_A3}"},
    )
    plan = recipes.plan("geob", t2, items[2:], "GBX", "GBR")
    assert list(plan) == ["geob/geoBoundaries-GBR-ADM1__GBX.gpkg"]
    with pytest.raises(KeyError):
        recipes.plan("p", dict(t2, stem="{nope}", filter={}), items[2:], "GBR", "GBR")
    with pytest.raises(ValueError, match="only GeoPackages"):
        recipes.plan("p", dict(recipes.metadata.T2_DEFAULTS, stem="same"), items[:2], "JAM", "JAM")


def test_extract_region_raster_and_layers(workspace, tmp_path):
    base, coll, lines = workspace
    region = box(-10, -5, 10, 5)
    item_json = recipes.item_json_path(base, coll, "river_rp10")
    out = recipes.extract_region(
        [item_json], tmp_path / "t2" / "pkg" / "x__AAA.tif", geometry=region
    )
    with rasterio.open(out) as ds:
        assert ds.width == 40 and ds.height == 20  # 0.5 degree cells
        assert ds.nodata == -9999
    jsons = [recipes.item_json_path(base, lines, f"demo_{layer}") for layer in ("lines", "points")]
    gpkg = recipes.extract_region(
        jsons,
        tmp_path / "t2" / "pkg" / "demo__AAA.gpkg",
        geometry=region,
        clip="geometry",
        layers=["lines", "points"],
    )
    import pyogrio

    assert sorted(name for name, _ in pyogrio.list_layers(gpkg)) == ["lines", "points"]
    gdf = geopandas.read_file(gpkg, layer="lines")
    assert len(gdf) == 2 and "bbox" not in gdf.columns  # the feature at lon 50 is outside
    whole = recipes.extract_region(jsons[:1], tmp_path / "t2" / "all.parquet", clip="none")
    assert len(geopandas.read_parquet(whole)) == 3


def test_write_summary(tmp_path):
    entries = [
        ("storm/b__JAM.tif", None, {"hazard": "cyclone", "epoch": "baseline", "return_period": 10}),
        ("storm/a__JAM.tif", None, {"hazard": "cyclone", "epoch": 2050, "return_period": 100}),
    ]
    out = recipes.write_summary(
        tmp_path / "s.csv", entries, {"epoch": "epoch", "rp": "return_period"}, 2010
    )
    assert out.read_text().splitlines() == [
        "epoch,rp,path",
        "2010,10,storm/b__JAM.tif",
        "2050,100,storm/a__JAM.tif",
    ]


def test_register_refuses_empty_layer(workspace):
    base, _, lines = workspace
    empty = geopandas.GeoDataFrame({"kind": []}, geometry=geopandas.GeoSeries([], crs=4326))
    path = recipes.write_geoparquet(
        empty, recipes.item_data_path(base, lines, "demo_empty", "parquet")
    )
    raw = Path(base).parent / "raw" / "lines.gpkg"
    spec = T1Item("parquet", {"layer": "empty"}, sources=[str(raw)])
    with pytest.raises(ValueError, match="empty"):
        recipes.register_item(base, lines, "demo_empty", spec, data_path=path, commit=COMMIT)
