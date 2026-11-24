import shutil
from pathlib import Path

import pystac
import pytest
import rasterio
import yaml
from click.testing import CliRunner
from shapely.geometry import Polygon, box, mapping

from irv_datapkg.catalog import (
    Catalog,
    CatalogError,
    CatalogWriter,
    ChecksumError,
    ImmutableError,
    build_item,
    check,
    extract,
    validate_catalog,
)
from irv_datapkg.catalog import _fs
from irv_datapkg.cli import main

from conftest import COMMIT, make_cog


@pytest.fixture
def cat(catalogue, tmp_path):
    base, _ = catalogue
    return Catalog.open(f"file://{base}", cache_dir=str(tmp_path / "cache"))


def test_fixture_is_valid(catalogue):
    base, _ = catalogue
    assert validate_catalog(str(base), check_files=True) == []


def test_items_are_stac(catalogue):
    base, _ = catalogue
    for path in Path(base, "flood").glob("*/*.json"):
        item = pystac.Item.from_file(str(path))
        assert item.properties["irv:dataset"] == "flood"


def test_collections_include_private(cat):
    ids = [c["id"] for c in cat.collections()]
    assert ids == ["flood", "points", "secret_tracks"]
    assert cat.access("secret_tracks") == "private"
    assert cat.items("secret_tracks")[0].asset.href.startswith(cat.private_base)


def test_latest_and_release_versions(catalogue, cat):
    base, _ = catalogue
    assert cat.version("flood") == "v2-r1"
    rel = Catalog.open(str(base), release="2026.1")
    assert rel.version("flood") == "v1-r1"
    assert rel.datasets() == ["flood", "points", "secret_tracks"]
    assert {i.version for i in rel.items("flood")} == {"v1-r1"}
    with pytest.raises(CatalogError):
        Catalog.open(str(base), release="2099.1")


def test_property_filters(cat):
    assert len(cat.items("flood")) == 2
    assert [i.id for i in cat.items("flood", return_period=100)] == ["river_rcp45_rp100"]
    assert [i.id for i in cat.items("flood", return_period="100")] == ["river_rcp45_rp100"]
    assert len(cat.items("flood", return_period=[10, 100], hazard="river")) == 2
    assert cat.items("flood", hazard="coastal") == []
    assert len(cat.items("flood", version="v1-r1")) == 2


def test_item_metadata(cat):
    item = cat.item("flood", "river_rcp45_rp10")
    band = item.asset.fields["raster:bands"][0]
    assert band["nodata"] == -9999.0 and band["unit"] == "m"
    assert item.properties["proj:epsg"] == 4326
    assert item.asset.fields["irv:commit"] == COMMIT
    assert item.asset.size == Path(item.asset.href).stat().st_size


def test_href(cat):
    item = cat.item("flood", "river_rcp45_rp10")
    assert cat.href(item) == item.asset.href and cat.href(item).startswith("/")
    with rasterio.open(cat.href(item)) as ds:
        assert ds.shape == (10, 20)
    assert _fs.gdal_path("s3://bucket/k/a.tif") == "/vsis3/bucket/k/a.tif"
    assert _fs.gdal_path("https://h/x/a.tif") == "/vsicurl/https://h/x/a.tif"
    assert _fs.gdal_path("file:///tmp/a.tif") == "/tmp/a.tif"
    assert _fs.private_base("s3://bucket/irv/") == "s3://bucket/irv-private"


def test_fetch_verifies_and_caches(cat, tmp_path):
    asset = cat.item("flood", "river_rcp45_rp10").asset
    (tmp_path / "out").mkdir()
    out = cat.fetch(asset, tmp_path / "out")
    assert out == tmp_path / "out" / asset.filename
    cached = cat.fetch(asset)
    assert cached.read_bytes() == Path(asset.href).read_bytes()
    assert _fs.sha256_file(str(out)) == asset.sha256
    # a corrupted cache entry is re-downloaded
    cached.write_bytes(b"junk")
    assert _fs.sha256_file(str(cat.fetch(asset))) == asset.sha256
    # a checksum mismatch at the source is an error
    bad = type(asset)(
        **{
            **asset.__dict__,
            "version": "bad",
            "fields": {**asset.fields, "file:checksum": "1220" + "0" * 64},
        }
    )
    with pytest.raises(ChecksumError):
        cat.fetch(bad)


def test_private_prefix_missing(catalogue, tmp_path):
    base, _ = catalogue
    public_only = tmp_path / "pub"
    shutil.copytree(base, public_only)
    cat = Catalog.open(str(public_only))
    assert [c["id"] for c in cat.collections()] == ["flood", "points"]
    rel = Catalog.open(str(public_only), release="2026.1")
    with pytest.raises(CatalogError, match="not accessible"):
        rel.items("secret_tracks")


def test_lock_roundtrip(catalogue, tmp_path):
    base, _ = catalogue
    lock_path = Catalog.open(str(base), release="2026.1").write_lock(tmp_path / "datapkg.lock.yaml")
    lock = yaml.safe_load(lock_path.read_text())
    assert check(lock, "lock") == []
    assert lock["datasets"]["flood"]["version"] == "v1-r1"
    assert (
        lock["datasets"]["flood"]["items"]["river_rcp45_rp10"]["data"]["href"]
        == "flood/v1-r1/flood_v1-r1_rp10.tif"
    )
    locked = Catalog.from_lock(lock_path)
    assert len(locked.items("flood")) == 2
    lock["datasets"]["flood"]["items"]["river_rcp45_rp10"]["data"]["sha256"] = "0" * 64
    lock_path.write_text(yaml.safe_dump(lock))
    with pytest.raises(ChecksumError):
        Catalog.from_lock(lock_path).items("flood")


def test_extract_raster_bbox(cat, tmp_path):
    item = cat.item("flood", "river_rcp45_rp10")
    out = extract(item, tmp_path / "clip.tif", bbox=(0.05, 0.25, 0.55, 0.75))
    with rasterio.open(out) as ds:
        assert ds.shape == (6, 6)  # whole cells covering the bbox
        assert ds.bounds.left == pytest.approx(0.0) and ds.bounds.top == pytest.approx(0.8)
        assert ds.nodata == -9999.0 and ds.units == ("m",)
        assert (ds.read(1) == 1.0).all()


def test_extract_raster_polygon(cat, tmp_path):
    item = cat.item("flood", "river_rcp45_rp10")
    triangle = Polygon([(0.0, 0.0), (1.0, 0.0), (0.0, 1.0)])
    out = extract(item, tmp_path / "tri.tif", geometry=mapping(triangle))
    with rasterio.open(out) as ds:
        data = ds.read(1)
        assert ds.shape == (10, 10)
        assert data[9, 0] == 1.0 and data[0, 9] == -9999.0


def test_extract_vector(cat, tmp_path):
    import geopandas

    item = cat.item("points", "points")
    out = extract(item, tmp_path / "pts.parquet", bbox=(0.0, 0.0, 0.5, 1.0))
    assert list(geopandas.read_parquet(out)["name"]) == ["p0", "p1", "p2"]
    out = extract(item.asset.href, tmp_path / "pts2.parquet", geometry=box(1.0, 0.0, 1.3, 1.0))
    assert list(geopandas.read_parquet(out)["name"]) == ["p5", "p6"]


def test_immutability_and_releases(catalogue, tmp_path):
    base, _ = catalogue
    work = tmp_path / "w"
    shutil.copytree(base, work / "base")
    shutil.copytree(Path(str(base) + "-private"), work / "base-private")
    writer = CatalogWriter(work / "base")
    # same content again is a no-op
    item_json = Path(work, "base", "flood", "v1-r1", "river_rcp45_rp10.json")
    before = item_json.read_text()
    src = tmp_path / "flood_v1-r1_rp10.tif"
    shutil.copy(work / "base" / "flood" / "v1-r1" / "flood_v1-r1_rp10.tif", src)
    stac = build_item(
        src,
        "flood",
        "v1-r1",
        "river_rcp45_rp10",
        properties={"hazard": "river", "rcp": "4.5", "return_period": 10},
        source_url="https://example.org/flood/v1-r1/rp10.tif",
        retrieved="2026-01-15",
        commit=COMMIT,
    )
    writer.add_item(stac, src)
    assert item_json.read_text() == before
    # different content under a published name is refused
    make_cog(src, 5.0)
    changed = build_item(
        src,
        "flood",
        "v1-r1",
        "river_rcp45_rp10",
        source_url="x",
        retrieved="2026-01-15",
        commit=COMMIT,
    )
    with pytest.raises(ImmutableError):
        writer.add_item(changed, src)
    with pytest.raises(ImmutableError):
        writer.release("2026.1")
    # a released version is sealed: no new items can join it
    new_item = build_item(
        src,
        "flood",
        "v1-r1",
        "river_rcp45_rp1000",
        properties={"hazard": "river", "rcp": "4.5", "return_period": 1000},
        source_url="x",
        retrieved="2026-01-15",
        commit=COMMIT,
    )
    with pytest.raises(ImmutableError):
        writer.add_item(new_item, src)
    with pytest.raises(ValueError):
        writer.release("2026.2", pins={"flood": "v9-r1"})
    with pytest.raises(ValueError):
        build_item(src, "flood", "v1", "x", source_url="x", retrieved="2026-01-15", commit=COMMIT)
    # a new release from the previous one, bumping only flood
    writer.release("2026.2", from_release="2026.1", pins={"flood": "v2-r1"})
    rel = Catalog.open(str(work / "base"), release="2026.2")
    assert rel.version("flood") == "v2-r1" and rel.version("points") == "1.0-r1"
    assert writer.releases() == ["2026.1", "2026.2"]
    assert validate_catalog(str(work / "base")) == []


def test_validate_reports_problems(catalogue, tmp_path):
    base, _ = catalogue
    work = tmp_path / "bad"
    shutil.copytree(base, work)
    Path(work, "points", "1.0-r1", "points.parquet").write_bytes(b"x")
    errors = validate_catalog(str(work), private_base=str(base) + "-private", check_files=True)
    assert any("file:size" in e for e in errors) and any("file:checksum" in e for e in errors)
    assert check({"type": "Collection"}, "collection")


def test_cli(catalogue, tmp_path):
    base, _ = catalogue
    runner = CliRunner()
    cache = ["--cache-dir", str(tmp_path / "cache")]

    r = runner.invoke(main, ["validate", str(base), "--check-files"])
    assert r.exit_code == 0, r.output

    lock = tmp_path / "datapkg.lock.yaml"
    r = runner.invoke(
        main, ["lock", "--base", str(base), "--release", "2026.1", "flood", "-o", str(lock)]
    )
    assert r.exit_code == 0, r.output
    assert list(yaml.safe_load(lock.read_text())["datasets"]) == ["flood"]
    assert runner.invoke(main, ["validate", str(lock)]).exit_code == 0

    r = runner.invoke(
        main,
        [
            "fetch",
            "flood",
            "--lock",
            str(lock),
            "-p",
            "return_period=100",
            "-o",
            str(tmp_path / "f"),
        ]
        + cache,
    )
    assert r.exit_code == 0, r.output
    assert [p.name for p in (tmp_path / "f").iterdir()] == ["flood_v1-r1_rp100.tif"]

    r = runner.invoke(
        main,
        ["extract", "points", "--base", str(base), "--bbox", "0,0,0.5,1", "-o", str(tmp_path / "x")]
        + cache,
    )
    assert r.exit_code == 0, r.output
    assert (tmp_path / "x" / "points" / "1.0-r1" / "points.parquet").exists()

    work = tmp_path / "rel"
    shutil.copytree(base, work)
    r = runner.invoke(
        main,
        [
            "release",
            str(work),
            "2026.2",
            "--from",
            "2026.1",
            "--pin",
            "flood=v2-r1",
            "--private-base",
            str(base) + "-private",
        ],
    )
    assert r.exit_code == 0, r.output
    assert (
        Catalog.open(str(work), release="2026.2", private_base=str(base) + "-private").version(
            "flood"
        )
        == "v2-r1"
    )

    r = runner.invoke(main, ["fetch", "nope", "--base", str(base)] + cache)
    assert r.exit_code != 0


def test_any_fsspec_root(catalogue, tmp_path):
    import fsspec

    base, private = catalogue
    mem = fsspec.filesystem("memory")
    for src, dst in ((base, "/irv"), (private, "/irv-private")):
        for path in Path(src).rglob("*"):
            if path.is_file():
                mem.pipe(dst + "/" + str(path.relative_to(src)), path.read_bytes())
    cat = Catalog.open("memory://irv", release="2026.1", cache_dir=str(tmp_path / "cache"))
    assert cat.access("secret_tracks") == "private"
    item = cat.items("flood", return_period=10)[0]
    assert cat.href(item, gdal=False) == "memory://irv/flood/v1-r1/flood_v1-r1_rp10.tif"
    local = cat.fetch(item)
    assert _fs.sha256_file(str(local)) == item.asset.sha256
    assert validate_catalog("memory://irv", check_files=True) == []
