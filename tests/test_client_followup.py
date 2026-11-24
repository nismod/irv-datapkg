"""Catalogue client follow-up (WP1b): mirrors, portable locks, fetch modes,
locked fetches, direct item reads, filters, selective locks, stale-lock checks,
provenance and retirement. Synthetic fixtures only."""

import json
import os
import shutil
import stat
import uuid
from pathlib import Path

import fsspec
import pytest
import yaml
from click.testing import CliRunner

from irv_datapkg import publish as pub
from irv_datapkg.catalog import (
    ABSENT,
    Catalog,
    CatalogError,
    CatalogWriter,
    ChecksumError,
    ImmutableError,
    LockFile,
    RetiredError,
    build_item,
    check,
    validate_catalog,
)
from irv_datapkg.catalog import _fs
from irv_datapkg.cli import main

from conftest import COMMIT, flood_item


@pytest.fixture
def work(catalogue, tmp_path):
    """A writable copy of the fixture catalogue: (base, private)."""
    base, private = catalogue
    dest = tmp_path / "cat" / "base"
    shutil.copytree(base, dest)
    shutil.copytree(private, tmp_path / "cat" / "base-private")
    return dest, tmp_path / "cat" / "base-private"


def data_only_mirror(base, dest):
    """Copy only the data files (no STAC JSON) of a catalogue root."""
    for path in Path(base).rglob("*"):
        if path.is_file() and path.suffix != ".json":
            target = dest / path.relative_to(base)
            target.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy(path, target)
    return dest


# -- 1. mirrors and portable lock files ---------------------------------------


def test_lock_relative_base_and_mirror(work, tmp_path, monkeypatch):
    base, private = work
    monkeypatch.chdir(tmp_path)
    locks = tmp_path / "proj"
    locks.mkdir()
    lock_path = Catalog.open("cat/base", release="2026.1").write_lock(locks / "datapkg.lock.yaml")
    lock = yaml.safe_load(lock_path.read_text())
    assert check(lock, "lock") == []
    assert lock["base"] == "../cat/base"  # relative to the lock file, not the cwd
    assert "private_base" not in lock  # default {base}-private is not recorded
    entry = lock["datasets"]["secret_tracks"]["items"]["tracks"]["data"]
    assert entry["href"] == "secret_tracks/early-r1/tracks.parquet"  # relative to private base

    monkeypatch.chdir(locks)
    cat = Catalog.from_lock("datapkg.lock.yaml", cache_dir=str(tmp_path / "cache"))
    assert cat.base == str(base) and cat.private_base == str(private)
    assert len(cat.items("flood")) == 2
    # re-writing from a lock-opened catalogue keeps the base relative
    cat.write_lock(locks / "again.yaml")
    assert yaml.safe_load((locks / "again.yaml").read_text())["base"] == "../cat/base"

    # a mirror: same tree under another base; private follows the override
    mirror = tmp_path / "mirror" / "irv"
    shutil.copytree(base, mirror)
    shutil.copytree(private, tmp_path / "mirror" / "irv-private")
    m = Catalog.from_lock(lock_path, base=str(mirror), cache_dir=str(tmp_path / "cache"))
    assert m.private_base == str(mirror) + "-private"
    assert m.item("flood", "river_rcp45_rp10").asset.href.startswith(str(mirror) + "/")
    assert m.items("secret_tracks")[0].asset.href.startswith(str(mirror) + "-private/")
    explicit = Catalog.from_lock(lock_path, base=str(mirror), private_base=str(private))
    assert explicit.private_base == str(private)


def test_lock_file_url_kept_as_given(work, tmp_path):
    base, _ = work
    lock_path = Catalog.open(f"file://{base}", release="2026.1").write_lock(tmp_path / "l.yaml")
    assert yaml.safe_load(lock_path.read_text())["base"] == f"file://{base}"
    assert Catalog.from_lock(lock_path).base == str(base)
    # --record-base style: record the public URL while locking from a local copy
    lock = Catalog.open(str(base), release="2026.1").lock(base="https://example.org/irv")
    assert lock["base"] == "https://example.org/irv"


def test_old_style_lock_still_reads(work, tmp_path):
    """Locks written before this change: absolute base and private_base, no properties."""
    base, private = work
    lock = Catalog.open(str(base), release="2026.1").lock()
    lock["base"], lock["private_base"] = str(base), str(private)
    for d in lock["datasets"].values():
        d.pop("properties", None)
        for assets in d["items"].values():
            for entry in assets.values():
                entry.pop("source_filename", None)
    path = tmp_path / "old.yaml"
    path.write_text(yaml.safe_dump(lock))
    cat = Catalog.from_lock(path, cache_dir=str(tmp_path / "cache"))
    assert [i.id for i in cat.items("flood", return_period=100)] == ["river_rcp45_rp100"]
    # CLI falls back to the catalogue when the lock cannot filter by itself
    r = CliRunner().invoke(
        main,
        [
            "fetch",
            "flood",
            "--lock",
            str(path),
            "-p",
            "return_period=100",
            "-o",
            str(tmp_path / "o"),
        ]
        + ["--cache-dir", str(tmp_path / "cache")],
    )
    assert r.exit_code == 0, r.output
    assert [p.name for p in (tmp_path / "o").iterdir()] == ["flood_v1-r1_rp100.tif"]


# -- 2. fetch modes -----------------------------------------------------------


def test_fetch_link_modes(catalogue, tmp_path):
    base, _ = catalogue
    cat = Catalog.open(str(base), cache_dir=str(tmp_path / "cache"))
    item = cat.item("flood", "river_rcp45_rp10")
    cached = cat.fetch(item)
    assert cached == cat.cache_path(item)
    assert not os.stat(cached).st_mode & stat.S_IWUSR  # cache is read-only

    hard = cat.fetch(item, tmp_path / "hard.tif", link="hardlink")
    assert os.path.samefile(hard, cached)
    assert cat.fetch(item, hard, link="hardlink") == hard  # idempotent

    sym = cat.fetch(item, tmp_path / "sym.tif", link="symlink")
    assert sym.is_symlink() and Path(os.readlink(sym)) == cached

    copy = cat.fetch(item, tmp_path / "copy.tif")
    assert not os.path.samefile(copy, cached) and copy.read_bytes() == cached.read_bytes()
    assert os.stat(copy).st_mode & stat.S_IWUSR  # a copy is writable

    # replacing an existing destination is atomic and leaves no temp files
    copy2 = cat.fetch(item, hard, link="copy")
    assert not os.path.samefile(copy2, cached)
    assert sorted(p.name for p in tmp_path.iterdir() if p.name.startswith(".")) == []

    with pytest.raises(ValueError):
        cat.fetch(item, tmp_path / "x.tif", link="move")


def test_fetch_without_cache(catalogue, tmp_path):
    base, _ = catalogue
    cat = Catalog.open(str(base), cache_dir=str(tmp_path / "cache"))
    item = cat.item("flood", "river_rcp45_rp100")
    out = cat.fetch(item, tmp_path / "direct.tif", cache=False)
    assert _fs.sha256_file(str(out)) == item.asset.sha256
    assert not (tmp_path / "cache").exists()
    with pytest.raises(ValueError):
        cat.fetch(item, cache=False)
    # a bad checksum leaves neither the destination nor a partial file
    bad = type(item.asset)(
        **{
            **item.asset.__dict__,
            "fields": {**item.asset.fields, "file:checksum": "1220" + "0" * 64},
        }
    )
    with pytest.raises(ChecksumError):
        cat.fetch(bad, tmp_path / "bad.tif", cache=False)
    assert not (tmp_path / "bad.tif").exists()
    assert not list(tmp_path.glob("*.part"))


# -- 3. fetching from lock entries alone ----------------------------------------


def test_fetch_locked_without_stac(work, tmp_path):
    base, private = work
    lock_path = Catalog.open(str(base), release="2026.1").write_lock(tmp_path / "l.yaml")
    # the mirror has data files only: any STAC read would fail
    mirror = data_only_mirror(base, tmp_path / "m" / "irv")
    data_only_mirror(private, tmp_path / "m" / "irv-private")
    cat = Catalog.from_lock(lock_path, base=str(mirror), cache_dir=str(tmp_path / "cache"))
    out = cat.fetch_locked("flood", "river_rcp45_rp10", tmp_path / "a.tif", link="hardlink")
    assert _fs.sha256_file(str(out)) == cat.lockfile.entry("flood", "river_rcp45_rp10")[1]["sha256"]
    assert cat.fetch_locked("secret_tracks", "tracks").name == "tracks.parquet"
    asset = cat.locked_asset("flood", "river_rcp45_rp100")
    assert asset.source_filename == "rp100.tif" and asset.version == "v1-r1"
    assert cat.lockfile.select("flood", return_period=100) == ["river_rcp45_rp100"]
    assert cat.lockfile.select("flood", ["river_rcp45_rp10"]) == ["river_rcp45_rp10"]
    with pytest.raises(CatalogError):
        cat.lockfile.select("flood", epoch=2050)  # not recorded
    with pytest.raises(CatalogError):
        cat.fetch_locked("flood", "nope")
    with pytest.raises(CatalogError):
        Catalog.open(str(base)).fetch_locked("flood", "river_rcp45_rp10")
    # a tampered entry is caught while streaming
    lock = yaml.safe_load(lock_path.read_text())
    lock["datasets"]["flood"]["items"]["river_rcp45_rp10"]["data"]["sha256"] = "0" * 64
    lock_path.write_text(yaml.safe_dump(lock))
    tampered = Catalog.from_lock(lock_path, base=str(mirror), cache_dir=str(tmp_path / "c2"))
    with pytest.raises(ChecksumError):
        tampered.fetch_locked("flood", "river_rcp45_rp10", tmp_path / "t.tif", cache=False)


def test_cli_fetch_lock_uses_lock_only(work, tmp_path):
    base, _ = work
    lock = tmp_path / "datapkg.lock.yaml"
    runner = CliRunner()
    r = runner.invoke(
        main,
        ["lock", "--base", str(base), "--release", "2026.1", "-p", "flood:return_period=100"]
        + ["-o", str(lock)],
    )
    assert r.exit_code == 0, r.output
    mirror = data_only_mirror(base, tmp_path / "m" / "irv")
    r = runner.invoke(
        main,
        ["fetch", "flood", "--lock", str(lock), "--base", str(mirror), "-o", str(tmp_path / "o")]
        + ["--link", "symlink", "--cache-dir", str(tmp_path / "cache")],
    )
    assert r.exit_code == 0, r.output
    (out,) = (tmp_path / "o").iterdir()
    assert out.name == "flood_v1-r1_rp100.tif" and out.is_symlink()
    r = runner.invoke(
        main,
        ["fetch", "flood", "--lock", str(lock), "--base", str(mirror), "--no-cache"]
        + [
            "-p",
            "return_period=100",
            "-o",
            str(tmp_path / "n"),
            "--cache-dir",
            str(tmp_path / "c"),
        ],
    )
    assert r.exit_code == 0, r.output
    assert not (tmp_path / "c").exists() and len(list((tmp_path / "n").iterdir())) == 1
    r = runner.invoke(main, ["fetch", "flood", "--lock", str(lock), "-p", "return_period=10"])
    assert r.exit_code != 0 and "No matching locked items" in r.output


# -- 4. direct item reads -------------------------------------------------------


def test_item_reads_directly(catalogue, monkeypatch):
    base, _ = catalogue
    cat = Catalog.open(str(base))
    cat.collection("flood")
    reads = []
    real = _fs.read_json
    monkeypatch.setattr(_fs, "read_json", lambda url, so=None: reads.append(url) or real(url, so))
    item = cat.item("flood", "river_rcp45_rp10", version="v1-r1")
    assert item.version == "v1-r1"
    assert reads == [str(base) + "/flood/v1-r1/river_rcp45_rp10.json"]
    with pytest.raises(CatalogError):
        cat.item("flood", "../collection")


def test_item_falls_back_to_scan(work):
    base, _ = work
    vdir = base / "flood" / "v2-r1"
    (vdir / "river_rcp45_rp10.json").rename(vdir / "renamed.json")
    coll_path = base / "flood" / "collection.json"
    coll = json.loads(coll_path.read_text())
    for link in coll["links"]:
        if link["href"] == "./v2-r1/river_rcp45_rp10.json":
            link["href"] = "./v2-r1/renamed.json"
    coll_path.write_text(json.dumps(coll))
    cat = Catalog.open(str(base))
    assert cat.item("flood", "river_rcp45_rp10").url.endswith("renamed.json")
    with pytest.raises(CatalogError, match="not found"):
        cat.item("flood", "river_rcp45_rp9999")


# -- 5. filters -------------------------------------------------------------------


def test_filters_strict_and_absent(catalogue):
    base, _ = catalogue
    cat = Catalog.open(str(base))
    assert len(cat.items("flood", epoch=Catalog.ABSENT)) == 2
    assert len(cat.items("flood", epoch=None)) == 2
    assert Catalog.ABSENT is ABSENT and repr(ABSENT) == "Catalog.ABSENT"
    assert cat.items("flood", rcp=None) == []
    assert len(cat.items("flood", rcp=[None, "4.5"])) == 2
    assert len(cat.items("flood", rcp=4.5)) == 2  # existing string comparison
    assert len(cat.items("flood", return_period=[10, 100], strict=True)) == 2
    with pytest.raises(CatalogError, match="not dimensions"):
        cat.items("flood", epoch=2050, strict=True)
    assert cat.items("flood", epoch=2050) == []


# -- 6. selective locks -----------------------------------------------------------


def test_lock_selection(catalogue, tmp_path):
    base, _ = catalogue
    cat = Catalog.open(str(base), release="2026.1")
    lock = cat.lock(select={"flood": {"return_period": 100}})
    assert list(lock["datasets"]) == ["flood"]
    assert list(lock["datasets"]["flood"]["items"]) == ["river_rcp45_rp100"]
    assert lock["datasets"]["flood"]["properties"]["river_rcp45_rp100"] == {
        "hazard": "river",
        "rcp": "4.5",
        "return_period": 100,
    }
    assert check(lock, "lock") == []
    union = cat.lock(["points"], select={"flood": ["river_rcp45_rp10", {"return_period": [100]}]})
    assert list(union["datasets"]) == ["points", "flood"]
    assert sorted(union["datasets"]["flood"]["items"]) == ["river_rcp45_rp10", "river_rcp45_rp100"]
    assert "properties" not in union["datasets"]["points"]  # no dimensions
    with pytest.raises(CatalogError):
        cat.lock(select={"flood": {"return_period": 5}})
    with pytest.raises(CatalogError):
        cat.lock(select={"flood": "nope"})

    # a lock of a subset: other items are simply not in the view
    path = cat.write_lock(tmp_path / "l.yaml", select={"flood": "river_rcp45_rp10"})
    locked = Catalog.from_lock(path)
    assert [i.id for i in locked.items("flood")] == ["river_rcp45_rp10"]
    assert locked.items("flood", return_period=100) == []
    with pytest.raises(CatalogError):
        locked.items("flood", version="v2-r1")
    with pytest.raises(CatalogError):
        locked.item("flood", "river_rcp45_rp100")


# -- 7. stale-lock checks ---------------------------------------------------------


def test_lock_matches(work, tmp_path):
    base, _ = work
    cat = Catalog.open(str(base), release="2026.1")
    path = cat.write_lock(tmp_path / "l.yaml", select={"flood": {"return_period": 10}})
    assert cat.lock_matches(path)
    assert cat.lock_matches(path, datasets=["flood"])
    diff = cat.lock_matches(path, datasets=["flood", "points"])
    assert not diff and "points" in str(diff)
    assert cat.lock_matches(path, select={"flood": {"return_period": 10}})
    diff = cat.lock_matches(path, select={"flood": {"return_period": [10, 100]}})
    assert not diff and "river_rcp45_rp100 is not locked" in diff.problems[0]
    assert not cat.lock_matches(tmp_path / "missing.yaml")

    CatalogWriter(base).release("2026.2", from_release="2026.1", pins={"flood": "v2-r1"})
    diff = cat.lock_matches(path, release="2026.2")
    assert not diff and diff.problems == ["release 2026.1 != 2026.2"]
    # same release name, lock edited to another version
    lock = yaml.safe_load(path.read_text())
    lock["datasets"]["flood"]["version"] = "v2-r1"
    path.write_text(yaml.safe_dump(lock))
    diff = Catalog.from_lock(path).lock_matches(path)  # checks against the manifest
    assert not diff and "release pins v1-r1" in str(diff)

    runner = CliRunner()
    lock_out = tmp_path / "cli.yaml"
    args = ["lock", "--base", str(base), "--release", "2026.2", "flood", "-o", str(lock_out)]
    assert runner.invoke(main, args + ["--check"]).exit_code == 1
    assert runner.invoke(main, args).exit_code == 0
    r = runner.invoke(main, args + ["--check"])
    assert r.exit_code == 0 and "up to date" in r.output


# -- 8. provenance ------------------------------------------------------------------


def test_source_provenance(catalogue, tmp_path):
    base, _ = catalogue
    asset = Catalog.open(str(base)).item("flood", "river_rcp45_rp10").asset
    assert asset.source_url == "https://example.org/flood/v2-r1/rp10.tif"
    assert asset.fields["irv:source_filename"] == "rp10.tif" == asset.source_filename
    # points' source is a zip: still a file name
    points = Catalog.open(str(base)).item("points", "points").asset
    assert points.source_filename == "points.zip"

    assert _fs.source_filename("https://h/a/b%20c.tif?x=1") == "b c.tif"
    assert _fs.source_filename("s3://bucket/prefix/") is None
    assert _fs.source_filename("https://h/dataset") is None
    assert _fs.source_filename("internal") is None

    src = tmp_path / "f.tif"
    shutil.copy(asset.href, src)
    kw = dict(retrieved="2026-01-15", commit=COMMIT)
    item = build_item(src, "flood", "v9-r1", "x", source_url="internal", **kw)
    assert "irv:source_filename" not in item["assets"]["data"]
    item = build_item(
        src, "flood", "v9-r1", "x", source_url="internal", source_filename="up.tif", **kw
    )
    assert item["assets"]["data"]["irv:source_filename"] == "up.tif"
    assert check(item, "item") == []
    with pytest.raises(ValueError):
        build_item(src, "flood", "v9-r1", "x", source_url="x", source_filename="a/b.tif", **kw)
    # older items without the field: derived from irv:source_url
    legacy = type(asset)(**{**asset.__dict__, "fields": {"irv:source_url": "https://h/up.tif"}})
    assert legacy.source_filename == "up.tif"


# -- 9. relative hrefs --------------------------------------------------------------


def test_relative_href(catalogue):
    base, _ = catalogue
    cat = Catalog.open(str(base))
    assert cat.relative_href(cat.item("flood", "river_rcp45_rp10")) == (
        "flood/v2-r1/flood_v2-r1_rp10.tif"
    )
    assert (
        cat.relative_href(cat.items("secret_tracks")[0]) == "secret_tracks/early-r1/tracks.parquet"
    )
    stray = type(cat.items("points")[0].asset)(
        "data", "/elsewhere/x.tif", "points", "v", "i", "public"
    )
    with pytest.raises(CatalogError):
        cat.relative_href(stray)


# -- 10. retirement -----------------------------------------------------------------


def test_retirement(work, tmp_path):
    base, private = work
    writer = CatalogWriter(base)
    old_lock = Catalog.open(str(base), release="2026.1").write_lock(tmp_path / "old.yaml")

    with pytest.raises(ValueError, match="latest"):
        writer.retire("flood", "v2-r1", "tape")
    with pytest.raises(ValueError, match="most recent release"):
        writer.retire("flood", "v1-r1", "tape")
    writer.release("2026.2", from_release="2026.1", pins={"flood": "v2-r1"})
    with pytest.raises(ValueError):
        writer.retire("flood", "v1-r1", "")
    writer.retire(
        "flood", "v1-r1", "on tape; email irv@example.org", date="2027-07-01", location="tape-0042"
    )
    coll = json.loads((base / "flood" / "collection.json").read_text())
    assert coll["irv:retired"] == {
        "v1-r1": {
            "date": "2027-07-01",
            "note": "on tape; email irv@example.org",
            "location": "tape-0042",
        }
    }
    assert check(coll, "collection") == []
    # metadata updates keep the retirement
    writer.add_collection({k: v for k, v in coll.items() if k != "irv:retired"})
    assert "irv:retired" in json.loads((base / "flood" / "collection.json").read_text())

    # remove the data files, as the retirement procedure does
    for f in (base / "flood" / "v1-r1").glob("*.tif"):
        f.unlink()
    assert validate_catalog(str(base), check_files=True) == []

    old = Catalog.open(str(base), release="2026.1", cache_dir=str(tmp_path / "cache"))
    item = old.item("flood", "river_rcp45_rp10")  # metadata still resolves
    assert old.retirement("flood") == coll["irv:retired"]["v1-r1"]
    with pytest.raises(RetiredError, match="retired to cold storage on 2027-07-01"):
        old.fetch(item)
    with pytest.raises(RetiredError):
        old.href(item)
    with pytest.raises(RetiredError):
        old.lock()
    locked = Catalog.from_lock(old_lock, cache_dir=str(tmp_path / "cache"))
    with pytest.raises(RetiredError, match="tape-0042"):
        locked.fetch_locked("flood", "river_rcp45_rp10")
    diff = old.lock_matches(old_lock)
    assert not diff and "flood v1-r1 is retired" in str(diff)

    # producers cannot pin or extend a retired version
    with pytest.raises(ValueError, match="retired"):
        writer.release("2027.1", from_release="2026.1")
    with pytest.raises(ValueError, match="retired"):
        writer.release("2027.1", pins={"flood": "v1-r1"})
    src = tmp_path / "src"
    src.mkdir()
    new_item, path = flood_item(src, "v1-r1", 1000, 9.0)
    with pytest.raises(ImmutableError, match="retired"):
        writer.add_item(new_item, path)

    rows = {(r["dataset"], r["version"]): r for r in pub.retire_report(str(base))}
    assert rows[("flood", "v1-r1")]["retired"] == "2027-07-01"
    assert not rows[("flood", "v1-r1")]["eligible"]
    assert rows[("flood", "v1-r1")]["reason"] == "retired 2027-07-01"

    writer.unretire("flood", "v1-r1")
    assert "irv:retired" not in json.loads((base / "flood" / "collection.json").read_text())
    with pytest.raises(ValueError):
        writer.unretire("flood", "v1-r1")


def test_publish_tolerates_retirement(work):
    base, private = work
    remote = f"memory://remote-{uuid.uuid4().hex[:8]}/irv-datapkg"
    pub.publish(str(base), remote, local_private=str(private))
    writer = CatalogWriter(base)
    writer.release("2026.2", from_release="2026.1", pins={"flood": "v2-r1"})
    writer.retire("flood", "v1-r1", "on tape")
    # retired files removed from the remote, but still on the local drive
    fs = fsspec.filesystem("memory")
    rpath = remote[len("memory://") :]
    for f in fs.glob(rpath + "/flood/v1-r1/*.tif"):
        fs.rm(f)
    summary = pub.publish(str(base), remote, local_private=str(private))
    actions = {a.path: a.action for a in summary.actions}
    assert actions["flood/collection.json"] == "update"
    assert actions["flood/v1-r1/flood_v1-r1_rp10.tif"] == "retired"
    assert actions["flood/v1-r1/river_rcp45_rp10.json"] == "skip"
    assert not fs.glob(rpath + "/flood/v1-r1/*.tif")
    assert summary.to_dict()["counts"]["retired"] == 2
    assert "2 retired files not re-uploaded" in summary.format()
    assert validate_catalog(remote, private_base=remote + "-private") == []
    for root in (rpath, rpath + "-private"):
        if fs.exists(root):
            fs.rm(root, recursive=True)


def test_cli_retire_mark(work):
    base, _ = work
    CatalogWriter(base).release("2026.2", from_release="2026.1", pins={"flood": "v2-r1"})
    runner = CliRunner()
    r = runner.invoke(main, ["retire", str(base), "--mark", "flood=v1-r1"])
    assert r.exit_code != 0 and "--note" in r.output
    r = runner.invoke(
        main,
        ["retire", str(base), "--mark", "flood=v1-r1", "--note", "tape", "--now", "2027-07-01"],
    )
    assert r.exit_code == 0, r.output
    assert LockFile  # imported API
    coll = json.loads((base / "flood" / "collection.json").read_text())
    assert coll["irv:retired"]["v1-r1"]["date"] == "2027-07-01"
    r = runner.invoke(main, ["retire", str(base), "--mark", "flood=v2-r1", "--note", "x"])
    assert r.exit_code != 0 and "latest" in r.output


def test_numeric_filter_values_match_across_forms():
    from irv_datapkg.catalog.lock import matches

    assert matches({"return_period": 100}, {"return_period": "100.0"})
    assert matches({"return_period": "100"}, {"return_period": 100})
    assert matches({"epoch": 2030}, {"epoch": ["2050", "2030"]})
    assert not matches({"return_period": 100}, {"return_period": "1000"})
    assert matches({"rcp": "4.5"}, {"rcp": 4.5})
    assert not matches({"gcm": "a"}, {"gcm": "b"})
    assert matches({"flag": True}, {"flag": "True"})
