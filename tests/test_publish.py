"""Publishing, retirement report and link check (docs/storage.md). No network."""

import csv
import datetime as dt
import json
import shutil
import uuid
from pathlib import Path

import fsspec
import pytest
from click.testing import CliRunner

from irv_datapkg import linkcheck as lc
from irv_datapkg import publish as pub
from irv_datapkg.catalog import CatalogWriter, ImmutableError, validate_catalog
from irv_datapkg.cli import main

from conftest import flood_item


@pytest.fixture
def local(catalogue, tmp_path):
    """A private, writable copy of the fixture catalogue: (base, private)."""
    base, private = catalogue
    dest = tmp_path / "local"
    shutil.copytree(base, dest)
    shutil.copytree(private, tmp_path / "local-private")
    return dest, tmp_path / "local-private"


@pytest.fixture
def remote():
    """A fresh in-memory remote base."""
    url = f"memory://remote-{uuid.uuid4().hex[:8]}/irv-datapkg"
    yield url
    fs = fsspec.filesystem("memory")
    for root in (url, url + "-private"):
        path = root[len("memory://") :]
        if fs.exists(path):
            fs.rm(path, recursive=True)


def remote_files(url):
    fs, path = fsspec.core.url_to_fs(url)
    if not fs.exists(path):
        return set()
    path = path.rstrip("/")
    return {p[len(path) + 1 :] for p in fs.find(path)}


def test_classify():
    assert pub.classify("catalog.json") == pub.ROOT
    assert pub.classify("releases/2026.1/catalog.json") == pub.RELEASE
    assert pub.classify("flood/collection.json") == pub.COLLECTION
    assert pub.classify("flood/v1-r1/river.json") == pub.ITEM
    assert pub.classify("flood/v1-r1/river.tif") == pub.DATA
    assert pub.classify("flood/v1-r1/x.zarr/zarr.json") == pub.DATA
    assert pub.classify("extracts/ken/1.0/x.json") == pub.DATA


def test_publish_order(local, remote, monkeypatch):
    base, _ = local
    written = []
    real_copy = pub._copy

    def spy(src, dest, *a):
        real_copy(src, dest, *a)
        written.append(dest)

    monkeypatch.setattr(pub, "_copy", spy)
    summary = pub.publish(str(base), remote)
    assert summary.count("upload") == len(written) > 0
    assert summary.count("conflict") == 0

    def pos(suffix):
        return written.index(suffix)

    pub_root = remote + "/catalog.json"
    priv_root = remote + "-private/catalog.json"
    # roots go last; no dangling links at any point
    assert written[-1] == pub_root
    for i, dest in enumerate(written):
        rel_tier = pub.classify(
            dest.split("irv-datapkg-private/", 1)[-1].split("irv-datapkg/", 1)[-1]
        )
        for later in written[i + 1 :]:
            later_rel = later.split("irv-datapkg-private/", 1)[-1].split("irv-datapkg/", 1)[-1]
            assert pub.classify(later_rel) >= rel_tier
    # every item JSON arrives after its data file; collections after their items
    for dest in written:
        if dest.endswith(".json") and "/v" in dest and "collection" not in dest:
            item = json.loads(fsspec.open(dest, "r").open().read())
            data = dest.rsplit("/", 1)[0] + "/" + item["assets"]["data"]["href"][2:]
            assert pos(data) < pos(dest)
            coll = dest.rsplit("/", 2)[0] + "/collection.json"
            assert pos(dest) < pos(coll)
    # the private collection lands before the public release that pins it by id
    assert pos(remote + "-private/secret_tracks/collection.json") < pos(
        remote + "/releases/2026.1/catalog.json"
    )
    assert pos(priv_root) < pos(pub_root)
    assert validate_catalog(remote, check_files=True) == []


def test_publish_idempotent_and_incremental(local, remote, tmp_path):
    base, _ = local
    first = pub.publish(str(base), remote)
    again = pub.publish(str(base), remote)
    assert again.count("upload") == again.count("update") == 0
    assert again.count("skip") == first.count("upload")

    # add a version locally: only new files plus the mutable collection go up
    src = tmp_path / "src"
    src.mkdir()
    writer = CatalogWriter(base)
    item, path = flood_item(src, "v3-r1", 10, 3.0)
    writer.add_item(item, path)
    third = pub.publish(str(base), remote)
    changed = {(a.action, a.path) for a in third.actions if a.action != "skip"}
    assert changed == {
        ("upload", "flood/v3-r1/flood_v3-r1_rp10.tif"),
        ("upload", "flood/v3-r1/river_rcp45_rp10.json"),
        ("update", "flood/collection.json"),
    }
    assert validate_catalog(remote) == []


def test_dry_run_writes_nothing(local, remote):
    base, _ = local
    summary = pub.publish(str(base), remote, dry_run=True)
    assert summary.count("upload") > 0
    assert remote_files(remote) == set()
    assert remote_files(remote + "-private") == set()
    assert "Would upload" in summary.format()


def _overwrite(url, data):
    with fsspec.open(url, "wb") as f:
        f.write(data)


def test_refuses_changed_immutable_files(local, remote):
    base, _ = local
    pub.publish(str(base), remote)
    data_rel = "flood/v1-r1/flood_v1-r1_rp10.tif"
    fs, rpath = fsspec.core.url_to_fs(remote + "/" + data_rel)
    original = fs.cat_file(rpath)

    # different size: detected without hashing the remote
    _overwrite(remote + "/" + data_rel, original + b"x")
    with pytest.raises(ImmutableError, match="flood_v1-r1_rp10.tif"):
        pub.publish(str(base), remote)
    dry = pub.publish(str(base), remote, dry_run=True)
    assert [a.path for a in dry.conflicts] == [data_rel]

    # same size, different bytes: caught when hashing remote data
    corrupt = bytes([original[0] ^ 0xFF]) + original[1:]
    _overwrite(remote + "/" + data_rel, corrupt)
    with pytest.raises(ImmutableError):
        pub.publish(str(base), remote, verify_remote=True)

    # a changed item JSON is always refused, and nothing else is written
    _overwrite(remote + "/" + data_rel, original)
    item_url = remote + "/flood/v1-r1/river_rcp45_rp10.json"
    _overwrite(item_url, b'{"changed": true}\n')
    before = fsspec.open(remote + "/catalog.json", "rb").open().read()
    Path(base, "catalog.json").write_text(Path(base, "catalog.json").read_text() + " ")
    with pytest.raises(ImmutableError, match="river_rcp45_rp10.json"):
        pub.publish(str(base), remote)
    assert fsspec.open(remote + "/catalog.json", "rb").open().read() == before


def test_private_public_separation(local, remote, tmp_path):
    base, private = local
    pub.publish(str(base), remote)
    public = remote_files(remote)
    secret = remote_files(remote + "-private")
    assert not any("secret_tracks" in p for p in public)
    assert {p.split("/")[0] for p in secret} == {"catalog.json", "secret_tracks"}
    assert all(not p.startswith(("flood", "points")) for p in secret)

    # --no-private leaves the private remote alone
    other = remote + "-b"
    pub.publish(str(base), other, include_private=False)
    assert remote_files(other + "-private") == set()
    assert remote_files(other)

    # a private collection under the public root is refused
    shutil.copytree(private / "secret_tracks", base / "secret_tracks")
    with pytest.raises(pub.PublishError, match="irv:access"):
        pub.publish(str(base), remote + "-c", dry_run=True)

    with pytest.raises(pub.PublishError, match="overlaps"):
        pub.publish(str(base), remote, remote_private=remote + "/private")


def test_publish_to_file_remote_cli(local, tmp_path):
    base, _ = local
    dest = tmp_path / "remote" / "irv-datapkg"
    runner = CliRunner()
    r = runner.invoke(main, ["publish", str(base), f"file://{dest}", "--dry-run"])
    assert r.exit_code == 0, r.output
    assert "Would upload" in r.output and not dest.exists()
    report = tmp_path / "summary.json"
    r = runner.invoke(main, ["publish", str(base), f"file://{dest}", "--report", str(report)])
    assert r.exit_code == 0, r.output
    assert json.loads(report.read_text())["counts"]["conflict"] == 0
    assert (tmp_path / "remote" / "irv-datapkg-private" / "secret_tracks").is_dir()
    assert validate_catalog(str(dest), check_files=True) == []
    r = runner.invoke(main, ["publish", str(base), f"file://{dest}"])
    assert "upload 0 new" in r.output


# --- retire ----------------------------------------------------------------------


def _set_created(base, name, date):
    path = Path(base, "releases", name, "catalog.json")
    doc = json.loads(path.read_text())
    doc["irv:created"] = date + "T00:00:00Z"
    path.write_text(json.dumps(doc))


@pytest.fixture
def history(local, tmp_path):
    """flood: v1-r1 (in 2026.1), v2-r1 (never pinned), v3-r1 (latest, in 2026.2)."""
    base, private = local
    src = tmp_path / "src"
    src.mkdir()
    writer = CatalogWriter(base)
    item, path = flood_item(src, "v3-r1", 10, 3.0)
    writer.add_item(item, path)
    writer.release("2026.2", from_release="2026.1", pins={"flood": "v3-r1"})
    _set_created(base, "2026.1", "2026-06-30")
    _set_created(base, "2026.2", "2026-12-31")
    return base, private


def _rows(base, now):
    return {
        (r["dataset"], r["version"]): r
        for r in pub.retire_report(str(base), now=dt.date.fromisoformat(now))
    }


def test_retire_selection(history):
    base, _ = history
    rows = _rows(base, "2027-06-01")
    assert rows[("flood", "v2-r1")]["eligible"]
    assert rows[("flood", "v2-r1")]["reason"] == "never pinned by a release"
    assert not rows[("flood", "v1-r1")]["eligible"]  # 2026.1 is within a year
    assert not rows[("flood", "v3-r1")]["eligible"]  # latest
    assert not rows[("points", "1.0-r1")]["eligible"]
    assert rows[("secret_tracks", "early-r1")]["access"] == "private"
    assert rows[("flood", "v1-r1")]["pinned_by"] == "2026.1"
    assert rows[("flood", "v1-r1")]["bytes"] > 0

    rows = _rows(base, "2027-07-01")
    assert rows[("flood", "v1-r1")]["eligible"]
    assert rows[("flood", "v1-r1")]["reason"] == "last pinned 2026-06-30"

    # too recently retrieved: grace period for versions no release ever saw
    rows = _rows(base, "2026-12-01")
    assert not rows[("flood", "v2-r1")]["eligible"]


def test_retire_keeps_most_recent_release(history):
    base, _ = history
    # even years later, whatever the latest release pins stays online
    rows = _rows(base, "2030-01-01")
    assert not rows[("flood", "v3-r1")]["eligible"]
    assert not rows[("points", "1.0-r1")]["eligible"]


def test_retire_cli(history, tmp_path):
    base, _ = history
    out = tmp_path / "retire.csv"
    r = CliRunner().invoke(main, ["retire", str(base), "--now", "2027-07-01", "-o", str(out)])
    assert r.exit_code == 0, r.output
    assert "RETIRE" in r.output and "2 of 5 versions eligible" in r.output
    rows = list(csv.DictReader(out.open()))
    assert {(x["version"], x["eligible"]) for x in rows if x["dataset"] == "flood"} == {
        ("v1-r1", "True"),
        ("v2-r1", "True"),
        ("v3-r1", "False"),
    }


# --- linkcheck -------------------------------------------------------------------


class FakeResponse:
    def __init__(self, status, url):
        self.status_code, self.url = status, url

    def close(self):
        pass


class FakeSession:
    """Maps URL -> (HEAD status, GET status) or an exception; records calls."""

    def __init__(self, table):
        self.table, self.calls = table, []

    def _reply(self, method, url, **kw):
        self.calls.append((method, url))
        entry = self.table.get(url, (404, 404))
        if isinstance(entry, Exception):
            raise entry
        return FakeResponse(entry[0] if method == "HEAD" else entry[1], url)

    def head(self, url, **kw):
        return self._reply("HEAD", url, **kw)

    def get(self, url, **kw):
        assert kw.get("stream") is True
        return self._reply("GET", url, **kw)


def test_urls_for():
    assert lc.urls_for("https://example.org/a/") == ["https://example.org/a/"]
    assert lc.urls_for("Zenodo 10.5281/zenodo.7732393 / 10.5281/zenodo.11582369") == [
        "https://doi.org/10.5281/zenodo.7732393",
        "https://doi.org/10.5281/zenodo.11582369",
    ]
    assert lc.urls_for("Dryad doi:10.5061/dryad.dk1j0") == ["https://doi.org/10.5061/dryad.dk1j0"]
    assert lc.urls_for("s3://wri-projects/Aqueduct/v2") == [
        "https://wri-projects.s3.amazonaws.com/?list-type=2&max-keys=1&prefix=Aqueduct/v2"
    ]
    assert lc.urls_for("Oxford (not public)") == []
    assert lc.urls_for("") == []


def test_linkcheck_mocked(tmp_path, catalogue):
    csv_path = tmp_path / "datasets.csv"
    csv_path.write_text(
        "id,title,upstream,upstream_version,access,used_by,work_package\n"
        "a,A,https://ok.example/a,1,public,x,WP5\n"
        "b,B,https://nohead.example/b,1,public,x,WP5\n"
        "c,C,https://gone.example/c,1,public,x,WP5\n"
        "d,D,Zenodo 10.5281/zenodo.1,1,public,x,WP5\n"
        "e,E,Oxford (not public),1,private,x,WP5\n"
        "f,F,https://down.example/f,1,public,x,WP5\n"
        "g,G,https://ok.example/a,1,public,x,WP5\n"
    )
    session = FakeSession(
        {
            "https://ok.example/a": (200, 200),
            "https://nohead.example/b": (405, 200),
            "https://gone.example/c": (404, 404),
            "https://doi.org/10.5281/zenodo.1": (302, 302),
            "https://down.example/f": ConnectionError("no route"),
            "https://example.org/flood": (200, 200),
            "https://example.org/points": (200, 200),
        }
    )
    base, _ = catalogue
    targets = lc.targets_from_csv(str(csv_path)) + lc.targets_from_catalogue(str(base))
    results = lc.linkcheck(targets, session=session, now=dt.datetime(2026, 12, 1))
    by = {(r.source, r.dataset): r for r in results}
    assert by[("datasets.csv", "a")].ok and by[("datasets.csv", "a")].method == "HEAD"
    assert by[("datasets.csv", "b")].ok and by[("datasets.csv", "b")].method == "GET"
    assert by[("datasets.csv", "c")].ok is False and by[("datasets.csv", "c")].status == 404
    assert by[("datasets.csv", "d")].ok
    assert by[("datasets.csv", "e")].ok is None
    assert "ConnectionError" in by[("datasets.csv", "f")].error
    assert by[("catalogue", "flood")].ok
    assert by[("catalogue", "secret_tracks")].ok is None  # "Oxford (not public)"
    # identical URLs are requested once
    assert session.calls.count(("HEAD", "https://ok.example/a")) == 1

    out = lc.write_report(results, str(tmp_path / "report.csv"))
    rows = list(csv.DictReader(open(out)))
    assert rows[0].keys() == set(lc.FIELDS) or list(rows[0].keys()) == lc.FIELDS
    assert {r["ok"] for r in rows} == {"true", "false", ""}
    assert rows[0]["checked_at"] == "2026-12-01T00:00:00Z"


def test_linkcheck_cli(tmp_path, monkeypatch):
    csv_path = tmp_path / "datasets.csv"
    csv_path.write_text("id,upstream\na,https://ok.example/a\nc,https://gone.example/c\n")
    session = FakeSession({"https://ok.example/a": (200, 200)})
    monkeypatch.setattr(lc, "default_session", lambda: session)
    out = tmp_path / "lc.csv"
    runner = CliRunner()
    r = runner.invoke(main, ["linkcheck", "--datasets-csv", str(csv_path), "-o", str(out)])
    assert r.exit_code == 0, r.output
    assert "1 ok, 1 failed, 0 skipped" in r.output
    r = runner.invoke(
        main, ["linkcheck", "--datasets-csv", str(csv_path), "-o", str(out), "--fail-on-error"]
    )
    assert r.exit_code == 1
    assert out.exists()


# --- rules/publish.smk -----------------------------------------------------------


def test_publish_rules(local, tmp_path):
    """Run the catalogue_* rules against a file:// remote (needs snakemake)."""
    pytest.importorskip("snakemake")
    import subprocess
    import sys

    import yaml

    base, private = local
    smk = Path(__file__).parents[1] / "rules" / "publish.smk"
    work = tmp_path / "wf"
    work.mkdir()
    (work / "Snakefile").write_text(f'include: "{smk}"\n')
    remote = tmp_path / "remote" / "irv-datapkg"
    cfg = work / "config.yaml"
    cfg.write_text(
        yaml.safe_dump(
            {
                "workspace_dir": str(work),
                "catalogue_base": str(base),
                "publish_remote": f"file://{remote}",
            }
        )
    )

    def run(*targets, extra=()):
        cmd = [sys.executable, "-m", "snakemake", *targets, "-c1", "--configfile", str(cfg)]
        return subprocess.run(
            cmd + list(extra), cwd=work, capture_output=True, text=True, timeout=300
        )

    r = run("catalogue_publish_dry_run")
    assert r.returncode == 0, r.stderr
    assert not remote.exists()
    r = run("catalogue_publish")
    assert r.returncode == 0, r.stderr
    assert validate_catalog(str(remote), check_files=True) == []
    r = run(
        "catalogue_release",
        extra=["--config", "release=2026.2", "release_from=2026.1", "release_pins=flood=v2-r1"],
    )
    assert r.returncode == 0, r.stderr
    assert (base / "releases" / "2026.2" / "catalog.json").exists()
    r = run("catalogue_publish")
    assert r.returncode == 0, r.stderr
    assert (remote / "releases" / "2026.2" / "catalog.json").exists()
    r = run("catalogue_retire_report")
    assert r.returncode == 0, r.stderr
    assert list((work / "reports").glob("retire-*.csv"))
    r = run("linkcheck", extra=["-n"])  # parse only: the real rule uses the network
    assert r.returncode == 0, r.stderr
