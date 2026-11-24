"""Published analysis results: a run root under results/open-gira/{run}/ (spec section 8)."""

import json

import yaml

from irv_datapkg.catalog import (
    Catalog,
    CatalogWriter,
    build_collection,
    build_item,
    check,
    validate_catalog,
)
from irv_datapkg.publish import publish

from conftest import COMMIT, make_parquet

RUN = "20261124-0123456-r1"


def make_run(root, run=RUN):
    """A one-layer run root written with the ordinary producer API."""
    run_root = root / "results" / "open-gira" / run
    src = root / "src"
    src.mkdir(parents=True, exist_ok=True)
    writer = CatalogWriter(run_root)
    writer.add_collection(
        build_collection(
            "rail_edges",
            title="Rail network edges",
            description="open-gira rail network",
            license="ODbL-1.0",
            citation="open-gira contributors",
            upstream_url="https://github.com/nismod/open-gira",
            upstream_version=COMMIT,
            dimensions=["osm_extract"],
        )
    )
    path = make_parquet(src / "planet-latest.parquet")
    item = build_item(
        path,
        "rail_edges",
        run,
        "planet-latest",
        properties={"osm_extract": "planet-latest", "irv:source_path": "x/edges.gpq"},
        source_url="open-gira://results/x/edges.gpq",
        retrieved="2026-11-24",
        commit=COMMIT,
    )
    writer.add_item(item, path)
    root_url = run_root / "catalog.json"
    doc = json.loads(root_url.read_text())
    doc.update(
        {
            "id": f"open-gira-{run}",
            "irv:producer": "open-gira",
            "irv:run": run,
            "irv:commit": COMMIT[:7],
            "irv:dirty": False,
            "irv:config_hash": "ab" * 32,
            "irv:input_lock": None,
            "irv:created": "2026-11-24T12:00:00Z",
        }
    )
    root_url.write_text(json.dumps(doc, indent=2))
    return run_root


def test_run_root_is_a_valid_catalogue(tmp_path):
    run_root = make_run(tmp_path)
    assert validate_catalog(str(run_root), check_files=True) == []
    cat = Catalog.open(str(run_root))
    assert cat.version("rail_edges") == RUN
    (item,) = cat.items("rail_edges", osm_extract="planet-latest")
    assert item.properties["irv:source_path"] == "x/edges.gpq"


def test_run_root_contract_is_checked(tmp_path):
    run_root = make_run(tmp_path)
    root_url = run_root / "catalog.json"
    doc = json.loads(root_url.read_text())
    doc["irv:run"] = "20261124-0123456-r2"
    del doc["irv:config_hash"]
    root_url.write_text(json.dumps(doc))
    problems = validate_catalog(str(run_root))
    assert any("does not match its directory" in p for p in problems)
    assert any("irv:config_hash" in p for p in problems)
    assert any("expected ['20261124-0123456-r2']" in p for p in problems)


def test_ordinary_root_is_not_a_run(catalogue):
    base, _ = catalogue
    assert not [p for p in validate_catalog(str(base)) if "irv:run" in p]


def test_run_lock(tmp_path):
    run_root = make_run(tmp_path)
    item = Catalog.open(str(run_root)).item("rail_edges", "planet-latest")
    lock = {
        "lock_version": 1,
        "base": str(run_root),
        "run": RUN,
        "datasets": {
            "rail_edges": {
                "version": RUN,
                "access": "public",
                "items": {
                    "planet-latest": {
                        "data": {
                            "href": f"rail_edges/{RUN}/planet-latest.parquet",
                            "sha256": item.asset.sha256,
                            "size": item.asset.size,
                        }
                    }
                },
            }
        },
    }
    assert check(lock, "lock") == []
    assert check({**lock, "release": "2026.1"}, "lock")  # release and run are exclusive
    path = tmp_path / "results.lock.yaml"
    path.write_text(yaml.safe_dump(lock))
    cat = Catalog.from_lock(path)
    out = cat.fetch_locked("rail_edges", "planet-latest", tmp_path / "out.parquet", cache=False)
    assert out.stat().st_size == item.asset.size


def test_publish_run_root(tmp_path):
    run_root = make_run(tmp_path / "local")
    remote = tmp_path / "remote" / "results" / "open-gira" / RUN
    summary = publish(str(run_root), str(remote), include_private=False)
    assert summary.count("upload") == 4  # data, item, collection, root
    assert validate_catalog(str(remote), check_files=True) == []
    again = publish(str(run_root), str(remote), include_private=False, dry_run=True)
    assert {a.action for a in again.actions} == {"skip"}
