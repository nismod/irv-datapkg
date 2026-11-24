"""End-to-end release on synthetic data: T0 -> T1 (Snakemake) -> deep validation
-> release -> publish to a file:// remote -> audit -> consumer lock and fetch.

Run with ``pytest -m e2e``. The Snakemake run is the ``workflow_run`` session
fixture in conftest.py (tiny synthetic STORM and Aqueduct T0 files; nothing is
downloaded). Steps use the same code paths as production: the CLI commands
``validate --deep``, ``release``, ``publish``, ``audit``, ``lock`` and ``fetch --lock``.
"""

import hashlib
import json
import shutil
from pathlib import Path

import pytest
import yaml
from click.testing import CliRunner

from irv_datapkg.catalog import Catalog, validate_catalog
from irv_datapkg.cli import main

from conftest import AQUEDUCT, SNAKEMAKE

pytestmark = [
    pytest.mark.skipif(not Path(SNAKEMAKE).exists(), reason="snakemake not installed"),
    pytest.mark.e2e,
]

RELEASE = "2026.1"


def _cli(*args):
    result = CliRunner().invoke(main, [str(a) for a in args])
    assert result.exit_code == 0, result.output
    return result.output


@pytest.fixture(scope="module")
def released(workflow_run, tmp_path_factory):
    """Copy the workflow's local catalogue, release it and publish to file://."""
    _, ws = workflow_run
    root = tmp_path_factory.mktemp("e2e")
    local = root / "catalogue"
    shutil.copytree(ws / "catalogue", local)
    remote = root / "remote"
    _cli("release", local, RELEASE, "--description", "synthetic end-to-end release")
    _cli("publish", local, f"file://{remote}", "--report", root / "publish.json")
    return local, remote, json.loads((root / "publish.json").read_text())


def test_local_catalogue_is_deep_valid(workflow_run):
    _, ws = workflow_run
    warnings = []
    errors = validate_catalog(str(ws / "catalogue"), deep=True, warnings=warnings)
    assert errors == []
    # recipes.to_cog writes the full ADR 0004 profile: nothing to warn about
    assert warnings == []


def test_publish_uploads_then_is_idempotent(released):
    local, remote, report = released
    # 144 data files + 144 items + 2 collections + 1 release + 1 root
    assert report["counts"]["upload"] == 292 and report["counts"]["conflict"] == 0
    assert (remote / "releases" / RELEASE / "catalog.json").exists()
    again = _cli("publish", local, f"file://{remote}", "--dry-run")
    assert "Would upload 0 new" in again and "update 0 mutable" in again


def test_remote_is_deep_valid_and_audits_clean(released):
    _, remote, _ = released
    out = _cli("validate", f"file://{remote}", "--deep", "--strict")
    assert out.strip() == "valid"
    out = _cli("audit", f"file://{remote}", "--release", RELEASE)
    assert "audited 144 of 144 assets: 144 ok" in out  # 140 STORM + 4 Aqueduct


def test_consumer_lock_and_fetch(released, tmp_path):
    _, remote, _ = released
    lock = tmp_path / "datapkg.lock.yaml"
    _cli(
        "lock",
        "aqueduct_flood",
        "storm_fixed_rp",
        "--base",
        f"file://{remote}",
        "--release",
        RELEASE,
        "-p",
        "storm_fixed_rp:gcm=constant",
        "-p",
        "storm_fixed_rp:return_period=100",
        "-o",
        lock,
    )
    data = yaml.safe_load(lock.read_text())
    assert data["release"] == RELEASE
    assert len(data["datasets"]["aqueduct_flood"]["items"]) == len(AQUEDUCT)
    assert len(data["datasets"]["storm_fixed_rp"]["items"]) == 1
    _cli(
        "lock",
        "aqueduct_flood",
        "storm_fixed_rp",
        "--base",
        f"file://{remote}",
        "--release",
        RELEASE,
        "-p",
        "storm_fixed_rp:gcm=constant",
        "-p",
        "storm_fixed_rp:return_period=100",
        "-o",
        lock,
        "--check",
    )

    # Python API, as open-gira / infra-risk-vis use it
    cat = Catalog.from_lock(lock, cache_dir=tmp_path / "cache")
    river = cat.items("aqueduct_flood", hazard="river", rcp="4.5")
    assert len(river) == 1
    path = cat.fetch(river[0], tmp_path / "inputs")
    assert hashlib.sha256(path.read_bytes()).hexdigest() == river[0].asset.sha256
    storm = cat.fetch_locked(
        "storm_fixed_rp",
        cat.lockfile.item_ids("storm_fixed_rp")[0],
        tmp_path / "storm",
        link="symlink",
    )
    assert storm.is_symlink() and storm.stat().st_size > 0

    # CLI fetch from the lock alone, then audit the lock against the remote
    out = _cli(
        "fetch",
        "aqueduct_flood",
        "--lock",
        lock,
        "-p",
        "hazard=coastal",
        "--cache-dir",
        tmp_path / "cache2",
        "-o",
        tmp_path / "coastal",
    )
    assert len(out.split()) == 2
    assert "audited 5 of 5 assets: 5 ok" in _cli("audit", lock)
