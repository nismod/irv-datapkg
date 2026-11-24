"""Run the Snakemake workflow on a synthetic workspace (no downloads).

T0 outputs for two recipes (storm_fixed_rp: static items; aqueduct_flood: items
listed by a checkpoint) are faked with tiny global GeoTIFFs plus provenance
sidecars, then T1 (COG + STAC registration) and T2 (country extract + summary
CSV) run for real. The workflow run itself is the session fixture
``workflow_run`` in conftest.py, shared with test_e2e.py.
"""

import csv
from pathlib import Path

import pytest
import rasterio

from irv_datapkg import recipes
from irv_datapkg.catalog import Catalog, validate_catalog

from conftest import AQUEDUCT, SNAKEMAKE

pytestmark = [
    pytest.mark.skipif(not Path(SNAKEMAKE).exists(), reason="snakemake not installed"),
    pytest.mark.e2e,
]


@pytest.fixture(scope="module")
def run(workflow_run):
    return workflow_run


def test_t1_catalogue_is_valid(run):
    _, ws = run
    base = ws / "catalogue"
    assert validate_catalog(str(base), check_files=True) == []
    cat = Catalog.open(str(base))
    assert sorted(cat.datasets()) == ["aqueduct_flood", "storm_fixed_rp"]
    assert len(cat.items("storm_fixed_rp")) == 140
    item = cat.items("storm_fixed_rp", gcm="constant", return_period=100)[0]
    assert item.properties["epoch"] == "baseline" and item.properties["rcp"] == "historical"
    assert recipes.is_cog(item.asset.href) == []
    river = cat.items("aqueduct_flood", hazard="river", rcp="4.5")[0]
    assert river.properties["gcm"] == "GFDL-ESM2M" and river.properties["return_period"] == 100
    assert river.asset.fields["irv:source_url"].endswith(f"/v2/{AQUEDUCT[0]}.tif")
    coastal = cat.items("aqueduct_flood", hazard="coastal", subsidence="wtsub")[0]
    assert coastal.properties["sea_level_rise"] == "0" and coastal.properties["epoch"] == 2080


def test_t2_extracts_and_summaries(run):
    work, _ = run
    jam = work / "data" / "JAM"
    with open(jam / "storm.csv") as fh:
        rows = list(csv.DictReader(fh))
    assert len(rows) == 140
    assert set(rows[0]) == {"hazard", "epoch", "rcp", "gcm", "return_period", "path"}
    assert {r["epoch"] for r in rows} == {"2010", "2050"}
    path = jam / "storm" / "STORM_FIXED_RETURN_PERIODS_constant_100_YR_RP__JAM.tif"
    assert path.exists()
    with rasterio.open(path) as ds:
        assert ds.width < 72 and ds.height < 36  # clipped to Jamaica

    with open(jam / "aqueduct_flood.csv") as fh:
        rows = list(csv.DictReader(fh))
    # the "nosub" coastal baseline is left out of T2, as before
    assert sorted(r["path"] for r in rows) == sorted(
        f"aqueduct_flood/{slug}__JAM.tif" for slug in AQUEDUCT if "nosub" not in slug
    )
    assert {r["hazard"] for r in rows} == {"river", "coastal"}
