"""Run the Snakemake workflow on a synthetic workspace (no downloads).

T0 outputs for two recipes (storm_fixed_rp: static items; aqueduct_flood: items
listed by a checkpoint) are faked with tiny global GeoTIFFs plus provenance
sidecars, then T1 (COG + STAC registration) and T2 (country extract + summary
CSV) run for real.
"""

import csv
import os
import shutil
import subprocess
import sys
import zipfile
from pathlib import Path

import pytest
import rasterio

from irv_datapkg import recipes
from irv_datapkg.catalog import Catalog, validate_catalog

from test_recipes import make_tif

REPO = Path(__file__).parent.parent
SNAKEMAKE = shutil.which("snakemake") or str(Path(sys.executable).parent / "snakemake")

pytestmark = pytest.mark.skipif(not Path(SNAKEMAKE).exists(), reason="snakemake not installed")

AQUEDUCT = [
    "inunriver_rcp4p5_0000GFDL-ESM2M_2050_rp00100",
    "inunriver_historical_000000000WATCH_1980_rp00010",
    "inuncoast_rcp8p5_wtsub_2080_rp0100_0",
    "inuncoast_historical_nosub_hist_rp0100_0",
]


@pytest.fixture(scope="module")
def run(tmp_path_factory):
    work = tmp_path_factory.mktemp("workflow")
    for name in ("metadata", "bundled_data", "config", "scripts"):
        (work / name).symlink_to(REPO / name)
    ws = work / "ws"

    # storm_fixed_rp T0: the record zip, with every mosaic as a tiny GeoTIFF
    storm_raw = ws / "raw" / "storm_fixed_rp" / "1.0.0"
    storm_raw.mkdir(parents=True)
    tif = make_tif(work / "tiny.tif", width=72, height=36, nodata=0)
    with zipfile.ZipFile(storm_raw / "STORM_FIXED_RETURN_PERIODS.zip", "w") as zf:
        for model in (
            "constant",
            "CMCC-CM2-VHR4",
            "CNRM-CM6-1-HR",
            "EC-Earth3P-HR",
            "HadGEM3-GC31-HM",
        ):
            for rp in (
                list(range(10, 100, 10))
                + list(range(100, 1000, 100))
                + list(range(1000, 10001, 1000))
            ):
                zf.write(
                    tif,
                    f"STORM_FIXED_RETURN_PERIODS/STORM_FIXED_RETURN_PERIODS_{model}_{rp}_YR_RP.tif",
                )
    recipes.write_provenance(
        storm_raw / "provenance.json",
        [storm_raw / "STORM_FIXED_RETURN_PERIODS.zip"],
        tool="test",
        url_prefix="https://zenodo.org/records/10931452/files",
    )

    # aqueduct_flood T0 (checkpoint output directory)
    aq_raw = ws / "raw" / "aqueduct_flood" / "v2"
    aq_raw.mkdir(parents=True)
    for slug in AQUEDUCT:
        make_tif(aq_raw / f"{slug}.tif", width=72, height=36, nodata=-9999)
    recipes.write_provenance(
        aq_raw / "provenance.json",
        sorted(aq_raw.glob("*.tif")),
        tool="test",
        url_prefix="http://wri-projects.s3.amazonaws.com/AqueductFloodTool/download/v2",
    )

    env = dict(os.environ, ZENODO_TOKEN="x", CDSAPI_URL="https://cds.invalid", CDSAPI_KEY="x")
    env["PATH"] = f"{Path(sys.executable).parent}:{env['PATH']}"
    cmd = [
        SNAKEMAKE,
        "--snakefile",
        str(REPO / "Snakefile"),
        "--directory",
        str(work),
        "--configfile",
        str(REPO / "config.json"),
        "--config",
        f"workspace_dir={ws}",
        "--cores",
        "2",
        "--force-use-threads",
        "all_t1_storm_fixed_rp",
        "all_t1_aqueduct_flood",
        "data/JAM/storm.csv",
        "data/JAM/aqueduct_flood.csv",
    ]
    result = subprocess.run(cmd, env=env, capture_output=True, text=True)
    assert result.returncode == 0, result.stdout[-3000:] + result.stderr[-6000:]
    return work, ws


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
