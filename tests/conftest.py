"""Synthetic fixtures: a tiny catalogue (COGs, GeoParquet) and a workflow run."""

import os
import shutil
import subprocess
import sys
import zipfile
from pathlib import Path

import geopandas
import numpy as np
import pytest
import rasterio
import rasterio.shutil
from rasterio.io import MemoryFile
from rasterio.transform import from_origin
from shapely.geometry import Point

from irv_datapkg import recipes
from irv_datapkg.catalog import CatalogWriter, build_collection, build_item

COMMIT = "0123456789abcdef0123456789abcdef01234567"
NODATA = -9999.0


def make_cog(path, value, width=20, height=10):
    """20x10 float32 grid over lon 0..2, lat 0..1 (0.1 degree cells)."""
    data = np.full((1, height, width), value, dtype="float32")
    data[0, 0, 0] = NODATA
    profile = dict(
        driver="GTiff",
        width=width,
        height=height,
        count=1,
        dtype="float32",
        crs="EPSG:4326",
        transform=from_origin(0, 1, 0.1, 0.1),
        nodata=NODATA,
    )
    with MemoryFile() as mem:
        with mem.open(**profile) as ds:
            ds.write(data)
            ds.set_band_unit(1, "m")
            ds.set_band_description(1, "inundation depth")
        with mem.open() as ds:
            rasterio.shutil.copy(ds, str(path), driver="COG", compress="DEFLATE")
    return path


def make_parquet(path, n=10):
    gdf = geopandas.GeoDataFrame(
        {"name": [f"p{i}" for i in range(n)], "value": list(range(n))},
        geometry=[Point(0.05 + 0.2 * i, 0.5) for i in range(n)],
        crs="EPSG:4326",
    )
    gdf.to_parquet(path, write_covering_bbox=True)
    return path


def flood_item(src, version, rp, value):
    path = make_cog(src / f"flood_{version}_rp{rp}.tif", value)
    return (
        build_item(
            path,
            "flood",
            version,
            f"river_rcp45_rp{rp}",
            properties={"hazard": "river", "rcp": "4.5", "return_period": rp},
            source_url=f"https://example.org/flood/{version}/rp{rp}.tif",
            retrieved="2026-01-15",
            commit=COMMIT,
        ),
        path,
    )


@pytest.fixture(scope="session")
def catalogue(tmp_path_factory):
    """Write the fixture catalogue; return (base, private_base)."""
    root = tmp_path_factory.mktemp("cat")
    src = root / "src"
    src.mkdir()
    base, private = root / "base", root / "base-private"
    writer = CatalogWriter(base)

    writer.add_collection(
        build_collection(
            "flood",
            title="Synthetic flood",
            description="Tiny synthetic flood maps",
            license="CC-BY-4.0",
            citation="Nobody (2026) Synthetic flood.",
            upstream_url="https://example.org/flood",
            upstream_version="v1",
            processing=["convert to COG"],
            dimensions=["hazard", "rcp", "return_period"],
        )
    )
    for version in ("v1-r1", "v2-r1"):
        for rp, value in ((10, 1.0), (100, 2.0)):
            item, path = flood_item(src, version, rp, value)
            writer.add_item(item, path)

    writer.add_collection(
        build_collection(
            "points",
            title="Synthetic points",
            description="Tiny GeoParquet",
            license="ODbL-1.0",
            citation="Nobody (2026) Points.",
            upstream_url="https://example.org/points",
            upstream_version="1.0",
        )
    )
    pq = make_parquet(src / "points.parquet")
    writer.add_item(
        build_item(
            pq,
            "points",
            "1.0-r1",
            "points",
            source_url="https://example.org/points.zip",
            retrieved="2026-01-15",
            commit=COMMIT,
        ),
        pq,
    )

    writer.add_collection(
        build_collection(
            "secret_tracks",
            title="Private tracks",
            description="Not public",
            license="proprietary",
            citation="Someone (2026) Tracks.",
            upstream_url="Oxford (not public)",
            upstream_version="early",
            access="private",
        )
    )
    tracks = make_parquet(src / "tracks.parquet", n=3)
    writer.add_item(
        build_item(
            tracks,
            "secret_tracks",
            "early-r1",
            "tracks",
            source_url="internal",
            retrieved="2026-01-15",
            commit=COMMIT,
        ),
        tracks,
    )

    writer.release("2026.1", pins={"flood": "v1-r1"})
    return base, private


#
# Synthetic Snakemake workflow run (test_workflow.py, test_e2e.py)
#
REPO = Path(__file__).parent.parent
SNAKEMAKE = shutil.which("snakemake") or str(Path(sys.executable).parent / "snakemake")

AQUEDUCT = [
    "inunriver_rcp4p5_0000GFDL-ESM2M_2050_rp00100",
    "inunriver_historical_000000000WATCH_1980_rp00010",
    "inuncoast_rcp8p5_wtsub_2080_rp0100_0",
    "inuncoast_historical_nosub_hist_rp0100_0",
]


@pytest.fixture(scope="session")
def workflow_run(tmp_path_factory):
    """Run the real Snakemake workflow on a synthetic T0 workspace (no downloads):
    storm_fixed_rp and aqueduct_flood T1 plus Jamaica T2 summaries. Returns
    (workdir, workspace); the local catalogue is workspace/catalogue."""
    from test_recipes import make_tif

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
