# Infrastructure Resilience Assessment Data Packages

[![DOI](https://zenodo.org/badge/DOI/10.5281/zenodo.10411140.svg)](https://doi.org/10.5281/zenodo.10411140)

Standalone workflow to create national scale open-data packages from global open
datasets.

### Setup

Get the latest code by cloning this repository:

```bash
git clone git@github.com:nismod/irv-datapkg.git
```

or

```bash
git clone https://github.com/nismod/irv-datapkg.git
```

Install Python and packages - suggest using micromamba:

```bash
micromamba create -f environment.yml
```

Activate the environment:

```bash
micromamba activate datapkg
```

### Run

The data packages are produced using a
[`snakemake`](https://snakemake.readthedocs.io/) workflow.

The workflow expects `ZENODO_TOKEN`, `CDSAPI_KEY` and `CDSAPI_URL` to be set as
environment variables - these must be set before running any workflow steps.

If not interacting with Zenodo or the Copernicus Climate Data Store, these can
be dummy strings:

```bash
echo "placeholder" > ZENODO_TOKEN
echo "https://cds-beta.climate.copernicus.eu/api" > CDSAPI_URL
echo "test" > CDSAPI_KEY
```

See [Climate Data Store API
docs](https://cds-beta.climate.copernicus.eu/how-to-api#use-the-cds-api-client-for-data-access)
and [Zenodo API docs](https://developers.zenodo.org/#introduction) for access details.

Export from the file to the environment:

```bash
export ZENODO_TOKEN=$(cat ZENODO_TOKEN)
export CDSAPI_KEY=$(cat CDSAPI_KEY)
export CDSAPI_URL=$(cat CDSAPI_URL)
```

The workflow builds each dataset in tiers: a T0 raw mirror with provenance, T1
analysis-ready global layers (COG / GeoParquet) registered in a local STAC
catalogue, and T2 country extracts (the Zenodo packages). Paths come from
[`config.json`](./config.json): `workspace_dir` (default `workspace`), and
`raw_dir`, `build_dir` and `catalogue_base` under it. See
[`docs/recipes.md`](docs/recipes.md) for the layout, the targets (`all_t1`,
`all_t1_{dataset}`) and how to add a dataset. On the production machine, point
the workspace at the mounted drive:

```bash
snakemake --cores 8 --config workspace_dir=/path/to/workspace all_t1
```

Check what will be run, if we ask for everything produced by the rule `all`,
before running the workflow for real:

```bash
snakemake --dry-run all
```

Run the workflow, asking for `all`, using 8 cores, with verbose log messages:

```bash
snakemake --cores 8 --verbose all
```

### Upload and publish

To publish, first [create a Zenodo token](https://zenodo.org/account/settings/applications/tokens/new/),
save it and export it as the `ZENODO_TOKEN` environment variable.

Workflow settings are in [`config.json`](./config.json): `datapkg_version` (the
version recorded in uploaded packages) and `zenodo_url` (the Zenodo instance to
use). To test against the sandbox instead of the live site, create a token on
[sandbox.zenodo.org](https://sandbox.zenodo.org) and either edit `zenodo_url` to
`sandbox.zenodo.org`, or override it for a single run:

```bash
snakemake --cores 1 --config zenodo_url=sandbox.zenodo.org zenodo/GBR.deposited
```

Upload a single data package:

```bash
snakemake --cores 1 zenodo/GBR.deposited
```

Publish (cannot be undone) either programmatically:

```bash
snakemake --cores 1 zenodo/GBR.published
```

Or after review online, through the Zenodo website ([sandbox](https://sandbox.zenodo.org/me/uploads), [live](https://zenodo.org/me/uploads))

### Post-publication

To get a quick list of DOIs from the Zenodo package json:

```bash
cat zenodo/*.deposition.json | jq '.metadata.prereserve_doi.doi'
```

To generate [`records.csv`](./records.csv) with details of published packages:

```bash
python scripts/published_metadata.py
```

To record Zenodo view and download statistics for the published records:

```bash
python scripts/record_zenodo_view_download_stats.py
```

The script reads `records.csv` from the repository's `main` branch and writes
the results to `stats.csv` in the current directory.

### Catalogue client

irv-datapkg publishes analysis-ready (T1) datasets as a static STAC catalogue
under one base URI. See [`docs/catalogue-spec.md`](docs/catalogue-spec.md) for
the contract and [`docs/adr`](docs/adr) for the decisions behind it. The
`irv_datapkg.catalog` module and the `irv-datapkg` command read the catalogue
from any fsspec root: a local path, `file://`, `s3://` or `https://`.

```python
from irv_datapkg.catalog import Catalog, extract

cat = Catalog.open("s3://bucket/irv-datapkg", release="2026.1")
items = cat.items("aqueduct_flood", hazard="river", rcp="4.5", return_period=100)
cat.href(items[0])                   # /vsis3/... path for windowed reads with GDAL/rasterio
cat.fetch(items[0], "input/")        # download, verify sha256, cache locally
cat.fetch(items[0], "input/a.tif", link="hardlink")  # no second copy on disk
cat.fetch(items[0], "input/a.tif", cache=False)      # stream straight to dest
cat.items("aqueduct_flood", gcm=Catalog.ABSENT, strict=True)  # property absent/null
items[0].asset.source_filename       # upstream file name (irv:source_filename)
extract(items[0], "clip.tif", bbox=(-1.0, 50.0, 2.0, 53.0))

# lock only what you use, check it, and fetch from it (also from a mirror)
cat.write_lock("datapkg.lock.yaml", select={"aqueduct_flood": {"return_period": [100, 1000]}})
assert cat.lock_matches("datapkg.lock.yaml", datasets=["aqueduct_flood"])
locked = Catalog.from_lock("datapkg.lock.yaml", base="/mnt/mirror/irv-datapkg")
locked.fetch_locked("aqueduct_flood", "inunriver_rcp4p5_gfdl_2050_rp0100", "input/")
```

```bash
irv-datapkg lock --base s3://bucket/irv-datapkg --release 2026.1 aqueduct_flood
irv-datapkg lock --base s3://bucket/irv-datapkg --release 2026.1 -p aqueduct_flood:return_period=100
irv-datapkg lock --base s3://bucket/irv-datapkg --release 2026.1 aqueduct_flood --check
irv-datapkg fetch aqueduct_flood --lock datapkg.lock.yaml -p return_period=100 -o input/ --link hardlink
irv-datapkg extract gridfinder --lock datapkg.lock.yaml --geometry region.geojson -o extracts/
irv-datapkg validate s3://bucket/irv-datapkg --check-files
irv-datapkg validate workspace/catalogue --deep         # + STAC schemas, COG/GeoParquet profiles
irv-datapkg audit s3://bucket/irv-datapkg --sample 100  # re-hash files, report mismatches
irv-datapkg release s3://bucket/irv-datapkg 2026.2 --from 2026.1 --pin aqueduct_flood=v2-r2
```

Private datasets are read from `{BASE}-private` using the same credentials. The
download cache defaults to `~/.cache/irv-datapkg` (set `IRV_DATAPKG_CACHE` to
change it). Producers write the catalogue with `build_collection`, `build_item`
and `CatalogWriter` (`add_collection`, `add_item`, `release`, `retire`).

Quality checks, the checksum audit and the end-to-end CI test are described in
[`docs/qa.md`](docs/qa.md); the plan for retiring consumers' download rules is in
[`docs/deprecation.md`](docs/deprecation.md).

The client installs without GDAL Python bindings (`pip install irv-datapkg`).
The producer workflow also needs `pip install irv-datapkg[producer]`, or GDAL
and cdsapi from conda-forge as in `environment.yml`.

### Development Notes

In case of warnings about `GDAL_DATA` not being set, try running:

```bash
export GDAL_DATA=$(gdal-config --datadir)
```

To format the workflow (snakefmt 2.x; settings in pyproject.toml):

```bash
snakefmt Snakefile rules
```

To format the Python code (100 columns, from `[tool.black]` in pyproject.toml):

```bash
black src tests scripts
```

Tests: `pytest` (everything), `pytest -m "not e2e"` (fast unit tests) or
`pytest -m e2e` (Snakemake end-to-end runs on synthetic data, docs/qa.md).

### Related work

These Python libraries may be a useful place to start analysis of the data in
the packages produced by this workflow:

- [`snkit`](https://github.com/tomalrussell/snkit) helps clean network data
- [`nismod-snail`](https://github.com/nismod/snail) is designed to help
  implement infrastructure exposure, damage and risk calculations

The [`open-gira`](https://github.com/nismod/snail) repository contains a larger
workflow for global-scale open-data infrastructure risk and resilience analysis.

## Acknowledgments

> MIT License, Copyright (c) 2023 Tom Russell and irv-datapkg contributors

This research received funding from the FCDO Climate Compatible Growth
Programme. The views expressed here do not necessarily reflect the UK
government's official policies.
