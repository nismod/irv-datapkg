# Quality assurance: deep validation, checksum audit, end-to-end test

Status: WP14. Complements `irv-datapkg validate` (catalogue-spec.md: schema and
layout rules) with checks of the data files themselves, a checksum audit for
storage, and a synthetic end-to-end release run in CI.

| Tool | Checks | Reads |
| --- | --- | --- |
| `irv-datapkg validate BASE` | irv JSON Schema, layout, ids, versions, release pins, file sizes | JSON + file sizes |
| `... --check-files` | + sha256 of every asset | all data |
| `... --deep` | + STAC core and extension schemas, COG and GeoParquet profiles (ADR 0004), sha256 | all data (or `--sample N`) |
| `irv-datapkg audit BASE` or `audit datapkg.lock.yaml` | size and sha256 of every asset (or a sample), as a report | all data (or `--sample N`) |
| `pytest -m e2e` | T0 -> T1 Snakemake run, deep validation, release, publish, audit, consumer lock + fetch | synthetic files only |

## Deep validation

```bash
irv-datapkg validate workspace/catalogue --deep                # local catalogue
irv-datapkg validate workspace/catalogue --deep -d aqueduct_flood  # one dataset
irv-datapkg validate s3://bucket/irv-datapkg --deep --sample 200 --seed 1 \
    --endpoint-url https://data.source.coop --anon               # remote, sampled
snakemake -c1 all_t1 --config validate_deep=true                # after a T1 build
```

`validate_catalog(base, deep=True, sample=None, seed=None, datasets=None,
warnings=[])` is the Python API. Errors are returned; warnings (deviations
worth a look) go to the `warnings` list and are printed as `warning:` lines.
Exit status is 1 on errors, or on warnings too with `--strict`.

**STAC.** Every root catalogue, collection, item and release manifest is
validated against the STAC 1.0.0 core schema for its type and the schema of
every entry in `stac_extensions`. The core schemas and the extensions that
`build_item`/`build_collection` use (file v2.1.0, projection v1.1.0, raster
v1.1.0, table v1.2.0, scientific v1.0.0) are bundled in
`src/irv_datapkg/catalog/stac_schemas/`, so this works offline and on
machines that cannot reach schemas.stacspec.org. Any other extension schema
is fetched over HTTPS (10 s timeout); if that fails, the check for that
schema is skipped with a warning naming the URL and the error. When
`producer.EXT` gains or bumps an extension, add its schema to the bundle.

**COG** (`.tif`, ADR 0004):

| Check | Error | Warning |
| --- | --- | --- |
| rio-cogeo `cog_validate` (IFD layout, internal overviews, tiling) | its errors | |
| compression | none | not ZSTD/DEFLATE |
| predictor | | none |
| tiles (images over 512 px) | stripped | not 512 x 512 |
| overviews (images over 512 px) | none | |
| `proj:shape` | differs from the file | |
| `raster:bands` | band count, `data_type`, declared `nodata` not set or different (NaN = NaN) | no nodata declared and none set |

rio-cogeo is in the `qa` and `dev` extras (`pip install 'irv-datapkg[qa]'`).
Without it the rio-cogeo layout check is skipped with a warning and the other
checks still run. Remote COGs are read through GDAL, so for S3 set GDAL's own
variables (`AWS_PROFILE`, `AWS_S3_ENDPOINT`, `AWS_NO_SIGN_REQUEST`, ...);
fsspec `--storage-option`s do not reach GDAL.

**GeoParquet** (`.parquet`, footer only):

| Check | Error | Warning |
| --- | --- | --- |
| `geo` metadata | missing or not JSON | |
| `version` | not 1.0.0 / 1.1.0 | 1.0.0 (ADR 0004: 1.1) |
| `primary_column` | not described, not a column, differs from `table:primary_geometry` | |
| `encoding` | not WKB or a native encoding | |
| `crs` | null (unknown), or not EPSG:4326 / OGC:CRS84 (absent = CRS84) | |
| `geometry_types` | missing, unknown type | empty (any type) |
| `covering.bbox` | missing, or refers to columns that do not exist | |
| `table:row_count` | differs from the file | |
| row groups | | any over 1M rows, or (except the last) under 10k rows |

**Sampling.** `--sample N` (with `--seed` for a repeatable choice) limits the
file checks (sha256 and format) to N assets picked at random across the whole
catalogue; JSON documents are still all checked. Use it for spot checks of a
large remote catalogue. `--dataset` (repeatable) limits everything to the
named datasets.

## Checksum audit

```bash
irv-datapkg audit /mnt/oxford/irv-datapkg/catalogue -o audit.csv -j 8   # production
irv-datapkg audit s3://bucket/irv-datapkg --release 2026.1 --sample 500 \
    --endpoint-url https://data.source.coop --anon -o audit-remote.csv  # remote
irv-datapkg audit datapkg.lock.yaml [--base MIRROR]                     # a consumer's lock
```

`audit` walks the public and private roots (or the assets of a lock file),
re-hashes each data file with `--jobs` threads and compares size and sha256
with the STAC asset (or the lock entry). Each file gets a status:

| Status | Meaning | Fails the audit |
| --- | --- | --- |
| `ok` | size and sha256 match | |
| `mismatch` | sha256 differs | yes |
| `size` | size differs (not hashed) | yes |
| `missing` | file not found | yes |
| `error` | could not be read (permissions, network) | yes |
| `retired` | missing, but the version is in `irv:retired` (cold storage) | |
| `unchecked` | no `file:checksum` recorded | |

Failures are printed as they are found; `-v` prints every file; `-o` writes a
CSV with every result (`access, dataset, version, item, asset, href, status,
expected/actual size and sha256, detail`). Exit status is 1 if anything
failed. `--release` limits the audit to the versions a release pins,
`--dataset` to named datasets, `--sample N --seed S` to a random sample.

Suggested schedule (docs/storage.md): a full audit of the Oxford drive
catalogue before each release (release procedure step 3 can use
`validate --deep` instead, which includes sha256), a full audit of the
remote after each publish (step 8), and a sampled audit of the remote
monthly, next to the link check.

## End-to-end test (`pytest -m e2e`)

`tests/test_e2e.py` runs the release path on synthetic data with the real code:

1. The `workflow_run` fixture (tests/conftest.py, shared with
   tests/test_workflow.py) fakes T0 for `storm_fixed_rp` and `aqueduct_flood`
   with tiny GeoTIFFs and provenance sidecars, then runs Snakemake for T1
   (COG conversion, STAC registration via the checkpoint for Aqueduct) and a
   Jamaica T2 summary. Nothing is downloaded.
2. `validate_catalog(..., deep=True)` on the local catalogue: no errors and no
   warnings.
3. `irv-datapkg release` (2026.1) and `irv-datapkg publish` to a `file://`
   remote; a second publish is a no-op.
4. `irv-datapkg validate file://... --deep --strict` and `irv-datapkg audit
   --release 2026.1` on the remote.
5. A consumer: `irv-datapkg lock` with a selection, `lock --check`,
   `Catalog.from_lock` + `items` + `fetch`, `fetch_locked` with symlinks,
   `irv-datapkg fetch --lock`, and `irv-datapkg audit datapkg.lock.yaml`.

`pytest` runs everything (the e2e tests are skipped if snakemake is not
installed); `pytest -m "not e2e"` runs the fast unit tests only.

## CI

`ci/test.yml` (to copy to `.github/workflows/test.yml`, see ci/README.md) has
three jobs: unit tests on Python 3.9 and 3.13, the e2e test, and lint (flake8,
black, snakefmt, advisory `snakemake --lint`, and `snakemake -n` dry runs with
dummy `ZENODO_TOKEN`/`CDSAPI_*`). `ci/linkcheck.yml` is the monthly upstream
link check (docs/storage.md section 7). Consumer repositories have their own
proposals: open-gira `ci/datapkg.yml`, infra-risk-vis `ci/etl-datapkg.yml`.
