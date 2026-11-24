# Dataset recipes: how to add a dataset

irv-datapkg produces each dataset in three tiers (ADR 0001):

| Tier | What | Where (config.json key) |
| --- | --- | --- |
| T0 raw mirror | Upstream files as downloaded, plus a provenance sidecar | `raw_dir` = `{workspace_dir}/raw/{dataset}/{upstream}/` |
| (build) | Intermediate files: extracted archives, VRTs, filtered PBFs | `build_dir` = `{workspace_dir}/build/{dataset}/{version}/` |
| T1 analysis-ready | One COG or GeoParquet per variant, each a STAC item in a local catalogue | `catalogue_base` = `{workspace_dir}/catalogue/{dataset}/{version}/` (`{catalogue_base}-private` for private datasets) |
| T2 extracts | Per-country clips of T1, packaged for Zenodo | `data/{ISO3}/` (and `zenodo/`, `zenodo_data/`) |

`workspace_dir` defaults to `workspace` (git-ignored). On the production machine,
point it at the mounted workspace, e.g.
`snakemake --config workspace_dir=/mnt/oxford/irv-datapkg ...`. The other three
keys default to subdirectories of it (`{workspace_dir}` is substituted) and can
be set separately. `catalogue_base` is the local STAC root that publishing
(rules/publish.smk, docs/storage.md) copies to `{BASE}`.

A dataset is one **recipe**: a metadata file and a rules file.

```
docs/datasets.csv                 dataset id, upstream, access (the register of ids)
metadata/<package>.yml            datapackage.json fields + STAC collection(s) + T2 settings
rules/<dataset>.smk               T0 download and T1 standardise rules, item declarations
```

Everything else is generic: rules/common.smk (paths, registry), rules/catalogue.smk
(STAC registration, `all_t1`), rules/t2.smk (extracts, summaries, checksums,
datapackage.json). Helpers are in `irv_datapkg.recipes`.

## Running

```bash
export ZENODO_TOKEN=x CDSAPI_URL=x CDSAPI_KEY=x     # dummy values are fine unless used
snakemake -n all_t1_storm_fixed_rp                  # dry run: one dataset's T1
snakemake -n all_t1                                 # dry run: every dataset's T1
snakemake -c 8 all_t1_storm_fixed_rp                # build and register T1
irv-datapkg validate workspace/catalogue --check-files
snakemake -n data/JAM/datapackage.json              # one T2 country package
snakemake -n all                                    # every country package
```

`all_t1` validates the local catalogue after building. Datasets whose item list
comes from the downloaded files (`aqueduct_flood`, `isimip_heat_drought`) use a
checkpoint: a dry run lists their T0 job only, and their T1/T2 jobs appear once
it has run.

profiles/default/config.yaml (picked up automatically) sets `force-use-threads`,
so `run:` jobs do not each start a new Snakemake process. Keep it: without it the
~45k small registration and extract jobs spend most of their time re-parsing the
workflow.

## Adding a dataset (checklist for a migration issue)

1. **Id.** Find the dataset in `docs/datasets.csv` (add a row if it is new). The
   `id` is the STAC collection id and directory name. Check the licence: anything
   not open goes in with `access: private` (ADR 0002).
2. **Metadata.** Copy `docs/templates/dataset.yml` to `metadata/<package>.yml`.
   Fill the `data_*` fields (they are the T2 datapackage.json resource), one
   `collections:` entry per dataset id (version `{upstream}-r1`, SPDX `license`,
   `dimensions`, `processing` steps in plain words, `units`/`nodata` defaults),
   and `t2:` (`enabled: false` unless the dataset belongs in the country packages).
3. **Rules.** Copy `docs/templates/dataset.smk` to `rules/<dataset>.smk` and add
   an `include:` line in the Snakefile next to the other recipes. Write:
   - `<dataset>_items()`: returns `{item_id: T1Item(...)}`, and call
     `register_t1("<dataset>", <dataset>_items)`. Each item has `ext` (`tif` or
     `parquet`), `properties` (every name in `dimensions`, plus anything useful
     such as `upstream_name`) and `sources` (the T0 files it comes from).
   - a **T0** rule writing into `raw_dir("<dataset>")`, with a provenance sidecar
     (see below). Download verbatim: no unzipping or editing in the raw mirror.
   - a **T1** rule writing `f"{t1_dir('<dataset>')}/{{item}}.tif"` (or `.parquet`)
     with the format helpers. Read archive members in place with
     `recipes.vsizip(...)` where GDAL can; extract to `build_dir()` otherwise.
   - nothing for registration or T2: `t1_register`, `t2_extract` and
     `t2_summary` are generic.
4. **Check without data.** `snakemake -n all_t1_<dataset>`, `snakemake --lint`,
   `snakefmt --check Snakefile rules/` and `pytest` (tests check every metadata
   file against the schema, datasets.csv and the STAC collection schema). If a
   parsing function maps upstream file names to properties, test it on real names
   from the upstream listing.
5. **Build on the production machine.** `snakemake -c N all_t1_<dataset>` then
   `irv-datapkg validate {catalogue_base} --check-files`. Spot-check one item:
   `rio cogeo validate` (or `recipes.is_cog`), grid identical to the source,
   nodata, units, and the item properties.
6. **T2 (only if `t2.enabled`).** `snakemake -c N data/JAM/datapackage.json` and
   compare with the previous package. Then the Zenodo flow is unchanged
   (`all`, `all_uploaded`, `all_published`).

## Conventions

- **Item ids** match `^[a-z0-9][a-z0-9_.-]*$`. Use the lower-cased upstream name
  where there is one (traceable), else short names (`grid`, `lccs_class_2022`).
- **Properties, not file names.** Variant dimensions are item properties
  (catalogue-spec section 4): `hazard`, `rcp` (`"4.5"`, or `"historical"`),
  `ssp`, `epoch` (integer year or `"baseline"`), `gcm`, `return_period`
  (integer years), plus dataset-specific names. Parse upstream file names once,
  in the recipe, never downstream. The T2 summary CSVs are generated from these.
- **Versions** are `{upstream}-r{N}` (catalogue-spec section 2). Bump `N` in the
  metadata file whenever output bytes or metadata change. The writer refuses to
  replace published content.
- **Formats** (ADR 0004): `recipes.to_cog` (ZSTD + predictor, 512 px blocks,
  overviews, nodata/units/descriptions set, source grid kept: `crs`/`nodata`
  only *assign*), `recipes.to_geoparquet`/`write_geoparquet` (GeoParquet 1.1,
  EPSG:4326, bbox covering column, Hilbert-sorted, 100k-row groups). Use
  `overview_resampling="average"` only for continuous fields.

## Provenance sidecars (T0)

Every download writes a JSON sidecar: `<file>.provenance.json` for one file, or
`<dir>/provenance.json` for a bulk download. It records each file's upstream
URL, sha256 and size, the retrieval date and the tool (with version). T1
registration reads it to fill the STAC asset fields `irv:source_url` and
`irv:retrieved`.

- Python rules: `recipes.download(url, dest)` streams over HTTP(S) and writes the
  sidecar; `recipes.write_provenance(...)` records files fetched another way
  (CDS API, ...).
- Shell rules: `irv-datapkg provenance -o provenance.json --tool "aws s3 sync"
  --url-prefix s3://bucket/path FILES...` (or `--source-url`, or `--urls-from
  urls.txt`). `provenance_cmd(...)` in rules/common.smk builds that command.
- `recipes.verify_provenance(sidecar)` re-checks sizes and sha256.

## Helper API (`irv_datapkg.recipes`)

| Helper | Use |
| --- | --- |
| `download(url, dest)` | HTTP(S) download with sidecar |
| `write_provenance(sidecar, files, tool=, url_prefix=/source_url=/urls=)`, `find_provenance(path)`, `verify_provenance(sidecar)` | provenance sidecars |
| `to_cog(src, dst, nodata=, crs=, units=, descriptions=, scale=, offset=, tags=, compress=, overview_resampling=)` | any GDAL raster (GeoTIFF, VRT, ASCII grid, `/vsizip/...`) to COG |
| `netcdf_to_cog(nc, variable, dst, ...)` | one NetCDF variable to COG |
| `build_vrt(tiles, dst, vrt_nodata=)` | mosaic tiles (then `to_cog`) |
| `to_geoparquet(src, dst, layer=)`, `points_to_geoparquet(table, dst, x=, y=)`, `write_geoparquet(gdf, dst)` | vectors to GeoParquet |
| `ogr2ogr_geoparquet_cmd(src, dst, layer)` | shell command for layers too big for memory (OSM) |
| `unzip(archive, dest, members=, flatten=)`, `vsizip(archive, member/basename=)`, `archive_member(s)` | zip/tar handling (refuses path traversal) |
| `is_cog(path)` | ADR 0004 COG checks (empty list = fine) |
| `T1Item(ext, properties, sources, source_url=, datetime=, units=, nodata=, extra=)` | item declaration |
| `register_item(base, collection, item_id, spec)` | build and add a STAC item (used by `t1_register`) |
| `plan`, `extract_region`, `write_summary` | T2 (used by rules/t2.smk) |

Workflow helpers (rules/common.smk): `raw_dir(ds)`, `build_dir(ds)`,
`build_tmpdir(ds)`, `t1_dir(ds)`, `t1_item(ds, item_id)`, `t1_items(ds)`,
`register_t1(ds, fn)`, `provenance_cmd(...)`, `T2_EXTRA[package]` (extra files
for a package, e.g. documentation PDFs).

## T2 settings (`t2:` in metadata)

| Key | Default | Meaning |
| --- | --- | --- |
| `enabled` | | include the resource in every country package |
| `items` | all | item ids to include |
| `filter` | `{}` | `{property: value or [values]}`; values may use `{ISO3}`, `{ADM0_A3}` |
| `exclude_patterns` | `[]` | regular expressions on item ids to leave out |
| `stem` | `{item_id}` | file name `{stem}__{ISO3}.{ext}`; format over item properties |
| `layer` | stem | GeoPackage layer name; several items with one stem become layers of one file |
| `vector_format` | `gpkg` | `gpkg` or `parquet` |
| `clip` | `bbox` | `bbox` (rectangle of the country bounds, as before), `geometry` (mask rasters, keep intersecting features), `none` |
| `summary`, `summary_columns`, `baseline_epoch` | `false` | write `data/{ISO3}/<package>.csv` from item properties; `epoch: baseline` becomes `baseline_epoch` |

## Issue template for a dataset migration

```
Migrate <dataset_id> to a T0/T1(/T2) recipe

- [ ] row in docs/datasets.csv checked (upstream, version, licence, access)
- [ ] metadata/<package>.yml with collections: and t2:
- [ ] rules/<dataset_id>.smk: items, T0 with provenance, T1 standardise; included in Snakefile
- [ ] dry run: snakemake -n all_t1_<dataset_id>; snakemake --lint; snakefmt --check; pytest
- [ ] production: snakemake all_t1_<dataset_id>; irv-datapkg validate --check-files
- [ ] spot checks: COG/GeoParquet profile, grid unchanged, nodata, units, properties
- [ ] consumers: note the item filters open-gira / infra-risk-vis will use
```
