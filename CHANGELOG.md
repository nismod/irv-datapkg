# Changelog

## Unreleased (planned 0.3.0)

### Added: QA, CI and deprecation plan (WP14)

- `validate_catalog(deep=True, sample=, seed=, datasets=, warnings=)` and
  `irv-datapkg validate --deep [--sample N --seed S] [-d DATASET] [--strict]`:
  STAC 1.0.0 core + extension schema validation with bundled schemas
  (`catalog/stac_schemas/`, works offline; unbundled extensions are fetched
  or skipped with a warning), COG checks (rio-cogeo validator, compression,
  tiling, overviews, declared nodata and data type), GeoParquet checks
  (version, EPSG:4326, `covering.bbox`, geometry types, row groups, row
  count) and sha256. `validate` also takes remote options (`-S`,
  `--endpoint-url`, `--profile`, `--anon`). See docs/qa.md.
- `irv-datapkg audit BASE|LOCK [--sample N] [--release R] [-d DATASET] [-j N] [-o CSV]`
  re-hashes assets (local or remote, or a lock file's assets against its base
  or `--base MIRROR`) and reports mismatched, missing and unreadable files
  (`irv_datapkg.catalog.qa.audit`).
- `all_t1` and `catalogue_publish` deep-validate with `--config validate_deep=true`.
- `qa` extra (`rio-cogeo`); `dev` now includes rio-cogeo and black.
  `[tool.black]` (100 columns) and `[tool.snakefmt]` (88) in pyproject.toml.
- `pytest -m e2e` (tests/test_e2e.py): synthetic T0 -> T1 Snakemake run, deep
  validation, release, publish to `file://`, audit, consumer lock and fetch.
- CI proposals in `ci/` (copy to `.github/workflows/`, see ci/README.md):
  `test.yml` (unit, e2e, lint jobs) and `linkcheck.yml`.
- docs/deprecation.md: every consumer download rule, the dataset replacing
  it, and when it may be removed.

- Item property filters compare numbers numerically, so `100`, `"100"` and
  `100.0` match each other.

### Added: catalogue client and producer (`irv_datapkg.catalog`, WP1)

- Static STAC catalogue contract (`docs/catalogue-spec.md`, JSON Schema in
  `irv-catalogue-schema.json`): collections, items, release manifests and
  consumer lock files, on any fsspec root (local, `file://`, `s3://`, `https://`),
  with a private `{BASE}-private` root.
- `Catalog.open(base, release=...)`: datasets, collections, versions, items
  filtered by dimension properties, GDAL paths for windowed reads (`href`),
  sha256-verified cached downloads (`fetch`) and clipping (`extract`).
- Producer side: `build_collection`, `build_item`, `CatalogWriter`
  (`add_collection`, `add_item`, `release`); published versions are immutable
  and sealed once a release pins them. `validate_catalog`.
- `irv-datapkg` CLI: `fetch`, `lock`, `validate`, `extract`, `release`,
  `publish`, `retire`, `linkcheck`.

### Added: follow-up from consumer integrations (open-gira, infra-risk-vis)

- Mirrors and portable locks: `Catalog.from_lock(path, base=None, private_base=None)`
  overrides the recorded base. Lock files record `base` as given (relative
  paths are relative to the lock file; `file://` kept) and `private_base`
  only when it is not `{base}-private`. Asset hrefs stay relative to the base.
- `Catalog.fetch(obj, dest=None, link="copy"|"hardlink"|"symlink", cache=True)`:
  link from the cache instead of copying, or `cache=False` to stream straight
  to `dest` with sha256/size verified while streaming and an atomic rename.
  Cached files are now read-only.
- `Catalog.fetch_locked(dataset, item_id, dest=None, key=None, ...)` and
  `Catalog.locked_asset(...)` fetch from a lock entry alone (no STAC reads);
  `LockFile` parses lock files (`select`, `entry`, `pins`). `irv-datapkg fetch
  --lock` uses it, with `--link` and `--no-cache`.
- `Catalog.item()` reads `{dataset}/{version}/{item_id}.json` directly.
  Catalogues opened from a lock list only the locked items.
- `items(..., strict=True)` rejects filter keys not in `irv:dimensions`;
  `None` / `Catalog.ABSENT` matches a property that is absent or null.
- Selective locks: `lock()`/`write_lock(datasets, select={dataset: ids or
  filters}, base=...)`; `irv-datapkg lock --item DATASET/ID -p DATASET:KEY=VALUE
  --record-base`. Locks also record each item's dimension `properties` and
  the asset `source_filename`.
- `Catalog.lock_matches(lock_path, datasets=None, release=None, select=None)`
  returns a truthy/falsy `LockDiff` with `problems`; `irv-datapkg lock --check`.
- Provenance: optional asset field `irv:source_filename` (upstream file name;
  `build_item(source_filename=None)` defaults to the basename of
  `irv:source_url`); `Asset.source_url`, `Asset.source_filename`.
- `Catalog.relative_href(obj)`, `Catalog.cache_path(obj)`,
  `Catalog.dimensions(dataset)`, `Catalog.retirement(dataset, version)`.
- Retirement: `irv:retired` in `collection.json`
  (`{version: {date, note, location}}`); `CatalogWriter.retire()` /
  `unretire()` and `irv-datapkg retire --mark`. Clients raise `RetiredError`
  for retired versions; `validate` accepts their missing files; releases
  refuse to pin them; `publish` does not re-upload their files; the
  retirement report shows a `retired` column.
