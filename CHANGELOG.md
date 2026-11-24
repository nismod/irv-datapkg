# Changelog

## Unreleased (planned 0.3.0)

- Item property filters compare numbers numerically, so `100`, `"100"` and
  `100.0` match each other.
- Published analysis results (`docs/catalogue-spec.md` section 8): the
  `results/open-gira/{run}/` contract. A run root is an ordinary catalogue root
  (one collection per output layer, version = run id) with run metadata
  (`irv:producer`, `irv:run`, `irv:commit`, `irv:config_hash`,
  `irv:input_lock`). `validate` checks it against the new `results_run` schema;
  lock files may pin a `run` instead of a `release`.

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
