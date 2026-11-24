# irv-datapkg catalogue specification

Status: draft for WP1. Implements ADRs 0002, 0003 and 0004. The machine-readable
part is [`src/irv_datapkg/catalog/irv-catalogue-schema.json`](../src/irv_datapkg/catalog/irv-catalogue-schema.json)
(JSON Schema 2020-12, one `$defs` entry per document kind: `collection`, `item`,
`release`, `lock`). `irv-datapkg validate` checks the schema plus the layout rules below.

All documents are STAC 1.0.0 JSON. irv-specific fields use the `irv:` prefix.
The terms MUST and MUST NOT are requirements; everything else is guidance.

## 1. Layout

Everything lives under one base URI `{BASE}`: a local path, `file://`, `s3://`,
`https://` or any other fsspec URL. Locations differ only in `{BASE}`.

```
{BASE}/
  catalog.json                        # STAC root: child -> collections, irv:release -> releases
  releases/YYYY.N/catalog.json        # release manifest (section 5)
  partitions/{scheme}-v{N}/cells.parquet # immutable partition cells, ADR 0005 (not yet produced)
  {dataset}/collection.json           # one collection per dataset, all versions
  {dataset}/{version}/{item}.json     # STAC item, one per file/variant
  {dataset}/{version}/{file}.tif|.parquet|.zarr
  extracts/{region}/{pkg_version}/    # T2 extracts (not yet produced)
  results/open-gira/{run}/            # published analysis outputs (not yet produced)
{BASE}-private/                       # same layout, authenticated
  catalog.json
  {dataset}/collection.json ...
```

- `{BASE}-private` is the string `{BASE}` with `-private` appended
  (`s3://bucket/irv` -> `s3://bucket/irv-private`). Clients MAY override it.
- A private dataset lives only under `{BASE}-private` and MUST NOT appear in the
  public root catalogue. Releases live only under the public `{BASE}` and refer
  to private datasets by id (section 5).
- All hrefs inside documents are relative, so a catalogue can be copied or
  mirrored without rewriting. An asset file sits beside its item JSON
  (`"href": "./name.tif"`).
- `partitions/`, `extracts/` and `results/` are reserved now; their content is
  specified by later work packages.

## 2. Identifiers and versions

| Name | Rule | Example |
| --- | --- | --- |
| dataset id | `^[a-z][a-z0-9_]*$`, from `docs/datasets.csv` | `aqueduct_flood` |
| version | `{upstream}-r{N}`: `^[A-Za-z0-9][A-Za-z0-9._-]*-r[1-9][0-9]*$` | `v2-r1`, `R2023A-r2`, `planet-250414-r1` |
| item id | `^[a-z0-9][a-z0-9_.-]*$`, unique within a version | `inunriver_rcp4p5_gfdl_2050_rp0100` |
| release | `YYYY.N` | `2026.1` |

- `{upstream}` is the upstream version string as published. For unversioned
  sources, use the retrieval date `YYYYMMDD`.
- `-rN` is the irv-datapkg processing revision. Bump it whenever output bytes
  or metadata change for the same upstream version.
- A published `{dataset}/{version}/` directory is **immutable**. The writer
  refuses to replace a file or item with different content. Re-writing
  identical content is a no-op. Fixes go out as a new revision.
- Once a release pins a version, that version is **sealed**: no new items can
  be added to it either.
- A version whose files have moved to cold storage is listed in the
  collection's `irv:retired` (section 7). Its item JSON stays.
- `collection.json` is the only mutable per-dataset document. It gains links,
  versions and retirements as they happen. `irv:versions` lists versions oldest
  first, and the last one is "latest".

## 3. Collection (`{dataset}/collection.json`)

Required in addition to STAC Collection fields:

| Field | Meaning |
| --- | --- |
| `license` | SPDX identifier (`CC-BY-4.0`, `ODbL-1.0`), else `proprietary`/`other` with a `license` link |
| `sci:citation` | Preferred citation (STAC scientific extension). `sci:doi` if available |
| `irv:upstream_url` | Where the upstream data comes from (URL, or description such as `Zenodo 10.5281/...`) |
| `irv:upstream_version` | Upstream version as published |
| `irv:access` | `public` or `private`. MUST match the root it is under |
| `irv:processing` | Ordered list of processing steps, in plain words |
| `irv:dimensions` | Names of item properties that distinguish variants. Every item MUST have them |
| `irv:versions` | Published versions, oldest first |
| `irv:retired` | Optional. Versions whose files are in cold storage: `{version: {"date": "YYYY-MM-DD", "note": ..., "location": ...}}` (section 7) |

```json
{
  "type": "Collection", "stac_version": "1.0.0", "id": "aqueduct_flood",
  "title": "WRI Aqueduct flood hazard maps", "description": "...",
  "license": "CC-BY-4.0",
  "sci:citation": "Ward, P.J. et al. 2020. Aqueduct Floods Methodology. WRI.",
  "irv:access": "public",
  "irv:upstream_url": "s3://wri-projects/AqueductFloodTool/download/v2",
  "irv:upstream_version": "v2",
  "irv:processing": ["convert to COG (ZSTD, 512 px tiles, overviews)", "set nodata 0"],
  "irv:dimensions": ["hazard", "rcp", "epoch", "gcm", "return_period"],
  "irv:versions": ["v2-r1"],
  "extent": {"spatial": {"bbox": [[-180, -90, 180, 90]]}, "temporal": {"interval": [[null, null]]}},
  "links": [{"rel": "item", "href": "./v2-r1/inunriver_rcp4p5_gfdl_2050_rp0100.json"}]
}
```

## 4. Item (`{dataset}/{version}/{item}.json`) and assets

Item properties:

- `irv:dataset`, `irv:version`: MUST equal the collection id and the version
  directory.
- Variant dimensions are plain properties, named in `irv:dimensions`, never
  parsed from file names. Use these names where they apply: `hazard`, `rcp`
  (string, e.g. `"4.5"`), `ssp` (e.g. `"ssp245"`), `epoch` (integer year or
  `"baseline"`), `gcm`, `return_period` (integer years). Add other names as needed.
- `datetime`: the data's nominal time if it has one, else the retrieval date.
  Scenario epochs go in `epoch`, not `datetime`.
- `proj:epsg` (or `proj:wkt2`), plus `proj:shape`/`proj:transform` for rasters.
  T1 rasters keep the source grid (ADR 0001).

Each asset (role `data` for the primary file) MUST have:

| Field | Meaning |
| --- | --- |
| `file:size` | Bytes (STAC file extension) |
| `file:checksum` | sha256 as a multihash: `1220` + 64 hex digits |
| `irv:source_url` | Upstream URL of the T0 file(s) it was derived from |
| `irv:retrieved` | Retrieval date `YYYY-MM-DD` |
| `irv:commit` | irv-datapkg git commit that produced it (7-40 hex) |
| `irv:source_filename` | Optional. Upstream file name, e.g. `inunriver_rcp4p5_0000GFDL-ESM2M_2050_rp00100.tif`. `build_item` sets it to the basename of `irv:source_url` when that looks like a file; pass `source_filename=` otherwise. Consumers use it to name fetched files as the upstream did |
| `raster:bands` | Rasters only: each band has `nodata`, `data_type` and `unit` (`null` if unitless), plus `scale`/`offset` |

Vector assets carry `table:row_count`, `table:columns` and `table:primary_geometry`.

```json
{
  "type": "Feature", "stac_version": "1.0.0", "id": "inunriver_rcp4p5_gfdl_2050_rp0100",
  "collection": "aqueduct_flood", "bbox": [-180, -90, 180, 90], "geometry": {"type": "Polygon", "...": "..."},
  "properties": {
    "datetime": "2025-06-01T00:00:00Z",
    "irv:dataset": "aqueduct_flood", "irv:version": "v2-r1",
    "hazard": "river", "rcp": "4.5", "epoch": 2050, "gcm": "gfdl", "return_period": 100,
    "proj:epsg": 4326
  },
  "assets": {
    "data": {
      "href": "./inunriver_rcp4p5_gfdl_2050_rp0100.tif",
      "type": "image/tiff; application=geotiff; profile=cloud-optimized", "roles": ["data"],
      "file:size": 123456789, "file:checksum": "1220ab12...",
      "irv:source_url": "http://wri-projects.s3.amazonaws.com/AqueductFloodTool/download/v2/inunriver_rcp4p5_0000GFDL-ESM2M_2050_rp00100.tif",
      "irv:source_filename": "inunriver_rcp4p5_0000GFDL-ESM2M_2050_rp00100.tif",
      "irv:retrieved": "2025-06-01", "irv:commit": "879cd80",
      "raster:bands": [{"data_type": "float32", "nodata": -9999, "unit": "m", "scale": 1.0, "offset": 0.0}]
    }
  }
}
```

## 5. Release manifest (`releases/YYYY.N/catalog.json`)

A release is a STAC Catalog that **pins one version of each dataset by
reference**. Making a release writes only this one file. No data or item is
copied, so a release can add or bump one dataset and leave the rest untouched.
Releases are immutable. They are made every six months, or more often when needed.

```json
{
  "type": "Catalog", "stac_version": "1.0.0", "id": "irv-datapkg-2026.1",
  "description": "irv-datapkg release 2026.1",
  "irv:release": "2026.1", "irv:created": "2026-06-30T12:00:00Z",
  "irv:datasets": {
    "aqueduct_flood": {"version": "v2-r1", "access": "public"},
    "iris": {"version": "early-r1", "access": "private"}
  },
  "links": [
    {"rel": "root", "href": "../../catalog.json"},
    {"rel": "child", "href": "../../aqueduct_flood/collection.json", "title": "aqueduct_flood v2-r1"}
  ]
}
```

- `irv:datasets` is authoritative. `child` links are there for STAC browsers and
  exist only for public datasets.
- Every pinned version MUST be in that collection's `irv:versions`. A new
  release MUST NOT pin a retired version (old releases that pin it stay valid).
- The root catalogue links each release with `rel: "irv:release"`.
- `irv-datapkg release BASE 2026.2 --from 2026.1 --pin aqueduct_flood=v2-r2`
  starts from the previous release's pins. Without `--from`, every dataset is
  pinned to its latest version.

## 6. Consumer lock file (`datapkg.lock.yaml`)

Consumers (open-gira, infra-risk-vis) commit a lock file and never follow
"latest". The lock file records the release and, for each dataset used, the
pinned version and every locked asset's location and checksum. The client
refuses an item whose checksum differs from the lock file.

```yaml
lock_version: 1
base: s3://example-bucket/irv-datapkg       # as given: URL, absolute path, or relative path
private_base: s3://example-bucket/irv-datapkg-private   # only if not {base}-private
release: '2026.1'
datasets:
  aqueduct_flood:
    version: v2-r1
    access: public
    items:
      inunriver_rcp4p5_gfdl_2050_rp0100:
        data:
          href: aqueduct_flood/v2-r1/inunriver_rcp4p5_gfdl_2050_rp0100.tif   # relative to base (or private_base)
          sha256: ab12...
          size: 123456789
          source_filename: inunriver_rcp4p5_0000GFDL-ESM2M_2050_rp00100.tif   # optional
    properties:              # optional: each item's irv:dimensions values (null if absent)
      inunriver_rcp4p5_gfdl_2050_rp0100: {hazard: river, rcp: '4.5', gcm: gfdl, epoch: 2050, return_period: 100}
```

- `base` and `private_base` are recorded as given. A relative local path is
  relative to the lock file's directory; `file://` URLs are kept. When
  `private_base` is absent it is `{base}-private`.
- Asset `href`s are relative to the base, so a lock works unchanged against a
  mirror: `Catalog.from_lock(path, base=MIRROR)` (private base then
  `MIRROR-private` unless `private_base=` is given), or `--lock ... --base MIRROR`.
- A lock MAY list only some items of a dataset (`write_lock(select=...)`,
  `irv-datapkg lock --item DATASET/ID` or `-p DATASET:KEY=VALUE`). A catalogue
  opened from a lock lists only the locked items.
- A locked asset can be fetched from its entry alone, with no STAC reads
  (`Catalog.fetch_locked`, `irv-datapkg fetch --lock`); `properties` lets
  the client select items by dimension without reading item JSON.
- Older lock files (absolute `base`, always a `private_base`, no
  `properties`/`source_filename`) remain valid.

Create it with `irv-datapkg lock --base {BASE} --release 2026.1 aqueduct_flood ...`,
check it with `irv-datapkg lock ... --check` (or `Catalog.lock_matches`), and
read it with `Catalog.from_lock("datapkg.lock.yaml")` or `--lock` on any CLI command.

## 7. Retired versions

A version's files may move to cold storage (docs/storage.md section 6). The
collection then records it:

```json
"irv:retired": {
  "v1-r1": {"date": "2027-07-01", "note": "On Oxford tape; ask irv-datapkg maintainers to restore", "location": "tape LTO-0042"}
}
```

- Every key MUST be in `irv:versions`. `date` is required; `note` and
  `location` are free text.
- The version's item JSON and its `irv:versions` entry stay, so citations and
  old release manifests still resolve to metadata. Its asset files MAY be
  missing; `validate` does not report them.
- Clients MUST refuse to fetch (or hand out GDAL paths for) a retired version
  with an error that gives the date and note, and lock files MUST NOT be
  written for it. `CatalogWriter` refuses to add items to it or pin it in a
  new release.
- Mark with `CatalogWriter.retire(dataset, version, note)` or
  `irv-datapkg retire BASE --mark DATASET=VERSION --note ...`. After a
  restore, `CatalogWriter.unretire` removes the entry.
