# 0003 Catalogue and releases

Status: accepted

## Decision

- The contract between producer and consumers is a **static STAC catalogue**
  plus immutable files under `{BASE}`:

  ```
  {BASE}/
    catalog.json                      # STAC root
    releases/YYYY.N/catalog.json      # release manifest
    partitions/{scheme}/cells.parquet # spatial partition cell lists (ADR 0005)
    {dataset}/collection.json         # licence, citation, processing
    {dataset}/{version}/{item}.json   # STAC item per file/variant
    {dataset}/{version}/{item}.tif|.parquet|.zarr
    extracts/{region}/{pkg_version}/  # T2
    results/open-gira/{run}/          # published analysis outputs
  {BASE}-private/                     # same layout, authenticated
  ```

- `{version}` = upstream version + processing revision, e.g. `v2-r1`.
  Published version directories are immutable.
- Each asset records size and sha256 (STAC `file` extension), source URL,
  retrieval date and the irv-datapkg commit that produced it.
- Scenario dimensions (hazard, rcp/ssp, epoch, gcm, return period) are STAC
  item properties, not parsed from file names.
- A **release** is made every **six months**. It is a manifest that pins one
  version of each dataset by reference, so a dataset can be added without
  bumping or copying anything else.
- Consumers lock to a release (`datapkg.lock.yaml`), not to "latest".
