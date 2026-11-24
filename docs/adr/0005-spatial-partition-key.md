# 0005 Spatial partition key

Status: accepted

## Decision

- An adaptive partition key is developed as its **own small library**, so it
  can be reused outside irv-datapkg.
- Method: start from a coarse grid and repeatedly subdivide the most populous
  cell until there are about **200 cells** of mixed resolution covering the globe.
- Population source: **GHSL POP, epoch 2020, coarsest available resolution**
  (R2023A, 30 arcsec / 1 km).
- Implement both **quadkey** (default, because it fits rectangular bbox
  filtering) and **H3**. Other DGGS are future work.
- Publish the list of cells with bbox and boundary geometry
  (`{BASE}/partitions/{scheme}/cells.parquet`) as catalogue metadata.
- Review prior art before writing the method.

Implementation: `packages/adaptive-partitions/` (see its README and `docs/prior-art.md`).
