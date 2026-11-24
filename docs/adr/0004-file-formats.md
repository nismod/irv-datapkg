# 0004 File formats

Status: accepted

| Kind | Standard |
| --- | --- |
| Raster | COG, source CRS and grid, ZSTD or DEFLATE + predictor, 512 px tiles, overviews, explicit nodata, band descriptions, units and scale/offset in metadata |
| Vector | GeoParquet 1.1, EPSG:4326, bbox covering column, Hilbert-sorted, ~100k-row groups; one file per layer, or for large layers (OSM) Hive-partitioned on the adaptive spatial key of ADR 0005 |
| Multi-dimensional | Zarr v3 with CF conventions, only where time/ensemble dimensions matter |
| Tabular | Parquet; CSV only for small lookups |

Partitioning by country is rejected: partition sizes are far too uneven.
