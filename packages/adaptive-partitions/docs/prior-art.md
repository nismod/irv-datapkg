# Prior art: spatial partitioning for large GeoParquet layers

How this review was done: a short web search in November 2026, with some
gaps filled from background knowledge. Some project sites (geoparquet.org,
the geoparquet-io guide) could not be fetched from the build environment, so
what is said about them comes from search-result summaries. Check the links
before quoting details.

## The problem

A global OSM-derived layer such as buildings, roads or POIs is too large for
one file. Readers mostly do bbox queries, either one country or region or a
window around an asset. The goals for the partition are:

1. similar partition sizes (no 10 GB China file next to a 1 kB Nauru file);
2. a stable, published key, so that every release of every layer uses the
   same partitions and consumers can pre-compute which keys they need;
3. cheap pruning for bbox queries, at the directory level (Hive) and again
   at the row-group level (GeoParquet 1.1 bbox covering + Hilbert sort);
4. full global coverage with an unambiguous rule for each feature.

## Approaches seen in practice

**Administrative partitions.** The "admin-partitioned GeoParquet
distribution" of Google/Microsoft building footprints splits by country
(and by admin-1 for big countries) ([Cloud Native Geo blog,
2023](https://cloudnativegeo.org/blog/2023/10/the-admin-partitioned-geoparquet-distribution/)).
Users understand it, but sizes vary by orders of magnitude, borders change,
and oceans/disputed areas need special cases. ADR 0004 rejects this as the
primary key. Country *extracts* (ADR 0006) remain a separate product.

**Fixed-precision geohash (Overture Maps).** Overture's Sedona pipeline
computes two geohashes. A coarse one (precision 3, ~156 km) is used to
repartition into files, and a fine one (precision 8) sorts rows within files
so row groups can be skipped
([Wherobots](https://wherobots.com/overture-maps-data-cloud-native-geoparquet-apache-sedona/),
[Wherobots](https://wherobots.com/wherobots-provides-ready-to-use-spatially-indexed-overture-maps/)).
Overture's public layout itself is Hive-partitioned by `theme`/`type`, with
files of bounded size inside. A fixed precision has the same unevenness
problem as countries, only milder. The coarse-key-for-files plus
fine-sort-for-row-groups pattern is exactly what ADR 0004 adopts (adaptive
key for files, Hilbert sort within).

**Data-adaptive trees in distributed engines (Apache Sedona / GeoSpark).**
Sedona's spatial partitioners are a uniform grid, a quadtree and a KDB-tree,
built from a *sample of the data*. The KDB-tree depends on insertion order.
The quadtree is deterministic, which is why some follow-up work prefers it
([SOLAR, arXiv 2504.01292](https://arxiv.org/pdf/2504.01292);
[GeoSpark paper](https://jiayuasu.github.io/publication/y2019-geospark-geoinformatica);
[R `apache.sedona` reference](https://search.r-project.org/CRAN/packages/apache.sedona/apache.sedona.pdf)).
These are close to our method (split the heaviest cell), but the tree is
fitted to each dataset and so differs between layers and releases. We want
one key for all layers, so we fit it to a proxy for data density
(population) instead of to each dataset. Sedona's docs also suggest
sorting by geohash before writing GeoParquet for filter pushdown
([Sedona GeoParquet tutorial](https://sedona.apache.org/1.7.0/tutorial/files/geoparquet-sedona-spark)).

**GeoParquet tooling (geoparquet-io / gpio).** It offers Hilbert sorting,
bbox covering columns, and partitioning by admin boundary, H3, quadkey or
KD-tree, with row groups sized by rows or bytes
([geoparquet-io intro](https://cloudnativegeo.org/blog/2026/03/introducing-geoparquet-io/),
[partition guide](https://cholmes.github.io/geoparquet-io/guide/partition),
[spatial indices concepts](https://geoparquet.org/geoparquet-io/concepts/spatial-indices)).
The community guidance is: sort only for files under ~10 GB, partition
beyond that, and aim for row groups of ~50-100 MB compressed. The bbox
covering is only useful once the data are spatially sorted
([GeoParquet discussion #188](https://github.com/opengeospatial/geoparquet/discussions/188)).

**Space-filling curves for extended objects (GeoMesa XZ2, Iceberg).**
GeoMesa indexes points with Z2 (a Z-order curve on lon/lat) and
lines/polygons with XZ2. XZ2 is the XZ-ordering of Böhm, Klump & Kriegel
(1999), which gives each extended object exactly one key by using enlarged
cells, so objects that cross cell edges are handled without duplication
([GeoMesa index overview](https://www.geomesa.org/documentation/3.2/user/datastores/index_overview.html),
[XZ2SFC javadoc](https://www.javadoc.io/static/org.locationtech.geomesa/geomesa-z3_2.11/2.2.3/org/locationtech/geomesa/curve/XZ2SFC.html),
[Böhm et al. 1999](https://www.dbs.ifi.lmu.de/Publikationen/Boehm/Ordering_99.pdf)).
The Apache Iceberg geo work considered XZ2 as a partition transform for the
same reason ([Iceberg dev list](https://mail-archive.com/dev@iceberg.apache.org/msg06271.html),
[Wherobots on Iceberg geo](https://wherobots.com/blog/iceberg-geo-technical-insights-and-implementation-strategies/),
[CARTO on Iceberg v3 geometry](https://carto.com/blog/iceberg-v3-geometry-one-year-later/)).
Our representative-point rule is the simpler alternative to the same
"boundary object" problem. It keeps a 1:1 mapping, but bbox queries must
allow for features that reach outside their cell (see `keys_for_bbox`).

**Clustering on a DGGS cell id (BigQuery, Snowflake, Databricks).**
BigQuery cannot *partition* on GEOGRAPHY. It *clusters* by mapping
geographies to S2 cell ids
([Google Cloud blog](https://cloud.google.com/blog/products/data-analytics/best-practices-for-spatial-clustering-in-bigquery?hl=en)).
Lakehouse users often Z-order or cluster on H3/S2 ids. This is
fine-grained ordering, not a coarse directory key.

## Discrete global grids

* **Geographic quadtree / WorldCRS84Quad.** The OGC 2D Tile Matrix Set
  standard defines `WorldCRS84Quad`: 2 x 1 tiles of 180 degrees at level 0,
  halving at each level, covering the whole globe in CRS84
  ([OGC 17-083r4](https://docs.ogc.org/is/17-083r4/17-083r4.html)). Cells
  nest exactly and every edge is a meridian or parallel, so a cell is its
  own bbox. Cells are not equal-area: area falls off with cos(latitude).
* **Web Mercator quadkeys (Bing).** These are the familiar quadkey naming,
  but only cover |lat| < 85.05 degrees, so they fail the full-coverage
  requirement (Antarctic and Arctic features would need a special bucket).
* **H3.** Hexagonal, aperture 7. 122 base cells (12 pentagons), and
  children do *not* nest geometrically. Widely used for analytics, and
  global population products are already published on H3 (e.g. [Kontur
  Population](https://data.humdata.org/dataset/kontur-population-dataset),
  derived partly from GHSL).
* **S2.** A cube-face quadtree on the sphere with exact nesting, 6 faces at
  level 0, used by BigQuery. Not implemented here, but structurally like the
  quadkey scheme.
* **A5.** A newer equal-area pentagonal DGGS on a dodecahedron, with 31
  resolutions and 64-bit ids. It has DuckDB and R bindings
  ([GitHub](https://github.com/felixpalmer/a5),
  [DuckDB extension](https://www.duckdb.org/community_extensions/extensions/a5)).
  It is a good candidate for future work (ADR 0005), because equal area
  suits population weighting.

## What we take from this

* Partition on a **published, data-independent key**, fitted once to
  population (as a proxy for OSM feature density), not refitted per layer
  like Sedona's KDB-tree.
* **Adaptive quadtree** splitting (as in Sedona's quadtree partitioner) on
  the **WorldCRS84Quad** grid, so the key is deterministic, covers the
  globe, aligns with lon/lat bboxes and has standard tile semantics
  (`level`, `x`, `y`).
* Keep fine-grained ordering (Hilbert sort + bbox covering) *inside*
  partitions, as Overture and the GeoParquet guidance do.
* Assign each feature to exactly one partition by representative point, and
  document how to pad queries. XZ-ordering is the alternative if exact
  object containment becomes important.
