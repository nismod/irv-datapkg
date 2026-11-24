# adaptive-partitions

Population-balanced, adaptive spatial partition keys for Hive-partitioned
GeoParquet. Country partitions give very uneven file sizes; this library
builds one fixed list of about 200 mixed-resolution cells that cover the
globe and roughly balance population. That cell list is the Hive key
(`.../quadkey=q1302/part-0.parquet`).

> **Name.** `adaptive-partitions` (import `adaptive_partitions`) is a
> placeholder until the package moves to its own repository. Suggested
> names: **`popcells`** (short and descriptive) or `geopartition`. The code
> lives in `irv-datapkg/packages/adaptive-partitions/` for now and has no
> dependency on irv-datapkg. See ADR 0005.

## Method

1. **Aggregate** a population raster (GHSL GHS-POP 2020, R2023A) onto a
   regular geographic grid. The raster is streamed in strips of rows, and
   each pixel's value goes to the grid cell containing the pixel centre.
   Projected rasters (the 1 km Mollweide product) are handled by
   transforming pixel centres. A global 30 arcsec raster is never held in
   memory.
2. **Start** from a coarse set of cells covering the globe (quadkey level 1
   = 8 cells; H3 resolution 0 = 122 cells).
3. **Split** the most populous cell into its children, again and again
   (ties are broken by key, so the output is deterministic), until a
   stopping rule fires:
   * `target`: stop when the next split would exceed `target` cells (with
     `allow_overshoot`, stop once `target` is reached or passed);
   * `max_population`: stop when no cell has more than this many people;
   * cells at `max_level` are never split, and splitting stops when only
     empty cells are left.
4. **Publish** the cells (`key`, `scheme`, `level`, `population`, `xmin`,
   `ymin`, `xmax`, `ymax`, `area_km2`, `geometry`, plus a GeoParquet 1.1
   `bbox` covering column) as GeoParquet. Build parameters are stored in the
   file metadata (`adaptive_partitions` key). Per ADR 0003 the file goes to
   `{BASE}/partitions/{scheme}/cells.parquet`.

### Quadkey scheme (default): geographic EPSG:4326 quadtree

**Decision: a geographic quadtree (OGC `WorldCRS84Quad`) with
quadkey-style names, not Web Mercator tiles.**

* Web Mercator quadkeys stop at +/-85.05 degrees latitude, but the cells
  must cover the whole globe (Antarctica, Svalbard, Arctic shipping,
  polar stations). A Mercator scheme would need extra "polar cap" buckets
  that are not part of the tree.
* GeoParquet in this project is EPSG:4326 (ADR 0004). In a geographic
  quadtree every cell edge is a meridian or parallel, so **a cell is
  exactly its lon/lat bbox**. Directory pruning is then a plain
  `xmin <= qxmax and xmax >= qxmin ...` test against the cell table, with no
  projection or densification. Mercator tiles are rectangles only in
  EPSG:3857.
* Cells never cross the antimeridian (the level-0 split is at +/-180 and
  0). Polar cells just have `ymax = 90` or `ymin = -90`.
* It is an OGC standard tile matrix set: level 0 is 2 x 1 tiles of 180
  degrees, so cells also carry `tile_x` / `tile_y` in `WorldCRS84Quad`.
* The cost is that cells are not equal-area (area ~ cos(lat)). That does
  not matter here, because the splits are driven by population, not area.

Key format: `q` + level-0 digit (`0` west, `1` east) + one digit per level
(`0` NW, `1` NE, `2` SW, `3` SE, as in Bing). So `level = len(key) - 2`, and
a key is a prefix of all its descendants' keys. The `q` prefix stops Hive
readers from inferring an integer column and dropping leading zeros
(`0213` would become `213`).

Point rule: cells are closed on their **west and north** edges
(`xmin <= lon < xmax`, `ymin < lat <= ymax`). Longitude 180 wraps to -180,
and latitude -90 goes to the bottom row.

### H3 scheme

* Resolution 0 already has 122 cells (12 of them pentagons), so the default
  is `start_level=0` and `target` must be >= 122. With `target=200` only
  about 13 cells are split (+6 cells per hexagon split, +5 per pentagon), so
  the result is much less balanced than the quadkey scheme. Use a larger
  target (e.g. 400-1000) if you want balance with H3.
* H3 children do not nest geometrically, so mixed-resolution cell
  *polygons* overlap and leave gaps. This library defines membership in
  **index space**. The *reference resolution* `R` is the finest resolution
  in the key list. A point belongs to key `k` if its resolution-`R` cell is
  `k` or a descendant of `k`. The published geometry of `k` is the union of
  its resolution-`R` descendants, so geometries tile the globe exactly and
  agree with `assign()`. Some regions have small detached fragments; that
  is inherent to H3. `R` comes from the key list alone, so no extra
  metadata is needed.
* Antimeridian: cells that cross it are split into a MultiPolygon at +/-180
  (`crosses_antimeridian = True`; their bbox is the conservative
  `-180..180`, so use the geometry for exact tests). Poles: the two cells
  that contain a pole are closed along the pole line (lat +/-90). Edges are
  densified along great circles (more steps near the poles).
* Populations for split decisions are estimated at `max_level` (default 4).
  The published `population` is recomputed at `R`.

### Features that cross cells

`assign()` gives every feature exactly **one** key: the cell containing
its **representative point** (`shapely.point_on_surface`, which always
lies on the geometry). `how="centroid"` and `how="bbox_centre"` are also
available, and `(N, 4)` bbox arrays use their centre. So a long road can
reach beyond its cell. `keys_for_bbox(cells, bbox, buffer=...)` lists the
partitions whose region intersects the query. To get *every* feature
intersecting the query, buffer by your largest expected feature extent, or
read those partitions and filter on the per-row bbox covering. Features
crossing the antimeridian should be split upstream, as OSM extracts usually
are.

## Install

```bash
python -m venv .venv && . .venv/bin/activate
pip install -e "packages/adaptive-partitions[test]"
```

## Usage

```python
import adaptive_partitions as ap

cells = ap.build_cells("GHS_POP_E2020_GLOBE_R2023A_4326_30ss_V1_0.tif",
                       scheme="quadkey", target=200)
ap.write_cells(cells, "partitions/quadkey/cells.parquet")

cells = ap.read_cells("partitions/quadkey/cells.parquet")
gdf["quadkey"] = ap.assign(cells, gdf)                # GeoDataFrame -> keys
keys = ap.assign(cells, lonlat_array)                 # (N, 2) points
keys = ap.keys_for_bbox(cells, (-10, 49, 2, 61))      # GB-ish query
keys = ap.keys_for_bbox(cells, (170, -20, -170, -10)) # crosses antimeridian

# write one Hive partition per key
for key, part in gdf.groupby("quadkey"):
    out = f"osm_roads/quadkey={key}/part-0.parquet"
    part.drop(columns="quadkey").to_parquet(out, write_covering_bbox=True)
```

Lower-level pieces: `ap.aggregate_population(path, level=10)` returns a
`PopulationGrid` that can be reused across schemes and targets.
`ap.cells_from_keys(keys, scheme)` rebuilds geometry from a key list.

### CLI

```bash
adaptive-partitions build RASTER -o cells.parquet [--scheme quadkey|h3] \
    [--target 200] [--start-level N] [--max-level N] [--max-population P] \
    [--allow-overshoot] [--grid-level N] [--block-rows 512]
adaptive-partitions keys cells.parquet --bbox XMIN YMIN XMAX YMAX [--buffer D]
adaptive-partitions show cells.parquet
```

`--grid-level` sets the aggregation grid (default 10 = 0.176 degrees for
quadkey, 11 = 0.088 degrees for H3). It must be >= the quadkey
`--max-level`. Memory is about `2**(2L+1) * 8` bytes (16 MB at L=10,
64 MB at L=11) plus one strip of the raster.

### Running on the real GHSL POP 2020 raster

The raster is not downloaded here. Run this on the machine with the
production workspace mounted. The irv-datapkg rule
`download_jrc_ghsl_pop_built_s` puts it at
`incoming_data/jrc_ghsl/GHS_POP_E2020_GLOBE_R2023A_4326_30ss_V1_0.tif`
(source:
`https://jeodpp.jrc.ec.europa.eu/ftp/jrc-opendata/GHSL/GHS_POP_GLOBE_R2023A/GHS_POP_E2020_GLOBE_R2023A_4326_30ss/V1-0/GHS_POP_E2020_GLOBE_R2023A_4326_30ss_V1_0.zip`).

```bash
pip install -e packages/adaptive-partitions
GHS=incoming_data/jrc_ghsl/GHS_POP_E2020_GLOBE_R2023A_4326_30ss_V1_0.tif
adaptive-partitions build "$GHS" --scheme quadkey --target 200 \
    -o "$BASE/partitions/quadkey/cells.parquet"
adaptive-partitions build "$GHS" --scheme h3 --target 200 \
    -o "$BASE/partitions/h3/cells.parquet"
adaptive-partitions show "$BASE/partitions/quadkey/cells.parquet" | head -40
```

The 1 km Mollweide product (`GHS_POP_E2020_GLOBE_R2023A_54009_1000_V1_0.tif`)
works the same way; pixel centres are reprojected. Expected run time on a
laptop: about a minute for the 30 arcsec read and quadkey build. H3 takes a
few minutes, mostly building the descendant-union geometries.

## Tests

```bash
cd packages/adaptive-partitions && python -m pytest -q
```

The tests generate a small synthetic global population raster (0.25 degree
EPSG:4326, plus a 50 km Mollweide copy) with "cities" that include one on
the antimeridian and one near the north pole. They check:

* the cells cover the globe with no overlaps (planar area sums to
  360 x 180 deg^2 and geodesic area to the WGS84 surface);
* the target count is respected;
* the output is deterministic;
* `assign()` is correct (it matches the geometries, handles edges, poles
  and the antimeridian, reprojected input and empty geometries);
* bbox queries match brute force, including antimeridian queries;
* population is conserved;
* writing and reading GeoParquet round-trips, and the CLI works.

## Licence

MIT, see `LICENSE`.
