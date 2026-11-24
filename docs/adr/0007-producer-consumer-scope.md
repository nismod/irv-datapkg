# 0007 Producer and consumer scope

Status: accepted

| Item | Decision |
| --- | --- |
| Download, checksums, provenance for all third-party data | irv-datapkg |
| NetCDF/ASCII to COG, reprojection out of Mollweide/Homolosine for consumers that need it, rescaling, unzip/extract | irv-datapkg (format standardisation only; T1 keeps the source grid) |
| GADM simplification, HydroBASINS merge, capital-stock XLSX tidying, OSM tag filtering on a pinned planet | irv-datapkg |
| OSM network creation (updated snkit) | irv-datapkg |
| STORM wrap + mosaic | Dropped: use the published mosaics |
| OSM bbox slicing in open-gira | Dropped in favour of windowed reads on partitioned GeoParquet |
| Track parsing to a common schema (IBTrACS, STORM, IRIS, CHAZ, Emanuel) | Stays in open-gira for now; core parsing may later move to a library |
| Power network creation (incl. target population), wind fields, hazard intersection, damages, risk, NbS combination | open-gira |
| Web Mercator clipping, terracotta, Zarr stack, MBTiles, PostGIS | infra-risk-vis |

open-gira integrates in two stages: first fetch into the existing `input/`
paths; then downstream rules read remote data directly without the
intermediate `input/` copy.

infra-risk-vis publishes its serving bundle with docs on addressing it
directly for GIS and other web uses.
