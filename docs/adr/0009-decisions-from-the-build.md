# 0009 Decisions settled after the first build

Status: accepted (November 2026)

These settle questions raised while building WP0–WP4, WP10–WP12 and WP14
on synthetic data.

| Topic | Decision |
| --- | --- |
| Copernicus DEM GLO-90 | T1 publishes each upstream tile as a COG at its native resolution, plus a global VRT index. No mosaicking or resampling (ADR 0001). WP3's current global mosaic must be changed. |
| GEM seismic hazard | Public, with its CC-BY-NC-SA-4.0 licence and non-commercial terms prominent in the collection. |
| GADM | GADM 3.6 does not allow redistribution. GADM and every GADM-derived layer (including open-gira's admin-level and HydroBASINS-with-GADM-codes results) go in the private prefix. Public admin layers move to geoBoundaries (CC-BY) over time, in open-gira as well. |
| JRC CEMS-GLOFAS | T1 publishes one global COG per return period, mosaicked from the upstream tiles (same grid, no resampling). |
| ISIMIP heat/drought | Zenodo 10.5281/zenodo.11582369 is canonical for T1. 7732393 is an earlier version, not published. |
| Snakemake thread mode | Keep `force-use-threads` in `profiles/default`. Benchmark a real batch on the production machine; turn it off for heavy rules if memory or GDAL thread safety causes problems. |
| T2 country packages | The changes from the new extract path ship as datapkg 0.3.0: some file names change, geoBoundaries `metaData.txt` is dropped, `fluvial` becomes `river`, and the storm CSV gains `return_period`. Note them in the Zenodo description. |
| Partition library | Stays in `packages/adaptive-partitions/` inside irv-datapkg for now. Revisit after the first real build. |
| First release | `releases/2027.1` includes everything: WP5–WP7 datasets, OSM networks (WP8) and boundaries/regions (WP9). |
| EEZ source | Marine Regions "Union of world country boundaries and EEZs" for the nation/region relation (check its licence before publishing). |
| snkit | The team updates snkit outside the agent threads. WP8 waits for a snkit release. |
| irv-frontend | Attach `nismod/irv-frontend` in a later session and do WP13 after WP9 and WP11 land. |
