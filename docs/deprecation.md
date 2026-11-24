# Deprecating consumer download rules

Status: plan (WP14). No rule has been removed yet.

Once a dataset is published by irv-datapkg, the consumers' own download rules
for it become redundant (ADR 0001: only irv-datapkg talks to upstream hosts).
This page lists every legacy download rule in open-gira and infra-risk-vis,
the catalogue dataset that replaces it, and what has to be true before the
rule is deleted. The consumers keep the rules as a fallback until then: a
catalogue rule takes precedence only when its dataset is enabled in config
(open-gira `datapkg.sources.<dataset>.enabled`, infra-risk-vis
`datapkg.enabled`).

## Removal condition

A legacy rule may be removed when **all** of these hold for the dataset(s) it
downloads:

1. **Published in a release.** The dataset version is pinned by an
   irv-datapkg release manifest (`releases/YYYY.N/catalog.json`) on the
   public remote (or `{BASE}-private` for private data), and the remote passes
   `irv-datapkg validate {BASE} --deep` and `irv-datapkg audit {BASE}
   --release YYYY.N` (docs/qa.md).
2. **Consumer switched by default.** The consumer's mapping for the dataset
   is enabled in its committed config, and its committed lock file pins that
   release (`datapkg_check_lock` in open-gira, `datapkg_lock_check` in
   infra-risk-vis passes).
3. **Verified on production.** A run on the production machine with the
   dataset enabled reproduced the legacy outputs (open-gira: the downstream
   results the rule feeds, compared with a legacy run; infra-risk-vis: the
   raster COGs and terracotta metadata of the pipeline), and the comparison
   is recorded in the removal PR.
4. **Row-specific blockers resolved** (column "Also needs" below), e.g. a
   format converter or a directory form of `outputs`.

The removal PR deletes the rule, its `ruleorder` entry and any config only it
used, links the release and the verification run, and updates the status
column here and in the consumer's docs.

## Status key

- **Recipe**: `yes` if irv-datapkg has a T0/T1 recipe (`rules/<dataset>.smk`,
  `metadata/*.yml`) today, otherwise the work package that adds it.
- **Consumer**: `wired` (mapping in the consumer config, ready to enable),
  `mapping only` (the generic mechanism supports it; a few lines of config once
  item ids are known), `todo`, or a named blocker.
- No dataset is in a release yet, so no row meets condition 1.

## open-gira

Rule names as defined in `workflow/`. Details of each mapping:
open-gira `docs/src/user-guide/usage/datapkg-inputs.md` ("Dataset mapping").

| Legacy rule(s) | File | Replaced by | Recipe | Consumer | Also needs |
| --- | --- | --- | --- | --- | --- |
| `download_gridfinder` | power/gridfinder.smk | `gridfinder` | yes | wired | |
| `download_powerplants` | power/wri-powerplants.smk | `wri_powerplants` | yes | wired | |
| `download_ghsl`, `download_ghsl_all` | population-economy/ghsl-pop.smk | `ghsl_pop` | yes | wired | R2022A path (`gridfinder-targets.smk`) still legacy: move that reader to R2023A or publish R2022A first |
| `download_ghsl_built_s`, `download_ghsl_built_s_all` | buildings/ghsl-built.smk | `ghsl_built_s` | yes | wired | |
| `download_hazard_datasets` (checkpoint; aqueduct-river, aqueduct-coast branches) | flood/aqueduct.smk | `aqueduct_flood` | yes | wired | The checkpoint stays (downstream rules depend on it by name); only its download branch goes |
| `download_hazard_datasets` (river-jrc), `download_river_jrc`, `extract_river_jrc` | flood/river_jrc.smk | `jrc_flood_globalmaps` | yes | mapping only | |
| `download_hazard_datasets` (landslide-arup), `download_metadata`, `download_landslides` | landslide/landslide_arup.smk | `arup_landslide` | WP6 | mapping only | PDF report / metadata JSON are not published |
| `download_deltares` | flood/coastal_deltares.smk | `deltares_coastal_flood` | WP6 | needs downstream change | T1 is COG, not NetCDF |
| `download_gadm_levels` (+ `simplify_admin_bounds`) | context/gadm.smk | `boundaries_gadm` | WP9 | mapping only | Simplification moves to irv-datapkg (ADR 0007) |
| `download_ne_50m`, `unzip_ne_50m` | context/natural-earth.smk | `boundaries_naturalearth` | WP9 | needs extension | Directory form of `outputs` |
| `download_coastlines` | context/coastlines.smk | `naturalearth_coastlines` | WP6 | needs extension | Directory output, GeoParquet -> shapefile |
| `download_hydrobasins`, `extract_hydrobasins`, `extract_hydrobasins_level`, `extract_hydrobasins_all` | nature-ecosystems/hydrobasins.smk | `hydrobasins` | WP6 | mapping only | Merge moves to irv-datapkg |
| `download_glob_cover_2009`, `unzip_glob_cover_2009` | nature-ecosystems/land-cover.smk | `globcover_2009` | WP6 | mapping only | |
| `download_copernicus_global_dynamic_land_cover` | nature-ecosystems/land-cover.smk | `copernicus_lc100` | WP6 | mapping only | |
| `download_nbs_suitability` | nature-ecosystems/nbs-suitability.smk | `nbs_suitability` (private) | WP6 | mapping only | Private base credentials on production |
| `download_GDP` | population-economy/dryad-gdp.smk | `dryad_gdp_hdi` | WP6 | needs converter | NetCDF readers downstream |
| `download_giri_bem` | buildings/giri-bem.smk | `giri_bem` | WP6 | mapping only | |
| `download_pwt`, `extract_pwt` | buildings/penn-world-tables.smk | `penn_world_table` | WP6 | mapping only | XLSX tidying moves to irv-datapkg |
| `download_cwon`, `extract_cwon_produced_capital` | buildings/worldbank-cwon.smk | `worldbank_cwon` | WP6 | mapping only | |
| `download_imf_icsd`, `extract_imf_icsd` | buildings/imf-investment-capital-stock.smk | `imf_icsd` | WP6 | mapping only | |
| `download_IBTrACS` | tropical-cyclone/IBTrACS.smk | `ibtracs` | WP6 | mapping only | Track parsing stays in open-gira |
| `download_STORM`, `extract_storm_event`, `extract_all_storm_events` | tropical-cyclone/STORM.smk | `storm_tracks` | WP6 | needs extension | Per-file or directory outputs |
| `extract_storm_wind_speed_raster`, `extract_storm_RP_raster` (+ `wrap_*`, `mosaic_*`) | tropical-cyclone/STORM.smk | `storm_fixed_rp` | yes | mapping only | Wrap and mosaic are dropped (ADR 0007) |
| `download_IRIS`, `extract_IRIS` | tropical-cyclone/IRIS.smk | `iris` (private) | WP6 | needs extension | Directory form; private base |
| `download_osm` (+ `create_overall_bbox`, `create_bbox_extracts`, `slice`, `filter_osm_data`) | transport/openstreetmap.smk | `openstreetmap` | yes (WP8) | stage B | Windowed reads on partitioned GeoParquet replace bbox slicing |

CHAZ and Emanuel tracks are copied in by hand (no rule); `chaz_tracks` and
`emanuel_tracks` (private, WP6) replace the manual step.

## infra-risk-vis (etl/)

Pipeline rules are imported with a prefix (`use rule * from aqueduct as
aqueduct_*`), so `download_raw_data` in `pipelines/aqueduct/rules.smk` runs
as `aqueduct_download_raw_data`. Details: infra-risk-vis `etl/README.md`
("Pipeline mapping status"). Everything from `clip_raster` onwards stays.

| Pipeline: legacy rule(s) | Replaced by | Recipe | Consumer | Also needs |
| --- | --- | --- | --- | --- |
| aqueduct: `download_raw_data` | `aqueduct_flood` | yes | wired | Coastal subsidence / sea-level dimensions in `filters` |
| storm: `download`, `unpack` | `storm_fixed_rp` | yes | wired | |
| ghsl_pop: `download_and_unzip_raw_data` | `ghsl_pop` | yes | wired | |
| jrc_flood: `download_all` | `jrc_flood_glofas` | WP5 | wired | Global mosaic COG per RP from irv-datapkg |
| landslide_arup: `download_metadata`, `download_landslides` | `arup_landslide` | WP6 | wired | Dimension names from WP6 |
| ghsl_buildings: `download_all_building_types`, `download_non_residential_type` | `ghsl_built_s` | yes | todo | |
| gem_earthquake: `download`, `unpack` | `gem_seismic_hazard` | yes | todo | |
| isimip: `download`, `unpack` | `isimip_heat_drought` | yes | todo | Upstream version to reconcile |
| land_cover: `download_300m_2020_from_CDS` | `copernicus_lulc` | yes | todo | |
| dem: `download` | `merit_dem` | WP7 | todo | |
| cdd_miranda: `download`, `extract` | `cdd_miranda` | WP7 | todo | |
| nature: `download_1000m_organic_carbon_stocks`, `download_3arcsec_biodiversity_intactness`, `download_300m_forest_integrity_index` | `soilgrids_soc`, `biodiversity_intactness`, `forest_landscape_integrity` | WP7 | todo | FLII rescale at T1 or via `raster:bands` scale |
| social: `download_gridded_hdi` | `hdi_mosaiks` | WP7 | todo | |
| social: `download_rwi_metadata`, `download_rwi` | `relative_wealth_index` | WP7 | todo | `process_rwi_grid` stays, reading the GeoParquet |
| social: `download_worldpop_tiff`, `download_worldpop` | `worldpop_age_sex` | WP7 | todo | |
| traveltime_to_healthcare: `download_motorised`, `download_walking` | `traveltime_healthcare` | WP7 | todo | |
| iris: manual copy + `extract_netcdf_to_tiff` | `iris` (private) | WP6 | todo | Private base credentials |

osm_rail, osm_roads, gem_exposure and nbs-adaptation are vector or separate
workflows and out of scope for the `raster/raw` cut point.

## irv-datapkg itself

- The per-dataset download rules of the country-package workflow were replaced
  by the T0/T1 recipes and the generic T2 extract (WP3); nothing to remove.
- The custom Natural Earth map units in `bundled_data/` stay until the
  nation/region relation of ADR 0006 is published (WP9).
- irv-autopkg is retired by ADR 0008 once T2 packages and client extracts
  cover its use (irv-frontend downloads page redesign).

## Keeping this page current

Update a row when a recipe lands (Recipe), when a consumer mapping is wired or
enabled (Consumer), and when a rule is removed (delete the row and note the
release in CHANGELOG.md). A new download rule in a consumer should get a row
here, with the dataset id from `docs/datasets.csv`.
