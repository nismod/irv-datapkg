# Bundled JSON schemas

Offline copies of the schemas that `irv-datapkg validate --deep` checks STAC
documents against, so validation works without reaching schemas.stacspec.org
or stac-extensions.github.io. Each file is registered under its own `$id`.

| Directory | Source | Licence |
| --- | --- | --- |
| `stac-spec/v1.0.0/` | STAC 1.0.0 core (catalog, collection, item), as bundled in pystac 1.10.1 | Apache-2.0 (radiantearth/stac-spec) |
| `geojson/` | GeoJSON Feature/Geometry schemas (geojson.org/schema), as bundled in pystac 1.10.1 | MIT (geojson/schema) |
| `extensions/{name}/{version}.json` | STAC extensions used by `build_item`/`build_collection` (file v2.1.0, projection v1.1.0, raster v1.1.0, table v1.2.0, scientific v1.0.0), as bundled in pystac 1.16 | Apache-2.0 (stac-extensions) |

An extension schema not bundled here is fetched over HTTPS when it is
reachable; otherwise validation skips it with a warning. When `producer.EXT`
gains or bumps an extension, add its schema here.
