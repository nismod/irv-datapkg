#
# OpenStreetMap planet, filtered to infrastructure sectors
#
# Collection: openstreetmap (metadata/openstreetmap.yml, T2 package "openstreetmap")
#
# Sector filters: config/{sector}.txt (osmium tags-filter expressions) and
# config/{sector}.conf.ini (GDAL OSM driver layer/attribute configuration).
#
OSM_PLANET = COLLECTIONS["openstreetmap"].upstream_dir  # planet-250414
OSM_YEAR = f"20{OSM_PLANET[7:9]}"  # planet-YYMMDD
OSM_RAW = raw_dir("openstreetmap")
OSM_BUILD = build_dir("openstreetmap")
# sector -> GDAL OSM driver layers published as items (empty layers are not items)
OSM_LAYERS = {
    "rail": ["points", "lines", "multipolygons"],
    "roads-tertiary": ["lines"],
}


@lru_cache(maxsize=None)
def openstreetmap_items():
    return {
        f"{sector}_{layer}": T1Item(
            ext="parquet",
            properties={"sector": sector, "layer": layer},
            sources=[f"{OSM_RAW}/{OSM_PLANET}.osm.pbf"],
        )
        for sector, layers in OSM_LAYERS.items()
        for layer in layers
    }


register_t1("openstreetmap", openstreetmap_items)


rule openstreetmap_t0:
    """T0: the pinned planet file, md5-checked"""
    output:
        pbf=protected(f"{OSM_RAW}/{OSM_PLANET}.osm.pbf"),
        md5=f"{OSM_RAW}/{OSM_PLANET}.osm.pbf.md5",
        provenance=f"{OSM_RAW}/provenance.json",
    params:
        bucket=f"s3://osm-planet-eu-central-1/planet/pbf/{OSM_YEAR}/",
        planet=OSM_PLANET,
        provenance=provenance_cmd(
            "provenance.json",
            f"{OSM_PLANET}.osm.pbf {OSM_PLANET}.osm.pbf.md5",
            "aws s3 sync",
            url_prefix=f"s3://osm-planet-eu-central-1/planet/pbf/{OSM_YEAR}",
        ),
    shell:
        """
        mkdir -p $(dirname {output.pbf})
        cd $(dirname {output.pbf})
        aws s3 sync --no-sign-request {params.bucket} . \
            --exclude '*' \
            --include {params.planet}.osm.pbf \
            --include {params.planet}.osm.pbf.md5
        md5sum --check {params.planet}.osm.pbf.md5
        {params.provenance}
        """


rule openstreetmap_filter:
    """Intermediate: planet filtered to one sector's tags"""
    input:
        pbf=rules.openstreetmap_t0.output.pbf,
        expressions="config/{sector}.txt",
    output:
        pbf=f"{OSM_BUILD}/{OSM_PLANET}_{{sector}}.osm.pbf",
    shell:
        """
        osmium tags-filter \
            --expressions={input.expressions} \
            --output={output.pbf} \
            {input.pbf}
        """


rule openstreetmap_t1:
    """T1: one GeoParquet per sector and OGR layer (streamed with ogr2ogr)"""
    input:
        pbf=f"{OSM_BUILD}/{OSM_PLANET}_{{sector}}.osm.pbf",
        conf="config/{sector}.conf.ini",
    output:
        parquet=f"{t1_dir('openstreetmap')}/{{sector}}_{{layer}}.parquet",
    wildcard_constraints:
        sector="|".join(OSM_LAYERS),
        layer="points|lines|multilinestrings|multipolygons|other_relations",
    params:
        ogr2ogr=lambda wildcards, input, output: recipes.ogr2ogr_geoparquet_cmd(
            input.pbf, output.parquet, wildcards.layer
        ),
    shell:
        """
        OSM_CONFIG_FILE={input.conf} {params.ogr2ogr}
        """
