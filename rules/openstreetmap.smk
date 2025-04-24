#
# OpenStreetMap Planet
#
rule download_osm:
    output:
        pbf=protected("incoming_data/osm/planet-250414.osm.pbf"),
    shell:
        """
        mkdir -p incoming_data/osm
        cd incoming_data/osm
        aws s3 sync --no-sign-request s3://osm-planet-eu-central-1/planet/pbf/2025/ . \
            --exclude '*' \
            --include planet-250414.osm.pbf \
            --include planet-250414.osm.pbf.md5
        md5sum --check planet-250414.osm.pbf.md5
        """


rule filter_osm_data:
    input:
        pbf="incoming_data/osm/planet-250414.osm.pbf",
    output:
        pbf="incoming_data/osm/planet-250414_{SECTOR}.osm.pbf",
    shell:
        """
        osmium tags-filter \
            --expressions=config/{wildcards.SECTOR}.txt \
            --output={output.pbf} \
            {input.pbf}
        """


def boundary_bbox(wildcards):
    geom = boundary_geom(wildcards.ISO3)
    minx, miny, maxx, maxy = geom.bounds
    # LEFT,BOTTOM,RIGHT,TOP
    return f"{minx},{miny},{maxx},{maxy}"


rule geojson_boundary:
    output:
        json="data/{ISO3}/boundary__{ISO3}.geojson",
    run:
        geom = boundary_geom(wildcards.ISO3)
        json = '{"type":"Feature","geometry": %s}' % shapely.to_geojson(geom)
        with open(output.json, "w") as fh:
            fh.write(json)


rule extract_osm_data:
    input:
        pbf="incoming_data/osm/planet-250414_{SECTOR}.osm.pbf",
        json="data/{ISO3}/boundary__{ISO3}.geojson",
    output:
        pbf="data/{ISO3}/openstreetmap/openstreetmap_{SECTOR}__{ISO3}.osm.pbf",
    params:
        bbox_str=boundary_bbox,
    shell:
        """
        osmium extract \
            --polygon {input.json} \
            --set-bounds \
            --strategy=complete_ways \
            --output={output.pbf} \
            {input.pbf}
        """


rule convert_osm_data:
    input:
        pbf="data/{ISO3}/openstreetmap/openstreetmap_{SECTOR}__{ISO3}.osm.pbf",
    output:
        gpkg="data/{ISO3}/openstreetmap/openstreetmap_{SECTOR}__{ISO3}.gpkg",
    shell:
        """
        OSM_CONFIG_FILE=config/{wildcards.SECTOR}.conf.ini ogr2ogr -f GPKG -overwrite {output.gpkg} {input.pbf}
        """
