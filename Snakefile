from pathlib import Path

import geopandas
import irv_datapkg
import pandas

# import spatialpandas

BOUNDARIES = irv_datapkg.read_boundaries(Path("."))
BOUNDARY_LU = BOUNDARIES.set_index("CODE_A3")


def boundary_geom(iso3):
    return BOUNDARY_LU.loc[iso3, "geometry"]


rule all:
    input:
        expand("data/{ISO3}/datapackage.json", ISO3=BOUNDARIES.CODE_A3),


rule clean:
    shell:
        "rm -rf data"


rule datapackage:
    # required input must entail all the datapackage files
    input:
        "data/{ISO3}/aqueduct_flood.csv",
        "data/{ISO3}/gridfinder/grid__{ISO3}.gpkg",
        "data/{ISO3}/gridfinder/targes__{ISO3}.tif",
        "data/{ISO3}/isimip_heat_drought.csv",
        "data/{ISO3}/jrc_ghsl.csv",
        "data/{ISO3}/osm_road_and_rail/osm-road-and-rail__{ISO3}.gpkg",
        "data/{ISO3}/storm.csv",
        "data/{ISO3}/wri_powerplants/wri-powerplants__{ISO3}.gpkg",
    output:
        json="data/{ISO3}/datapackage.json",
    script:
        "scripts/generate_datapackage_json.py"


rule clip_tiff:
    input:
        tiff="incoming_data/{DATASET}/{SLUG}.tif",
    output:
        tiff="data/{ISO3}/{DATASET}/{SLUG}__{ISO3}.tif",
    run:
        irv_datapkg.crop_raster(input.tiff, output.tiff, boundary_geom(wildcards.ISO3))


rule clip_geopackage:
    input:
        gpkg="incoming_data/{DATASET}/{SLUG}.gpkg",
    output:
        gpkg="data/{ISO3}/{DATASET}/{SLUG}__{ISO3}.gpkg",
    run:
        gdf = geopandas.read_file(input.gpkg, engine="pyogrio")
        geom = boundary_geom(wildcards.ISO3)
        (xmin, ymin, xmax, ymax) = geom.bounds
        clipped = gdf.cx[xmin:xmax, ymin:ymax]
        clipped.to_file(
            output.gpkg, driver="GPKG", layer=wildcards.SLUG, engine="pyogrio"
        )


#
# WRI Aqueduct Flood Hazard
#
def summarise_aqueduct_input(wildcards):
    # should be the directory "incoming_data/aqueduct_flood"
    checkpoint_output = checkpoints.download_aqueduct.get(**wildcards).output.tiffs

    # should be a list of filename slugs (stripped of .tif extension)
    slugs = glob_wildcards(os.path.join(checkpoint_output, "{SLUG}.tif")).SLUG

    # should match ISO3 to "JAM" or whichever country code is requested in summary
    # and return a full list of file paths under "data/JAM/aqueduct_flood/*.tif"
    return expand(
        "data/{ISO3}/aqueduct_flood/{SLUG}__{ISO3}.tif", ISO3=wildcards.ISO3, SLUG=slugs
    )


rule summarise_aqueduct:
    input:
        summarise_aqueduct_input,
    output:
        "data/{ISO3}/aqueduct_flood.csv",
    run:
        raise NotImplementedError()


checkpoint download_aqueduct:
    output:
        tiffs=directory("incoming_data/aqueduct_flood"),
    shell:
        """
        mkdir --parents incoming_data/aqueduct_flood
        cd incoming_data/aqueduct_flood
        aws s3 sync --no-sign-request s3://wri-projects/AqueductFloodTool/download/v2/ . \
            --exclude '*' \
            --include '*.tif'
        """


#
# Gridfinder
#
rule download_gridfinder:
    output:
        grid="incoming_data/gridfinder/grid.gpkg",
        targets="incoming_data/gridfinder/targets.tif",
    shell:
        """
        mkdir --parents incoming_data/gridfinder
        cd incoming_data/gridfinder
        zenodo_get 10.5281/zenodo.3628142
        """


#
# Extreme heat/drought
#
checkpoint download_isimip:
    output:
        tiffs=directory("incoming_data/isimip_heat_drought"),
    shell:
        """
        mkdir --parents incoming_data/isimip_heat_drought
        cd incoming_data/isimip_heat_drought
        zenodo_get 10.5281/zenodo.7732393
        """


#
# JRC GHSL data
#
rule summarise_population:
    # EPOCH available in: range(1975, 2031, 5)
    # RESOLUTION available in: 4326_3ss, 4326_30ss, 54009_100, 54009_1000
    input:
        tiffs=expand(
            "incoming_data/jrc_ghsl/GHS_POP_E{EPOCH}_GLOBE_R2023A_{RESOLUTION}_V1_0.tif",
            EPOCH=range(1975, 2031, 5),
            RESOLUTION=["4326_30ss"],
        ),
    output:
        csv="data/{ISO3}/jrc_ghsl.csv",
    run:
        raise NotImplementedError()


rule download_population:
    output:
        tiff="incoming_data/jrc_ghsl/GHS_POP_E{EPOCH}_GLOBE_R2023A_{RESOLUTION}_V1_0.tif",
    shell:
        """
        mkdir --parents ./incoming_data/jrc_ghsl
        wget --no-clobber --directory-prefix=./incoming_data/jrc_ghsl \
            https://jeodpp.jrc.ec.europa.eu/ftp/jrc-opendata/GHSL/GHS_POP_GLOBE_R2023A/GHS_POP_E{wildcards.EPOCH}_GLOBE_R2023A_{wildcards.RESOLUTION}/V1-0/GHS_POP_E{wildcards.EPOCH}_GLOBE_R2023A_{wildcards.RESOLUTION}_V1_0.zip
        unzip {input} -d ./incoming_data/jrc_ghsl
        """


#
# OpenStreetMap Planet
#
rule download_osm:
    output:
        checksum="incoming_data/osm/planet-231106.osm.pbf.md5",
    shell:
        """
        mkdir --parents incoming_data/osm
        cd incoming_data/osm
        aws s3 sync --no-sign-request s3://osm-planet-eu-central-1/planet/pbf/2023/ . \
            --exclude '*' \
            --include planet-231106.osm.pbf \
            --include planet-231106.osm.pbf.md5
        md5sum --check planet-231106.osm.pbf.md5
        """


#
# STORM tropical cyclones
#
rule download_storm:
    output:
        tiffs=expand(
            "incoming_data/storm/STORM_FIXED_RETURN_PERIODS_{STORM_MODEL}_{STORM_RP}_YR_RP.tif",
            STORM_MODEL=[
                "constant",
                "CMCC-CM2-VHR4",
                "CNRM-CM6-1-HR",
                "EC-Earth3P-HR",
                "HadGEM3-GC31-HM",
            ],
            STORM_RP=(
                list(range(10, 100, 10))
                + list(range(100, 1000, 100))
                + list(range(1000, 10000, 1000))
            ),
        ),
    shell:
        """
        mkdir --parents incoming_data/storm
        cd incoming_data/storm
        zenodo_get 10.5281/zenodo.7438145
        """


def summarise_storm_input(wildcards):
    # should be the full list of STORM tiffs
    # like "incoming_data/storm/{slug}.tif"
    original_storm_tiffs = rules.download_storm.output.tiffs

    # should match ISO3 to "JAM" or whichever country code is requested in summary
    # and return a full list of file paths under "data/JAM/aqueduct_flood/*.tif"
    iso3 = wildcards.ISO3
    clipped_tiffs = []
    for fname in original_storm_tiffs:
        slug = fname.replace("incoming_data/storm/", "").replace(".tif", "")
        clipped_tiffs.append(f"data/{iso3}/storm/{slug}__{iso3}.tif")
    return clipped_tiffs


rule summarise_storm:
    input:
        tiffs=rules.download_storm.output.tiffs,
    output:
        csv="data/{ISO3}/storm.csv",
    run:
        raise NotImplementedError()


#
# WRI Power Plants
#
rule download_powerplants:
    output:
        csv="incoming_data/wri_powerplants/global_power_plant_database.csv",
    shell:
        """
        mkdir --parents incoming_data/wri_powerplants
        cd incoming_data/wri_powerplants
        wget https://wri-dataportal-prod.s3.amazonaws.com/manual/global_power_plant_database_v_1_3.zip
        unzip -o global_power_plant_database_v_1_3.zip
        """


rule powerplants_to_gpkg:
    input:
        csv=rules.download_powerplants.output.csv,
    output:
        gpkg="incoming_data/wri_powerplants/global_power_plant_database.gpkg",
    run:
        df = pandas.read_csv(input.csv)
        geoms = geopandas.points_from_xy(df.longitude, df.latitude)
        gdf = geopandas.GeoDataFrame(data=df, geometry=geoms, crs="EPSG:4326")
        gdf.to_file(output.gpkg, driver="GPKG", engine="pyogrio")
