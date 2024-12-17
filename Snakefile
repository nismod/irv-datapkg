import json
import shutil
from datetime import datetime
from pathlib import Path
from glob import glob

import geopandas
import irv_datapkg
import pandas
import requests
import shapely

DATAPKG_VERSION = "0.2.0"
ZENODO_URL = "sandbox.zenodo.org"
# ZENODO_URL = "zenodo.org"

BOUNDARIES = irv_datapkg.read_boundaries(Path("."))
BOUNDARY_LU = BOUNDARIES.set_index("CODE_A3")


envvars:
    "ZENODO_TOKEN",
    "CDSAPI_URL",
    "CDSAPI_KEY"


def boundary_geom(iso3):
    return BOUNDARY_LU.loc[iso3, "geometry"]


#
# Top-level rules
#
rule clean:
    shell:
        "rm -rf data"


rule all:
    input:
        expand("data/{ISO3}/datapackage.json", ISO3=BOUNDARIES.CODE_A3),


rule all_uploaded:
    input:
        expand("zenodo/{ISO3}.deposited", ISO3=BOUNDARIES.CODE_A3),


rule all_published:
    input:
        expand("zenodo/{ISO3}.published", ISO3=BOUNDARIES.CODE_A3),


#
# Data package
#
rule datapackage:
    input:
        checksums="data/{ISO3}/md5sum.txt",
    output:
        json="data/{ISO3}/datapackage.json",
    script:
        "scripts/generate_datapackage_json.py"


rule checksums:
    # input must require all the data package files
    # - summary CSVs require multiple TIFFs in turn
    input:
        "data/{ISO3}/aqueduct_flood.csv",
        "data/{ISO3}/gridfinder/grid__{ISO3}.gpkg",
        "data/{ISO3}/gridfinder/targets__{ISO3}.tif",
        "data/{ISO3}/isimip_heat_drought.csv",
        "data/{ISO3}/jrc_ghsl.csv",
        "data/{ISO3}/openstreetmap/openstreetmap_rail__{ISO3}.gpkg",
        "data/{ISO3}/openstreetmap/openstreetmap_roads-tertiary__{ISO3}.gpkg",
        "data/{ISO3}/storm.csv",
        "data/{ISO3}/wri_powerplants/wri-powerplants__{ISO3}.gpkg",
        "data/{ISO3}/copernicus_lulc/copernicus_lulc__{ISO3}.tif",
        "data/{ISO3}/copernicus_dem/copernicus_dem__{ISO3}.tif",
    output:
        checksums="data/{ISO3}/md5sum.txt",
    shell:
        """
        cd data/{wildcards.ISO3}
        md5sum **/*.* | grep "tif\\|gpkg" | sort -k 2 > md5sum.txt
        """


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

include: "rules/aqueduct_flood.smk"
include: "rules/gridfinder.smk"
include: "rules/isimip_heat_drought.smk"
include: "rules/jrc_floods.smk"
include: "rules/jrc_ghsl.smk"
include: "rules/openstreetmap.smk"
include: "rules/storm.smk"
include: "rules/wri_powerplants.smk"
include: "rules/copernicus_lulc.smk"
include: "rules/copernicus_dem.smk"
include: "rules/zenodo.smk"
