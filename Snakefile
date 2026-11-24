#
# irv-datapkg workflow
#
# T0 raw mirror -> T1 analysis-ready global layers (STAC catalogue) -> T2 per-region
# extracts (Zenodo country packages). See docs/recipes.md and docs/adr/.
#
import json
import os
import shutil
from datetime import datetime
from glob import glob
from pathlib import Path

import geopandas
import irv_datapkg
import pandas
import requests
import shapely


configfile: "config.json"


DATAPKG_VERSION = config["datapkg_version"]
ZENODO_URL = config["zenodo_url"]

BOUNDARIES = irv_datapkg.read_boundaries(Path("."))
BOUNDARY_LU = BOUNDARIES.set_index("CODE_A3")


envvars:
    "ZENODO_TOKEN",
    "CDSAPI_URL",
    "CDSAPI_KEY",


wildcard_constraints:
    ISO3="[A-Z]{3}",


def boundary_geom(iso3):
    return BOUNDARY_LU.loc[iso3, "geometry"]


def boundary_adm0_a3(iso3):
    """Get the ADM0 A3 code corresponding to this boundary

    In most cases this is a one-to-one mapping with no change, but where we have
    some territories split into custom units (for example, Kiribati, East and
    West of the antimeridian) this looks up the standard parent ADM0 A3 code.
    """
    return BOUNDARY_LU.loc[iso3, "ADM0_A3"]


#
# Top-level rules
#
rule all:
    """T2: every country data package (datapackage.json)"""
    input:
        expand("data/{ISO3}/datapackage.json", ISO3=BOUNDARIES.CODE_A3),


rule all_uploaded:
    input:
        expand("zenodo/{ISO3}.deposited", ISO3=BOUNDARIES.CODE_A3),


rule all_published:
    input:
        expand("zenodo/{ISO3}.published", ISO3=BOUNDARIES.CODE_A3),


rule clean:
    shell:
        "rm -rf data"


# Workspace paths, metadata and the T1 item registry
include: "rules/common.smk"
# Dataset recipes: T0 download, T1 standardise (one file per dataset)
include: "rules/aqueduct_flood.smk"
include: "rules/copernicus_dem.smk"
include: "rules/copernicus_lulc.smk"
include: "rules/gem_earthquake.smk"
include: "rules/geoboundaries.smk"
include: "rules/gridfinder.smk"
include: "rules/isimip_heat_drought.smk"
include: "rules/jrc_floods.smk"
include: "rules/jrc_ghsl.smk"
include: "rules/openstreetmap.smk"
include: "rules/storm.smk"
include: "rules/wri_powerplants.smk"
# Generic T1 registration (STAC items) and T2 extracts, then Zenodo
include: "rules/catalogue.smk"
include: "rules/t2.smk"
include: "rules/zenodo.smk"
