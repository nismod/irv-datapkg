import json
import shutil
from pathlib import Path
from glob import glob

import geopandas
import irv_datapkg
import pandas
import requests
import shapely

DATAPKG_VERSION = "0.1.0"
# ZENODO_URL = "sandbox.zenodo.org"
ZENODO_URL = "zenodo.org"

BOUNDARIES = irv_datapkg.read_boundaries(Path("."))
BOUNDARY_LU = BOUNDARIES.set_index("CODE_A3")


envvars:
    "ZENODO_TOKEN",


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
# Deposit to Zenodo
#
rule zip:
    input:
        "data/{ISO3}/datapackage.json",
    output:
        "zenodo/{ISO3}.zip",
    shell:
        """
        zip -r zenodo/{wildcards.ISO3}.zip data/{wildcards.ISO3}
        """


rule create_deposition:
    output:
        json="zenodo/{ISO3}.deposition.json",
    run:
        # Create deposition
        params = {"access_token": os.environ["ZENODO_TOKEN"]}
        r = requests.post(
            f"https://{ZENODO_URL}/api/deposit/depositions", params=params, json={}
        )
        r.raise_for_status()

        # Deposition details
        deposition = r.json()

        # Save details
        with open(output.json, "w") as fh:
            json.dump(deposition, fh, indent=2)


rule deposit:
    input:
        deposition="zenodo/{ISO3}.deposition.json",
        archive="zenodo/{ISO3}.zip",
        datapackage="data/{ISO3}/datapackage.json",
    output:
        touch("zenodo/{ISO3}.deposited"),
    run:
        params = {"access_token": os.environ["ZENODO_TOKEN"]}

        with open(input.deposition, "r") as fh:
            deposition = json.load(fh)

        with open(input.datapackage, "r") as fh:
            datapackage = json.load(fh)

        deposition_id = deposition["id"]
        bucket_url = deposition["links"]["bucket"]

        # Upload files
        path = Path(input.archive)
        print("Uploading", path)
        with open(path, "rb") as fh:
            r = requests.put(
                f"{bucket_url}/{path.name}",
                data=fh,
                params=params,
            )
            print(r.json())
            r.raise_for_status()

            # Set up metadata
        centroid = boundary_geom(wildcards.ISO3).centroid
        place_name = BOUNDARY_LU.loc[wildcards.ISO3, "NAME"]

        with open("metadata/zenodo_notes.html", "r") as fh:
            notes = fh.read()

        with open("metadata/zenodo_description.html", "r") as fh:
            description = fh.read()

        metadata = {
            "metadata": {
                "title": datapackage["title"],
                "description": description,
                "locations": [
                    {"lat": centroid.y, "lon": centroid.x, "place": place_name}
                ],
                "upload_type": "dataset",
                "access_right": "open",
                "license": "cc-by-sa-4.0",
                "creators": [
                    {
                        "name": "Russell, Tom",
                        "affiliation": "University of Oxford",
                        "orcid": "0000-0002-0081-400X",
                    },
                    {
                        "name": "Jaramillo, Diana",
                        "affiliation": "University of Oxford",
                    },
                    {
                        "name": "Nicholas, Chris",
                    },
                    {
                        "name": "Thomas, Fred",
                        "affiliation": "University of Oxford",
                        "orcid": "0000-0002-8441-5638",
                    },
                    {
                        "name": "Pant, Raghav",
                        "affiliation": "University of Oxford",
                        "orcid": "0000-0003-4648-5261",
                    },
                    {
                        "name": "Hall, Jim W.",
                        "affiliation": "University of Oxford",
                        "orcid": "0000-0002-2024-9191",
                    },
                ],
                "references": [
                    "Arderne, Christopher; Nicolas, Claire; Zorn, Conrad; & Koks, Elco E. (2020). Data from: Predictive mapping of the global power system using open data [Data set]. In Nature Scientific Data (1.1.1, Vol. 7, Number Article 19). Zenodo. DOI:10.5281/zenodo.3628142",
                    "Bloemendaal, Nadia; de Moel, H. (Hans); Muis, S; Haigh, I.D. (Ivan); Aerts, J.C.J.H. (Jeroen) (2020): STORM tropical cyclone wind speed return periods. 4TU.ResearchData. Dataset. DOI:10.4121/12705164.v3",
                    "Bloemendaal, Nadia; de Moel, Hans; Dullaart, Job; Haarsma, R.J. (Reindert); Haigh, I.D. (Ivan); Martinez, Andrew B.; et al. (2022): STORM climate change tropical cyclone wind speed return periods. 4TU.ResearchData. Dataset. DOI:10.4121/14510817.v3",
                    "Global Energy Observatory, Google, KTH Royal Institute of Technology in Stockholm, Enipedia, World Resources Institute. (2018) Global Power Plant Database. Published on Resource Watch and Google Earth Engine; http://resourcewatch.org/",
                    "Lange, S., Volkholz, J., Geiger, T., Zhao, F., Vega, I., Veldkamp, T., et al. (2020). Projecting exposure to extreme climate impact events across six event categories and three spatial scales. Earth's Future, 8, e2020EF001616. DOI:10.1029/2020EF001616",
                    "Natural Earth (2023) Admin 0 Map Units, v5.1.1. [Dataset] Available online: www.naturalearthdata.com/downloads/10m-cultural-vectors/10m-admin-0-details",
                    "OpenStreetMap contributors, Russell T., Thomas F., nismod/datapkg contributors (2023) Road and Rail networks derived from OpenStreetMap. [Dataset] Available at: https://global.infrastructureresilience.org",
                    "Pesaresi M., Politis P. (2023): GHS-BUILT-S R2023A - GHS built-up surface grid, derived from Sentinel2 composite and Landsat, multitemporal (1975-2030) European Commission, Joint Research Centre (JRC) PID: http://data.europa.eu/89h/9f06f36f-4b11-47ec-abb0-4f8b7b1d72ea, DOI:10.2905/9F06F36F-4B11-47EC-ABB0-4F8B7B1D72EA",
                    "Russell, T., Nicholas, C., & Bernhofen, M. (2023). Annual probability of extreme heat and drought events, derived from Lange et al 2020 (Version 2) [Data set]. Zenodo. DOI:10.5281/zenodo.8147088",
                    "Schiavina M., Freire S., Carioli A., MacManus K. (2023): GHS-POP R2023A - GHS population grid multitemporal (1975-2030).European Commission, Joint Research Centre (JRC) PID: http://data.europa.eu/89h/2ff68a52-5b5b-4a22-8f40-c41da8332cfe, DOI:10.2905/2FF68A52-5B5B-4A22-8F40-C41DA8332CFE",
                    "Ward, P.J., H.C. Winsemius, S. Kuzma, M.F.P. Bierkens, A. Bouwman, H. de Moel, A. Díaz Loaiza, et al. (2020) Aqueduct Floods Methodology. Technical Note. Washington, D.C.: World Resources Institute. Available online at: https://www.wri.org/publication/aqueduct-floods-methodology",
                ],
                "related_identifiers": [
                    {"identifier":"10.5281/zenodo.3628142", "relation": "isDerivedFrom", "resource_type": "dataset"},
                    {"identifier":"10.4121/12705164.v3", "relation": "isDerivedFrom", "resource_type": "dataset"},
                    {"identifier":"10.4121/14510817.v3", "relation": "isDerivedFrom", "resource_type": "dataset"},
                    {"identifier":"10.1029/2020EF001616", "relation": "isDerivedFrom", "resource_type": "dataset"},
                    {"identifier":"10.2905/9F06F36F-4B11-47EC-ABB0-4F8B7B1D72EA", "relation": "isDerivedFrom", "resource_type": "dataset"},
                    {"identifier":"10.5281/zenodo.8147088", "relation": "isDerivedFrom", "resource_type": "dataset"},
                    {"identifier":"10.2905/2FF68A52-5B5B-4A22-8F40-C41DA8332CFE", "relation": "isDerivedFrom", "resource_type": "dataset"},
                ],
                "communities": [{"identifier": "ccg"}],
                "notes": notes,
                "version": DATAPKG_VERSION,
            }
        }

        # Upload metadata
        r = requests.put(
            f"https://{ZENODO_URL}/api/deposit/depositions/{deposition_id}",
            params=params,
            json=metadata,
        )
        print(r.json())
        r.raise_for_status()


rule publish:
    input:
        "zenodo/{ISO3}.deposited",
        deposition="zenodo/{ISO3}.deposition.json",
    output:
        touch("zenodo/{ISO3}.published")
    run:
        params = {"access_token": os.environ["ZENODO_TOKEN"]}

        with open(input.deposition, "r") as fh:
            deposition = json.load(fh)

        deposition_id = deposition["id"]

        r = requests.post(
            f"https://{ZENODO_URL}/api/deposit/depositions/{deposition_id}/actions/publish",
            params=params
        )
        r.raise_for_status()


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
    output:
        checksums="data/{ISO3}/md5sum.txt",
    shell:
        """
        cd data/{wildcards.ISO3}
        md5sum **/*.* | grep "tif\|gpkg" | sort -k 2 > md5sum.txt
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


#
# WRI Aqueduct Flood Hazard
#
def summarise_aqueduct_input(wildcards):
    # should be the directory "incoming_data/aqueduct_flood"
    checkpoint_output = checkpoints.download_aqueduct.get(**wildcards).output.tiffs

    # should be a list of filename slugs (stripped of .tif extension)
    slugs = [
        slug
        for slug in glob_wildcards(os.path.join(checkpoint_output, "{SLUG}.tif")).SLUG
        if ("nosub" not in slug and "perc" not in slug and "1_5" not in slug)
    ]

    # should match ISO3 to "JAM" or whichever country code is requested in summary
    # and return a full list of file paths under "data/JAM/aqueduct_flood/*.tif"
    return expand(
        "data/{ISO3}/aqueduct_flood/{SLUG}__{ISO3}.tif", ISO3=wildcards.ISO3, SLUG=slugs
    )


rule summarise_aqueduct:
    input:
        summarise_aqueduct_input,
    output:
        csv="data/{ISO3}/aqueduct_flood.csv",
    run:
        coastal_paths = glob(f"data/{wildcards.ISO3}/aqueduct_flood/inuncoast*.tif")
        coastal_summary = pandas.DataFrame({"path": coastal_paths})
        # inuncoast_{RCP}_{SUB}_{EPOCH}_{RP}_{SLR}.tif
        coastal_meta = coastal_summary.path.str.extract(
            r"inuncoast_([^_]+)_([^_]+)_([^_]+)_([^_]+)"
        )
        coastal_meta.columns = ["rcp", "subsidence", "epoch", "return_period"]
        coastal_meta.drop(columns="subsidence", inplace=True)
        coastal_meta["gcm"] = "na"
        coastal_meta["hazard"] = "coastal"
        coastal_meta["path"] = coastal_summary.path

        fluvial_paths = glob(f"data/{wildcards.ISO3}/aqueduct_flood/inunriver*.tif")
        fluvial_summary = pandas.DataFrame({"path": fluvial_paths})
        # inunriver_{RCP}_{GCM}_{EPOCH}_{RP}.tif
        fluvial_meta = fluvial_summary.path.str.extract(
            r"inunriver_([^_]+)_([^_]+)_([^_]+)_([^_]+)"
        )
        fluvial_meta.columns = ["rcp", "gcm", "epoch", "return_period"]
        fluvial_meta["hazard"] = "fluvial"
        fluvial_meta["path"] = fluvial_summary.path

        meta = pandas.concat([coastal_meta, fluvial_meta])
        meta.path = meta.path.str.replace(f"data/{wildcards.ISO3}/", "")
        meta.gcm = meta.gcm.str.lstrip("0")
        meta.return_period = (
            meta.return_period.str.replace("rp", "").str.lstrip("0").astype(int)
        )
        meta.rcp = meta.rcp.str.replace("rcp", "").str.replace("p", ".")
        meta.epoch = (
            meta.epoch.str.replace("hist", "2010")
            .str.replace("1980", "2010")
            .astype(int)
        )

        columns = ["hazard", "epoch", "rcp", "gcm", "return_period", "path"]
        meta = meta[columns].sort_values(by=columns)

        meta.to_csv(output.csv, index=False)


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
def summarise_isimip_input(wildcards):
    # should be the directory "incoming_data/isimip_heat_drought"
    checkpoint_output = checkpoints.download_isimip.get(**wildcards).output.tiffs

    # should be a list of filename slugs (stripped of .tif extension)
    slugs = glob_wildcards(os.path.join(checkpoint_output, "{SLUG}.tif")).SLUG

    # should match ISO3 to "JAM" or whichever country code is requested in summary
    # and return a full list of file paths under "data/JAM/isimip_heat_drought/*.tif"
    return expand(
        "data/{ISO3}/isimip_heat_drought/{SLUG}__{ISO3}.tif",
        ISO3=wildcards.ISO3,
        SLUG=slugs,
    )


rule summarise_isimip:
    input:
        summarise_isimip_input,
    output:
        csv="data/{ISO3}/isimip_heat_drought.csv",
    run:
        paths = glob(f"data/{wildcards.ISO3}/isimip_heat_drought/*.tif")
        summary = pandas.DataFrame({"path": paths})
        # lange2020_hwmid-humidex_gfdl-esm2m_ewembi_rcp26_nosoc_co2_leh_global_annual_2006_2099_2030_occurrence.tif
        meta = summary.path.str.extract(
            r"lange2020_(?P<model>[^_]+)_(?P<gcm>[^_]+)_ewembi_(?P<rcp>[^_]+)_(?P<soc>[^_]+)_(?P<co2>[^_]+)_(?P<variable>[^_]+)_global_annual_\d+_\d+_(?P<epoch>[^_]+)"
        )


        def map_var(var):
            if var == "leh":
                return "extreme_heat"
            elif var == "led":
                return "drought"
            else:
                return "na"


        meta["hazard"] = meta.variable.apply(map_var)
        meta.drop(columns=["soc", "co2", "variable"], inplace=True)
        meta["path"] = summary.path
        meta.rcp = meta.rcp.str.replace("rcp26", "2.6").str.replace("rcp60", "6.0")
        meta.epoch = meta.epoch.str.replace("baseline", "2010")


        columns = ["hazard", "epoch", "rcp", "gcm", "model", "path"]
        meta = meta[columns].sort_values(by=columns)

        meta.to_csv(output.csv, index=False)


checkpoint download_isimip:
    output:
        tiffs=directory("incoming_data/isimip_heat_drought"),
    shell:
        """
        mkdir --parents incoming_data/isimip_heat_drought
        cd incoming_data/isimip_heat_drought
        zenodo_get 10.5281/zenodo.7732393

        # pick out occurrence files (ignore exposure)
        unzip lange2020_expected_occurrence.zip -d .
        mv lange2020_expected_occurrence/*_occurrence.tif .
        rm -r lange2020_expected_occurrence
        """


#
# JRC GHSL data
#
rule summarise_jrc_ghsl:
    input:
        tiffs=expand(
            "data/{{ISO3}}/jrc_ghsl/{DATASET}_E{EPOCH}_GLOBE_R2023A_{RESOLUTION}_V1_0__{{ISO3}}.tif",
            EPOCH=["2020", "2025"],
            RESOLUTION=["4326_30ss"],
            DATASET=[
                "GHS_POP",
                "GHS_BUILT_S",
                "GHS_BUILT_S_NRES",
            ],
        ),
        pdf="incoming_data/jrc_ghsl/GHSL_Data_Package_2023.pdf",
    output:
        csv="data/{ISO3}/jrc_ghsl.csv",
        pdf="data/{ISO3}/jrc_ghsl/GHSL_Data_Package_2023.pdf",
    run:
        shutil.copyfile(input.pdf, output.pdf)

        paths = glob(f"data/{wildcards.ISO3}/jrc_ghsl/*.tif")
        summary = pandas.DataFrame({"path": paths})

        meta = summary.path.str.extract(
            r"jrc_ghsl/(?P<dataset>.*)_E(?P<epoch>\d+)_GLOBE_(?P<release>[^_]+)_(?P<resolution>\w+)_V1_0"
        )
        meta["path"] = summary.path

        columns = ["dataset", "release", "resolution", "epoch", "path"]
        meta = meta[columns].sort_values(by=columns)

        meta.to_csv(output.csv, index=False)


rule download_jrc_ghsl_pop_built_s:
    # EPOCH available in: range(1975, 2031, 5)
    # RESOLUTION available in: 4326_3ss, 4326_30ss, 54009_100, 54009_1000
    # DATASET: GHS_POP, GHS_BUILT_S, GHS_BUILT_S_NRES, GHS_BUILT_V, GHS_BUILT_V_NRES
    # DATASET_PREFIX trims _NRES: GHS_POP, GHS_BUILT_S, GHS_BUILT_V
    output:
        tiff="incoming_data/jrc_ghsl/{DATASET}_E{EPOCH}_GLOBE_R2023A_{RESOLUTION}_V1_0.tif",
    params:
        DATASET_PREFIX=lambda wildcards, output: wildcards.DATASET.replace("_NRES", ""),
    shell:
        """
        wget --no-clobber --directory-prefix=./incoming_data/jrc_ghsl \
            https://jeodpp.jrc.ec.europa.eu/ftp/jrc-opendata/GHSL/{params.DATASET_PREFIX}_GLOBE_R2023A/{wildcards.DATASET}_E{wildcards.EPOCH}_GLOBE_R2023A_{wildcards.RESOLUTION}/V1-0/{wildcards.DATASET}_E{wildcards.EPOCH}_GLOBE_R2023A_{wildcards.RESOLUTION}_V1_0.zip
        unzip -n ./incoming_data/jrc_ghsl/{wildcards.DATASET}_E{wildcards.EPOCH}_GLOBE_R2023A_{wildcards.RESOLUTION}_V1_0.zip -d ./incoming_data/jrc_ghsl
        """


rule download_jrc_ghsl_docs:
    output:
        pdf="incoming_data/jrc_ghsl/GHSL_Data_Package_2023.pdf",
    shell:
        """
        wget --no-clobber --directory-prefix=./incoming_data/jrc_ghsl \
            https://ghsl.jrc.ec.europa.eu/documents/GHSL_Data_Package_2023.pdf
        """


#
# OpenStreetMap Planet
#
rule download_osm:
    output:
        pbf=protected("incoming_data/osm/planet-231106.osm.pbf"),
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


rule filter_osm_data:
    input:
        pbf="incoming_data/osm/planet-231106.osm.pbf",
    output:
        pbf="incoming_data/osm/planet-231106_{SECTOR}.osm.pbf",
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
        with open(output.json, 'w') as fh:
            fh.write(json)


rule extract_osm_data:
    input:
        pbf="incoming_data/osm/planet-231106_{SECTOR}.osm.pbf",
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
    # and return a full list of file paths under "data/JAM/storm/*.tif"
    iso3 = wildcards.ISO3
    clipped_tiffs = []
    for fname in original_storm_tiffs:
        slug = fname.replace("incoming_data/storm/", "").replace(".tif", "")
        clipped_tiffs.append(f"data/{iso3}/storm/{slug}__{iso3}.tif")
    return clipped_tiffs


rule summarise_storm:
    input:
        tiffs=summarise_storm_input,
    output:
        csv="data/{ISO3}/storm.csv",
    run:
        paths = glob(f"data/{wildcards.ISO3}/storm/*.tif")
        summary = pandas.DataFrame({"path": paths})
        # STORM_FIXED_RETURN_PERIODS_{STORM_MODEL}_{STORM_RP}_YR_RP.tif
        meta = summary.path.str.extract(
            r"STORM_FIXED_RETURN_PERIODS_(?P<gcm>[^_]+)_(?P<rp>[^_]+)_YR_RP"
        )


        def map_gcm_to_rcp(gcm):
            if gcm == "constant":
                return "historical"
            else:
                return "8.5"


        def map_gcm_to_epoch(gcm):
            if gcm == "constant":
                return "2010"
            else:
                return "2050"


        meta["hazard"] = "cyclone"
        meta["rcp"] = meta.gcm.apply(map_gcm_to_rcp)
        meta["epoch"] = meta.gcm.apply(map_gcm_to_epoch)
        meta["path"] = summary.path


        columns = ["hazard", "epoch", "rcp", "gcm", "path"]
        meta = meta[columns].sort_values(by=columns)

        meta.to_csv(output.csv, index=False)


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
        gpkg="incoming_data/wri_powerplants/wri-powerplants.gpkg",
    run:
        df = pandas.read_csv(input.csv)
        geoms = geopandas.points_from_xy(df.longitude, df.latitude)
        gdf = geopandas.GeoDataFrame(data=df, geometry=geoms, crs="EPSG:4326")
        gdf.to_file(output.gpkg, driver="GPKG", engine="pyogrio")
