import shutil
from pathlib import Path
from glob import glob

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


rule all_compressed:
    input:
        expand("data/{ISO3}.zip", ISO3=BOUNDARIES.CODE_A3),


rule clean:
    shell:
        "rm -rf data"


rule zip:
    input:
        "data/{ISO3}/datapackage.json",
    output:
        "data/{ISO3}.zip",
    shell:
        """
        zip -r data/{ISO3}.zip data/{ISO3}
        """


rule datapackage:
    # required input must entail all the datapackage files
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


rule extract_osm_data:
    input:
        pbf="incoming_data/osm/planet-231106_{SECTOR}.osm.pbf",
    output:
        pbf="data/{ISO3}/openstreetmap/openstreetmap_{SECTOR}__{ISO3}.osm.pbf",
    params:
        bbox_str=boundary_bbox,
    shell:
        """
        osmium extract \
            --bbox {params.bbox_str} \
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
        gpkg="incoming_data/wri_powerplants/global_power_plant_database.gpkg",
    run:
        df = pandas.read_csv(input.csv)
        geoms = geopandas.points_from_xy(df.longitude, df.latitude)
        gdf = geopandas.GeoDataFrame(data=df, geometry=geoms, crs="EPSG:4326")
        gdf.to_file(output.gpkg, driver="GPKG", engine="pyogrio")
