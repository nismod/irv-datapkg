import pandas
import geopandas


def read_boundaries():
    return geopandas.read_file("bundled_data/ne_10m_admin_0_map_units_custom.gpkg")


rule all:
    input:
        expand("data/{ISO3}/datapackage.json", ISO3=read_boundaries().CODE_A3),


rule datapackage:
    output:
        json="data/{ISO3}/datapackage.json",
    script:
        "scripts/generate_datapackage_json.py"


#
# WRI Aqueduct Flood Hazard
#
rule download_aqueduct_coastal_all:
    # SUBSIDENCE default to with-subsidence, wtsub
    # SLR default to 95th percentile sea-level rise
    input:
        tiffs=expand(
            "incoming_data/aqueduct/inuncoast_{RCP}_{SUBSIDENCE}_{YEAR}_{RP}_{SLR}.tif",
            RCP=["historical"],
            SUBSIDENCE=["wtsub"],
            YEAR=["hist"],
            RP=[
            f"rp{str(rp).zfill(4)}"
                for rp in (2, 5, 10, 25, 50, 100, 250, 500, 1000)
            ],
            SLR=["0"],
        )
        + expand(
            "incoming_data/aqueduct/inuncoast_{RCP}_{SUBSIDENCE}_{YEAR}_{RP}_{SLR}.tif",
            RCP=["rcp4p5", "rcp8p5"],
            SUBSIDENCE=["wtsub"],
            YEAR=[2030, 2050, 2080],
            RP=[
            f"rp{str(rp).zfill(4)}"
                for rp in (2, 5, 10, 25, 50, 100, 250, 500, 1000)
            ],
            SLR=["0"],
        ),


rule download_aqueduct_coastal:
    output:
        tiff="incoming_data/aqueduct/inuncoast_{RCP}_{SUBSIDENCE}_{YEAR}_{RP}_{SLR}.tif",
    shell:
        """
        mkdir -p ./incoming_data/aqueduct
        wget -nc -P ./incoming_data/aqueduct http://wri-projects.s3.amazonaws.com/AqueductFloodTool/download/v2/inuncoast_{wildcards.RCP}_{wildcards.SUBSIDENCE}_{wildcards.YEAR}_{wildcards.RP}_{wildcards.SLR}.tif
        """


rule download_aqueduct_river_all:
    input:
        tiffs=expand(
            "incoming_data/aqueduct/inunriver_{RCP}_{GCM}_{YEAR}_{RP}.tif",
            RCP=["historical"],
            GCM=["WATCH".zfill(14)],
            YEAR=["hist"],
            RP=[
            f"rp{str(rp).zfill(4)}"
                for rp in (2, 5, 10, 25, 50, 100, 250, 500, 1000)
            ],
        )
        + expand(
            "incoming_data/aqueduct/inunriver_{RCP}_{GCM}_{YEAR}_{RP}.tif",
            RCP=["rcp4p5", "rcp8p5"],
            GCM=[
                gcm.zfill(14)
                for gcm in (
                    "NorESM1-M",
                    "GFDL_ESM2M",
                    "HadGEM2-ES",
                    "IPSL-CM5A-LR",
                    "MIROC-ESM-CHEM",
                )
            ],
            YEAR=[2030, 2050, 2080],
            RP=[
            f"rp{str(rp).zfill(4)}"
                for rp in (2, 5, 10, 25, 50, 100, 250, 500, 1000)
            ],
        ),


rule download_aqueduct_river:
    output:
        tiff="incoming_data/aqueduct/inunriver_{RCP}_{GCM}_{YEAR}_{RP}.tif",
    shell:
        """
        mkdir -p ./incoming_data/aqueduct
        wget -nc -P ./incoming_data/aqueduct http://wri-projects.s3.amazonaws.com/AqueductFloodTool/download/v2/inunriver_{wildcards.RCP}_{wildcards.GCM}_{wildcards.YEAR}_{wildcards.RP}.tif
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
        mkdir -p incoming_data/gridfinder
        cd incoming_data/gridfinder
        zenodo_get 10.5281/zenodo.3628142
        """


#
# Extreme heat/drought
#
rule download_isimip:
    # The first two blocks expand to the extreme heat TIFFs
    #
    # Most drought models have all four GCMs and use "2005soc" in future
    # "h08", "lpjml", "pcr-globwb", "watergap2" use "histsoc" for baseline
    # "clm45" uses "2005soc" for baseline
    # MPI-HM has no "hadgem2-es"
    # Jules-W1 and Orchidee use "nosoc"
    output:
        tiffs=expand(
            "data/lange2020_hwmid-humidex_{GCM}_ewembi_{RCP}_nosoc_co2_leh_global_annual_2006_2099_{EPOCH}_{METRIC}.tif",
            GCM=["gfdl-esm2m", "hadgem2-es", "ipsl-cm5a-lr", "miroc5"],
            RCP=["rcp26", "rcp60"],
            EPOCH=["2030", "2050", "2080"],
            METRIC=["occurrence", "exposure"],
        )
        + expand(
            "data/lange2020_hwmid-humidex_{GCM}_ewembi_historical_nosoc_co2_leh_global_annual_1861_2005_{EPOCH}_{METRIC}.tif",
            GCM=["gfdl-esm2m", "hadgem2-es", "ipsl-cm5a-lr", "miroc5"],
            EPOCH=["baseline"],
            METRIC=["occurrence", "exposure"],
        )
        + expand(
            "data/lange2020_{MODEL}_{GCM}_ewembi_{RCP}_2005soc_co2_led_global_annual_2006_2099_{EPOCH}_{METRIC}.tif",
            MODEL=["clm45", "h08", "lpjml", "pcr-globwb", "watergap2"],
            GCM=["gfdl-esm2m", "hadgem2-es", "ipsl-cm5a-lr", "miroc5"],
            RCP=["rcp26", "rcp60"],
            EPOCH=["2030", "2050", "2080"],
            METRIC=["occurrence", "exposure"],
        )
        + expand(
            "data/lange2020_{MODEL}_{GCM}_ewembi_historical_histsoc_co2_led_global_annual_1861_2005_{EPOCH}_{METRIC}.tif",
            MODEL=["h08", "lpjml", "pcr-globwb", "watergap2"],
            GCM=["gfdl-esm2m", "hadgem2-es", "ipsl-cm5a-lr", "miroc5"],
            EPOCH=["baseline"],
            METRIC=["occurrence", "exposure"],
        )
        + expand(
            "data/lange2020_{MODEL}_{GCM}_ewembi_historical_2005soc_co2_led_global_annual_1861_2005_{EPOCH}_{METRIC}.tif",
            MODEL=["clm45"],
            GCM=["gfdl-esm2m", "hadgem2-es", "ipsl-cm5a-lr", "miroc5"],
            EPOCH=["baseline"],
            METRIC=["occurrence", "exposure"],
        )
        + expand(
            "data/lange2020_{MODEL}_{GCM}_ewembi_{RCP}_2005soc_co2_led_global_annual_2006_2099_{EPOCH}_{METRIC}.tif",
            MODEL=["mpi-hm"],
            GCM=["gfdl-esm2m", "ipsl-cm5a-lr", "miroc5"],
            RCP=["rcp26", "rcp60"],
            EPOCH=["2030", "2050", "2080"],
            METRIC=["occurrence", "exposure"],
        )
        + expand(
            "data/lange2020_{MODEL}_{GCM}_ewembi_historical_histsoc_co2_led_global_annual_1861_2005_{EPOCH}_{METRIC}.tif",
            MODEL=["mpi-hm"],
            GCM=["gfdl-esm2m", "ipsl-cm5a-lr", "miroc5"],
            EPOCH=["baseline"],
            METRIC=["occurrence", "exposure"],
        )
        + expand(
            "data/lange2020_{MODEL}_{GCM}_ewembi_{RCP}_nosoc_co2_led_global_annual_2006_2099_{EPOCH}_{METRIC}.tif",
            MODEL=["jules-w1", "orchidee"],
            GCM=["gfdl-esm2m", "hadgem2-es", "ipsl-cm5a-lr", "miroc5"],
            RCP=["rcp26", "rcp60"],
            EPOCH=["2030", "2050", "2080"],
            METRIC=["occurrence", "exposure"],
        )
        + expand(
            "data/lange2020_{MODEL}_{GCM}_ewembi_historical_nosoc_co2_led_global_annual_1861_2005_{EPOCH}_{METRIC}.tif",
            MODEL=["jules-w1", "orchidee"],
            GCM=["gfdl-esm2m", "hadgem2-es", "ipsl-cm5a-lr", "miroc5"],
            EPOCH=["baseline"],
            METRIC=["occurrence", "exposure"],
        ),
    shell:
        """
        mkdir -p incoming_data/isimip
        cd incoming_data/isimip
        zenodo_get 10.5281/zenodo.7732393
        """


#
# JRC GHSL data
#
rule download_population_all:
    # EPOCH available in: range(1975, 2031, 5)
    # RESOLUTION available in: 4326_3ss, 4326_30ss, 54009_100, 54009_1000
    input:
        expand(
            "incoming_data/ghsl/GHS_POP_E{EPOCH}_GLOBE_R2023A_{RESOLUTION}_V1_0.tif",
            EPOCH=[2020],
            RESOLUTION=["4326_30ss"],
        ),


rule download_population:
    output:
        tiff="incoming_data/ghsl/GHS_POP_E{EPOCH}_GLOBE_R2023A_{RESOLUTION}_V1_0.tif",
    shell:
        """
        mkdir -p ./incoming_data/ghsl
        wget -nc -P ./incoming_data/ghsl \
        https://jeodpp.jrc.ec.europa.eu/ftp/jrc-opendata/GHSL/GHS_POP_GLOBE_R2023A/GHS_POP_E{wildcards.EPOCH}_GLOBE_R2023A_{wildcards.RESOLUTION}/V1-0/GHS_POP_E{wildcards.EPOCH}_GLOBE_R2023A_{wildcards.RESOLUTION}_V1_0.zip
        unzip {input} -d ./incoming_data/ghsl
        """


#
# OpenStreetMap Planet
#
rule download_osm:
    output:
        pbf="incoming_data/osm/planet-231106.osm.pbf",
    shell:
        """
        mkdir -p incoming_data/osm
        aws s3 cp --no-sign-request s3://osm-planet-eu-central-1/planet/pbf/2023/planet-231106.osm.pbf ./incoming_data/osm/
        aws s3 cp --no-sign-request s3://osm-planet-eu-central-1/planet/pbf/2023/planet-231106.osm.pbf.md5 ./incoming_data/osm/
        md5sum -c ./incoming_data/osm/planet-231106.osm.pbf
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
        mkdir -p incoming_data/storm
        cd incoming_data/storm
        zenodo_get 10.5281/zenodo.7438145
        """


#
# WRI Power Plants
#
rule download_powerplants:
    output:
        csv="input/powerplants/global_power_plant_database.csv",
    shell:
        """
        mkdir -p incoming_data/powerplants
        cd incoming_data/powerplants
        wget https://wri-dataportal-prod.s3.amazonaws.com/manual/global_power_plant_database_v_1_3.zip
        unzip -o global_power_plant_database_v_1_3.zip
        """
