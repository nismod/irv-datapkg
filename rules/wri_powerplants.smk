#
# WRI Power Plants
#
rule download_powerplants:
    output:
        csv="incoming_data/wri_powerplants/global_power_plant_database.csv",
    shell:
        """
        mkdir -p incoming_data/wri_powerplants
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
