#
# WRI Global Power Plant Database
#
# Collection: wri_powerplants (metadata/wri_powerplants.yml, T2 package "wri_powerplants")
#
WRI_POWERPLANTS_RAW = raw_dir("wri_powerplants")
WRI_POWERPLANTS_URL = "https://wri-dataportal-prod.s3.amazonaws.com/manual/global_power_plant_database_v_1_3.zip"
WRI_POWERPLANTS_ZIP = f"{WRI_POWERPLANTS_RAW}/global_power_plant_database_v_1_3.zip"


@lru_cache(maxsize=None)
def wri_powerplants_items():
    return {"wri-powerplants": T1Item(ext="parquet", sources=[WRI_POWERPLANTS_ZIP])}


register_t1("wri_powerplants", wri_powerplants_items)


rule wri_powerplants_t0:
    """T0: the database zip"""
    output:
        zip=WRI_POWERPLANTS_ZIP,
        provenance=f"{WRI_POWERPLANTS_ZIP}.provenance.json",
    run:
        recipes.download(WRI_POWERPLANTS_URL, output.zip)


rule wri_powerplants_t1:
    """T1: power plant points to GeoParquet, read straight from the zip"""
    input:
        zip=WRI_POWERPLANTS_ZIP,
    output:
        parquet=f"{t1_dir('wri_powerplants')}/wri-powerplants.parquet",
    run:
        import zipfile

        member = recipes.archive_member(
            input.zip, "global_power_plant_database.csv"
        )
        with zipfile.ZipFile(input.zip) as zf, zf.open(member) as fh:
            df = pandas.read_csv(fh, low_memory=False)
        recipes.points_to_geoparquet(
            df, output.parquet, x="longitude", y="latitude"
        )
