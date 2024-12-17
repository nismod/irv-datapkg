
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
        mkdir -p incoming_data/isimip_heat_drought
        cd incoming_data/isimip_heat_drought
        zenodo_get 10.5281/zenodo.7732393

        # pick out occurrence files (ignore exposure)
        unzip lange2020_expected_occurrence.zip -d .
        mv lange2020_expected_occurrence/*_occurrence.tif .
        rm -r lange2020_expected_occurrence
        """
