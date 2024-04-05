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
                + list(range(1000, 10001, 1000))
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
