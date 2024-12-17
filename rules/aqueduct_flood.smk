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
        mkdir -p incoming_data/aqueduct_flood
        cd incoming_data/aqueduct_flood
        aws s3 sync --no-sign-request s3://wri-projects/AqueductFloodTool/download/v2/ . \
            --exclude '*' \
            --include '*.tif'
        """
