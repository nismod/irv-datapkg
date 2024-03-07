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
