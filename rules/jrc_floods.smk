#
# JRC global flood hazard maps
#
rule download_jrc_flood:
    output:
        zip="incoming_data/jrc_floods/floodMapGL_rp{RP}y.zip"
    shell:
        """
        output_dir=$(dirname {output.zip})

        wget -q -nc \
            --directory-prefix=$output_dir \
            https://cidportal.jrc.ec.europa.eu/ftp/jrc-opendata/FLOODS/GlobalMaps/floodMapGL_rp{wildcards.RP}y.zip
        """

rule extract_jrc_flood:
    input:
        tiff="incoming_data/jrc_floods/floodMapGL_rp{RP}y.zip"
    output:
        tiff="incoming_data/jrc_floods/floodMapGL_rp{RP}y.tif"
    shell:
        """
        output_dir=$(dirname {output.tiff})
        unzip $output_dir/floodMapGL_rp{wildcards.RP}y.zip floodMapGL_rp{wildcards.RP}y.tif -d $output_dir
        """

rule summarise_jrc_flood:
    input:
        tiffs=expand("data/{{ISO3}}/jrc_floods/floodMapGL_rp{RP}y__{{ISO3}}.tif", RP=[10, 20, 50, 100, 200, 500])
    output:
        csv="data/{ISO3}/jrc_floods.csv",
    run:
        paths = glob(f"data/{wildcards.ISO3}/jrc_floods/*.tif")
        summary = pandas.DataFrame({"path": paths})

        meta = summary.path.str.extract(
            r"jrc_floods/floodMapGL_rp(?P<rp>\d+)y"
        )
        meta["path"] = summary.path

        columns = ["rp", "path"]
        meta = meta[columns].sort_values(by=columns)

        meta.to_csv(output.csv, index=False)
