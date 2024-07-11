#
# Download GHS population data (epoch: 2020, resolution: 3 arcsec, coordinate system: WGS84)
#

rule pop_ghs_download:
    output:
        zip="incoming_data/ghs_pop/GHS_POP_E2020_GLOBE_R2023A_4326_3ss_V1_0.tif"
    shell:
        """
        mkdir -p incoming_data/ghs_pop
        cd incoming_data/ghs_pop
        wget https://jeodpp.jrc.ec.europa.eu/ftp/jrc-opendata/GHSL/GHS_POP_GLOBE_R2023A/GHS_POP_E2020_GLOBE_R2023A_4326_3ss/V1-0/GHS_POP_E2020_GLOBE_R2023A_4326_3ss_V1_0.zip"
        unzip GHS_POP_E2020_GLOBE_R2023A_4326_3ss_V1_0.zip
        """

rule pop_ghs_clip:
    input:
        tif="incoming_data/ghs_pop/GHS_POP_E2020_GLOBE_R2023A_4326_3ss_V1_0.tif",
        bounds="data/{ISO3}/boundaries__{ISO3}.gpkg"
    output:
        tif="data/{ISO3}/ghs_pop__{ISO3}.tif",
    shell:
        """
        gdalwarp \
            -co COMPRESS=LZW \
            -cutline {input.bounds} \
            -cl boundaries__{wildcards.ISO3} \
            -crop_to_cutline \
            {input.tif} \
            {output.tif}
        """

