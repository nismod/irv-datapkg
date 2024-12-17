#
# Copernicus LULC
#

rule download_lulc:
    output:
        archive="incoming_data/copernicus_lulc/archive.tgz",
    run:
        from irv_datapkg import download_from_CDS
        download_from_CDS(
            "satellite-land-cover",
            {
                'variable': 'all',
                'year': ['2022'],
                'version': ['v2_1_1'],
                'format': 'tgz'
            },
            output.archive
        )

rule convert_lulc:
    input:
        archive=rules.download_lulc.output.archive,
    output:
        tif = "incoming_data/copernicus_lulc/copernicus_lulc.tif",
    shell:
        """
        cd incoming_data/copernicus_lulc

        tar xvzf $(basename {input.archive})

        gdalwarp \
            -of Gtiff \
            -co COMPRESS=LZW \
            -ot Byte \
            -te -180.0000000 -90.0000000 180.0000000 90.0000000 \
            -tr 0.002777777777778 0.002777777777778 \
            -t_srs EPSG:4326 \
            NETCDF:C3S-LC-L4-LCCS-Map-300m-P1Y-2022-v2.1.1.nc:lccs_class \
            copernicus_lulc.tif
        """
