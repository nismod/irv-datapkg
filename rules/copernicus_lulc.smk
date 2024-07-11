#
# Copernicus LULC
#

rule download_lulc:
    output:
        zip="incoming_data/copernicus_lulc/archive.zip",
    run:
        path = os.path.join("incoming_data","copernicus_lulc")
        if not os.path.isdir(path):
            os.mkdir(path)

        from irv_datapkg import download_from_CDS
        download_from_CDS(
            "satellite-land-cover",
            "all",
            "zip",
            "v2.1.1",
            "2020",
            "incoming_data/copernicus_lulc/archive.zip")

rule convert_lulc:
    input:
        zip="incoming_data/copernicus_lulc/archive.zip",
    output:
        tif = "incoming_data/copernicus_lulc/C3S-LC-L4-LCCS-Map-300m-P1Y-2020-v2.1.1.tif",
    shell:
        """ 
        cd incoming_data/copernicus_lulc
        
        unzip archive.zip

        gdalwarp \
            -of Gtiff \
            -co COMPRESS=LZW \
            -ot Byte \
            -te -180.0000000 -90.0000000 180.0000000 90.0000000 \
            -tr 0.002777777777778 0.002777777777778 \
            -t_srs EPSG:4326 \
            NETCDF:C3S-LC-L4-LCCS-Map-300m-P1Y-2020-v2.1.1.nc:lccs_class \
            C3S-LC-L4-LCCS-Map-300m-P1Y-2020-v2.1.1.tif

        """