#
# Copernicus DEM
#
rule download_dem:
    output:
        dir=directory("incoming_data/copernicus_dem"),
    shell:
        """
        mkdir -p incoming_data/copernicus_dem
        cd incoming_data/copernicus_dem
        aws s3 sync s3://copernicus-dem-90m/ --no-sign-request .
        """

rule convert_dem:
    input:
        txt="incoming_data/copernicus_dem/tileList.txt",
    output:
        tiff="incoming_data/copernicus_dem/copernicus_dem.tif",
    shell:
        """
        cd incoming_data/copernicus_dem
        gdalbuildvrt -input_file_list tileList.txt copernicus_dsm_cog_30_DEM.vrt
        gdal_translate \
            -co "COMPRESS=LZW" \
            -co "TILED=yes" \
            -co "BIGTIFF=YES" \
            -of "GTiff" \
            copernicus_dsm_cog_30_DEM.vrt \
            copernicus_dem.tif
        """

