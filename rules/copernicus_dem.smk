#
# Copernicus DEM
#
rule download_dem:
    output:
        dir=directory("incoming_data/copernicus_dem/glo-90"),
    shell:
        """
        mkdir -p incoming_data/copernicus_dem
        cd incoming_data/copernicus_dem
        aws s3 sync s3://copernicus-dem-90m/ --no-sign-request .
        """

rule convert_dem:
    input:
        dir="incoming_data/copernicus_dem/glo-90",
    output:
        tiff="incoming_data/copernicus_dem/glo-90/copernicus_dsm_cog_30_DEM.tif",
    shell:
        """
        cd incoming_data/copernicus_dem/glo-90
        gdalbuildvrt -input_file_list tiffs.txt copernicus_dsm_cog_30_DEM.vrt
        gdal_translate -co "COMPRESS=LZW" -co "TILED=yes" -co "BIGTIFF=YES" -of "GTiff" copernicus_dsm_cog_30_DEM.vrt copernicus_dsm_cog_30_DEM.tif
        """

