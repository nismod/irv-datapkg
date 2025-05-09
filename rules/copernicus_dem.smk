#
# Copernicus DEM
#
rule list_dem_glo90:
    output:
        txt="incoming_data/copernicus_dem/COP-DEM_GLO-90-DGED__2023_1.txt",
    shell:
        """
        out_dir = $(dirname {output.txt})
        mkdir -p $out_dir

        curl -k -H "accept: csv" \
                    https://prism-dem-open.copernicus.eu/pd-desk-open-access/publicDemURLs/COP-DEM_GLO-90-DGED__2023_1 \
                    > {output.txt}
        """

rule download_dem_glo90:
    input:
        txt=rules.list_dem_glo90.output.txt
    output:
        dir=directory("incoming_data/copernicus_dem/archive"),
    shell:
        """
        mkdir -p {output.dir}.tmp
        pushd {output.dir}.tmp

        cat ../COP-DEM_GLO-90-DGED__2023_1.txt | parallel 'wget --no-clobber {{}}'

        popd
        mv {output.dir}.tmp {output.dir}
        """

rule extract_dem_glo90:
    input:
        dir=rules.download_dem_glo90.output.dir,
    output:
        dir=directory("incoming_data/copernicus_dem/tiles"),
    shell:
        """
        pushd incoming_data/copernicus_dem
            mkdir -p tiles

            # Extract
            find -type f -name '*.tar' | \
                head | \
                sed 's/.\\/archive\\///' | \
                sed 's/.tar//' | \
                parallel -j 1 \
                    tar xvf \
                        {{}}.tar \
                        --skip-old-files \
                        --strip-components=2 \
                        -C ./tiles/ \
                        {{}}/DEM/{{}}_DEM.tif
        popd
        """

rule convert_dem_glo90:
    input:
        dir=rules.extract_dem_glo90.output.dir,
    output:
        tiff="incoming_data/copernicus_dem/copernicus_dem.tif",
    shell:
        """
        pushd incoming_data/copernicus_dem
            # Build list
            find -type f -name '*.tif' > tileList.txt

            # Build VRT
            gdalbuildvrt -input_file_list tileList.txt copernicus_dem.vrt

            # Combine to big TIFF
            gdal_translate \
                -co "COMPRESS=LZW" \
                -co "TILED=yes" \
                -co "BIGTIFF=YES" \
                -of "GTiff" \
                copernicus_dem.vrt \
                copernicus_dem.tif
        popd
        """
