rule download_gem:
    output:
        tiff="incoming_data/gem_earthquake/v2023_1_pga_475_rock_3min.tif"),
    shell:
        """
        mkdir -p incoming_data/gem_earthquake
        cd incoming_data/gem_earthquake
        zenodo_get 10.5281/zenodo.8409647

        unzip GEM-GSHM_PGA-475y-rock_v2023.zip -d .
        """
