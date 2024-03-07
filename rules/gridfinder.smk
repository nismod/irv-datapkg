#
# Gridfinder
#
rule download_gridfinder:
    output:
        grid="incoming_data/gridfinder/grid.gpkg",
        targets="incoming_data/gridfinder/targets.tif",
    shell:
        """
        mkdir --parents incoming_data/gridfinder
        cd incoming_data/gridfinder
        zenodo_get 10.5281/zenodo.3628142
        """
