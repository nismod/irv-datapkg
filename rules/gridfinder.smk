#
# Gridfinder predicted power grid and electrification targets
#
# Collection: gridfinder (metadata/gridfinder.yml, T2 package "gridfinder")
#
GRIDFINDER_RAW = raw_dir("gridfinder")
GRIDFINDER_ZENODO = "3628142"
GRIDFINDER_FILES = {"grid": "grid.gpkg", "targets": "targets.tif", "lv": "lv.tif"}


@lru_cache(maxsize=None)
def gridfinder_items():
    return {
        item_id: T1Item(
            ext="parquet" if fname.endswith(".gpkg") else "tif",
            properties={"layer": item_id},
            sources=[f"{GRIDFINDER_RAW}/{fname}"],
        )
        for item_id, fname in GRIDFINDER_FILES.items()
    }


register_t1("gridfinder", gridfinder_items)


rule gridfinder_t0:
    """T0: the Zenodo record"""
    output:
        files=[f"{GRIDFINDER_RAW}/{fname}" for fname in GRIDFINDER_FILES.values()],
        provenance=f"{GRIDFINDER_RAW}/provenance.json",
    params:
        doi=f"10.5281/zenodo.{GRIDFINDER_ZENODO}",
        provenance=provenance_cmd(
            "provenance.json",
            " ".join(GRIDFINDER_FILES.values()),
            "zenodo_get",
            url_prefix=f"https://zenodo.org/records/{GRIDFINDER_ZENODO}/files",
        ),
    shell:
        """
        mkdir -p $(dirname {output.provenance})
        cd $(dirname {output.provenance})
        zenodo_get {params.doi}
        {params.provenance}
        """


rule gridfinder_t1_vector:
    """T1: predicted grid lines to GeoParquet"""
    input:
        gpkg=f"{GRIDFINDER_RAW}/grid.gpkg",
    output:
        parquet=f"{t1_dir('gridfinder')}/grid.parquet",
    run:
        recipes.to_geoparquet(input.gpkg, output.parquet)


rule gridfinder_t1_raster:
    """T1: targets and low-voltage rasters to COG"""
    input:
        tif=f"{GRIDFINDER_RAW}/{{item}}.tif",
    output:
        tif=f"{t1_dir('gridfinder')}/{{item}}.tif",
    wildcard_constraints:
        item="targets|lv",
    run:
        units = {"targets": None, "lv": "km"}[wildcards.item]
        recipes.to_cog(input.tif, output.tif, units=units)
