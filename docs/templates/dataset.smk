#
# <Dataset title>
#
# Collection: <dataset_id> (metadata/<package>.yml, T2 package "<package>")
#
# Copy to rules/<dataset_id>.smk, replace <...>, add an include line to the
# Snakefile (with the other dataset recipes) and see docs/recipes.md.
#
DATASET_RAW = raw_dir("<dataset_id>")  # {raw_dir}/<dataset_id>/<upstream version>
DATASET_URL = "https://example.org/<upstream>/<version>"
DATASET_VARIANTS = [10, 100]  # e.g. return periods


@lru_cache(maxsize=None)
def dataset_id_items():
    """Every T1 item: {item_id: T1Item}. Properties must include each name in
    the collection's `dimensions`; sources are the T0 files they come from."""
    items = {}
    for rp in DATASET_VARIANTS:
        name = f"upstream_name_rp{rp}"
        items[name.lower()] = T1Item(
            ext="tif",  # or "parquet"
            properties={
                "hazard": "<hazard>",
                "return_period": rp,
                "upstream_name": name,
            },
            sources=[f"{DATASET_RAW}/{name}.zip"],
        )
    return items


register_t1("<dataset_id>", dataset_id_items)


rule dataset_id_t0:
    """T0: download verbatim, with a provenance sidecar"""
    output:
        zip=f"{DATASET_RAW}/{{name}}.zip",
        provenance=f"{DATASET_RAW}/{{name}}.zip.provenance.json",
    run:
        # writes <file>.provenance.json (source URL, sha256, size, date, tool)
        recipes.download(f"{DATASET_URL}/{wildcards.name}.zip", output.zip)


rule dataset_id_t1:
    """T1: standardise to COG on the source grid (or GeoParquet for vectors)"""
    input:
        zip=lambda wildcards: t1_item("<dataset_id>", wildcards.item).sources[0],
    output:
        tif=f"{t1_dir('<dataset_id>')}/{{item}}.tif",
    wildcard_constraints:
        item="upstream_name_rp[0-9]+",
    run:
        name = t1_item("<dataset_id>", wildcards.item).properties["upstream_name"]
        recipes.to_cog(
            recipes.vsizip(input.zip, basename=f"{name}.tif"),
            output.tif,
            units="m",
            descriptions="<what the values are>",
        )
        # vectors: recipes.to_geoparquet(src, output.parquet)
        # NetCDF:  recipes.netcdf_to_cog(nc, "variable", output.tif, crs="EPSG:4326")
        # Registration as a STAC item (t1_register) and T2 extracts are generic.
