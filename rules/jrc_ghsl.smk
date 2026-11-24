#
# JRC Global Human Settlement Layer: population and built-up surface
#
# Collections: ghsl_pop, ghsl_built_s (metadata/jrc_ghsl.yml, T2 package "jrc_ghsl")
#
GHSL_URL = "https://jeodpp.jrc.ec.europa.eu/ftp/jrc-opendata/GHSL"
GHSL_RELEASE = "R2023A"
# EPOCH available in: range(1975, 2031, 5)
# RESOLUTION available in: 4326_3ss, 4326_30ss, 54009_100, 54009_1000
GHSL_EPOCHS = [2020, 2025]
GHSL_RESOLUTIONS = ["4326_30ss"]
# collection -> {upstream product: "use" property}; *_NRES is non-residential
GHSL_PRODUCTS = {
    "ghsl_pop": {"GHS_POP": None},
    "ghsl_built_s": {"GHS_BUILT_S": "total", "GHS_BUILT_S_NRES": "non_residential"},
}
GHSL_DOCS = "GHSL_Data_Package_2023.pdf"
GHSL_VERSION = COLLECTIONS["ghsl_pop"].version
if COLLECTIONS["ghsl_built_s"].version != GHSL_VERSION:
    raise ValueError("ghsl_t1 assumes ghsl_pop and ghsl_built_s share a version")


def ghsl_name(product, epoch, resolution):
    return f"{product}_E{epoch}_GLOBE_{GHSL_RELEASE}_{resolution}_V1_0"


def ghsl_items(collection):
    items = {}
    for product, use in GHSL_PRODUCTS[collection].items():
        for epoch in GHSL_EPOCHS:
            for resolution in GHSL_RESOLUTIONS:
                name = ghsl_name(product, epoch, resolution)
                props = {
                    "epoch": epoch,
                    "resolution": resolution,
                    "product": product,
                    "release": GHSL_RELEASE,
                    "upstream_name": name,
                }
                if use is not None:
                    props["use"] = use
                items[name.lower()] = T1Item(
                    ext="tif",
                    properties=props,
                    sources=[f"{raw_dir(collection)}/{name}.zip"],
                    datetime=f"{epoch}-01-01T00:00:00Z",
                )
    return items


register_t1("ghsl_pop", lru_cache(maxsize=None)(lambda: ghsl_items("ghsl_pop")))
register_t1("ghsl_built_s", lru_cache(maxsize=None)(lambda: ghsl_items("ghsl_built_s")))


rule ghsl_t0:
    """T0: one zip per product, epoch and resolution"""
    output:
        zip=f"{RAW}/{{collection}}/{GHSL_RELEASE}/{{product}}_E{{epoch}}_GLOBE_{GHSL_RELEASE}_{{resolution}}_V1_0.zip",
        provenance=f"{RAW}/{{collection}}/{GHSL_RELEASE}/{{product}}_E{{epoch}}_GLOBE_{GHSL_RELEASE}_{{resolution}}_V1_0.zip.provenance.json",
    wildcard_constraints:
        collection="ghsl_pop|ghsl_built_s",
        product="GHS_[A-Z_]+",
        epoch="[0-9]{4}",
    run:
        name = ghsl_name(wildcards.product, wildcards.epoch, wildcards.resolution)
        # the directory drops _NRES: GHS_BUILT_S_NRES lives under GHS_BUILT_S_GLOBE_R2023A
        folder = wildcards.product.replace("_NRES", "")
        url = f"{GHSL_URL}/{folder}_GLOBE_{GHSL_RELEASE}/{name.replace('_V1_0', '')}/V1-0/{name}.zip"
        recipes.download(url, output.zip)


rule ghsl_t1:
    """T1: COG of each layer, read straight from the zip"""
    input:
        zip=lambda wildcards: t1_item(wildcards.collection, wildcards.item).sources[0],
    output:
        tif=f"{CATALOGUE}/{{collection}}/{GHSL_VERSION}/{{item}}.tif",
    wildcard_constraints:
        collection="ghsl_pop|ghsl_built_s",
        item="ghs_[a-z0-9_]+",
    run:
        props = t1_item(wildcards.collection, wildcards.item).properties
        recipes.to_cog(
            recipes.vsizip(input.zip, basename=f"{props['upstream_name']}.tif"),
            output.tif,
            units=COLLECTIONS[wildcards.collection].units,
            overview_resampling="average",
        )


rule ghsl_docs_t0:
    """T0: GHSL Data Package documentation"""
    output:
        pdf=f"{raw_dir('ghsl_pop')}/{GHSL_DOCS}",
        provenance=f"{raw_dir('ghsl_pop')}/{GHSL_DOCS}.provenance.json",
    run:
        recipes.download(
            f"https://ghsl.jrc.ec.europa.eu/documents/{GHSL_DOCS}", output.pdf
        )


rule ghsl_docs_t2:
    """T2: the documentation travels with each country package"""
    input:
        pdf=rules.ghsl_docs_t0.output.pdf,
    output:
        pdf=f"data/{{ISO3}}/jrc_ghsl/{GHSL_DOCS}",
    shell:
        "cp {input.pdf} {output.pdf}"


T2_EXTRA["jrc_ghsl"] = lambda iso3: [f"data/{iso3}/jrc_ghsl/{GHSL_DOCS}"]
