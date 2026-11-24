#
# T1: register standardised files as STAC items in the local catalogue
#
# Each recipe writes {catalogue_base}/{dataset}/{version}/{item}.tif|.parquet and
# declares the item (properties, T0 sources) with register_t1(). This generic rule
# turns each file into {item}.json, creating or updating the collection from
# metadata/*.yml. Targets: all_t1 (everything, then validates the catalogue) or
# all_t1_{dataset} (one dataset). --config validate_deep=true makes all_t1 run
# the deep checks too (STAC schemas, COG/GeoParquet profiles, sha256; docs/qa.md).
#


def _t1_register_input(wildcards):
    coll = COLLECTIONS[wildcards.dataset]
    if wildcards.version != coll.version:
        raise ValueError(
            f"{wildcards.dataset} is at version {coll.version}, not {wildcards.version}"
        )
    spec = t1_item(wildcards.dataset, wildcards.item)
    return {"data": t1_data(wildcards.dataset, wildcards.item), "sources": spec.sources}


def _t1_register(wildcards, input, output):
    coll = COLLECTIONS[wildcards.dataset]
    spec = t1_item(wildcards.dataset, wildcards.item)
    item_url = recipes.register_item(
        CATALOGUE, coll, wildcards.item, spec, data_path=input.data
    )
    if os.path.abspath(item_url) != os.path.abspath(output.json):
        raise ValueError(f"Registered {item_url}, expected {output.json}")


rule t1_register:
    """Register a public T1 file as a STAC item"""
    input:
        unpack(_t1_register_input),
    output:
        json=f"{CATALOGUE}/{{dataset}}/{{version}}/{{item}}.json",
    wildcard_constraints:
        dataset="|".join(c.id for c in COLLECTIONS.values() if c.access == "public"),
        version="[^/]+",
        item="[^/]+",
    run:
        _t1_register(wildcards, input, output)


rule t1_register_private:
    """Register a private T1 file as a STAC item (under {catalogue_base}-private)"""
    input:
        unpack(_t1_register_input),
    output:
        json=f"{CATALOGUE}-private/{{dataset}}/{{version}}/{{item}}.json",
    wildcard_constraints:
        dataset="|".join(c.id for c in COLLECTIONS.values() if c.access == "private")
        or "^$",
        version="[^/]+",
        item="[^/]+",
    run:
        _t1_register(wildcards, input, output)


def _t1_targets_fn(dataset):
    return lambda wildcards: t1_targets(dataset)


# One target per dataset, so a checkpoint in one dataset (aqueduct_flood,
# isimip_heat_drought) only defers that dataset's jobs
for _dataset in T1_ITEMS:

    rule:
        name:
            f"all_t1_{_dataset}"
        input:
            _t1_targets_fn(_dataset),
        output:
            touch(f"{build_dir(_dataset)}/t1.done"),


rule all_t1:
    """T1: every registered dataset, then validate the local catalogue"""
    input:
        [f"{build_dir(dataset)}/t1.done" for dataset in T1_ITEMS],
    run:
        from irv_datapkg.catalog import validate_catalog

        warnings = []
        errors = validate_catalog(
            CATALOGUE,
            deep=bool(config.get("validate_deep", False)),
            warnings=warnings,
        )
        for warning in warnings:
            print(f"warning: {warning}")
        for error in errors:
            print(error)
        if errors:
            raise ValueError(f"{len(errors)} catalogue problems in {CATALOGUE}")
