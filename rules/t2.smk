#
# T2: per-country extracts of T1 items, using the catalogue client ``extract``
#
# The t2: block of each metadata/*.yml chooses items, file names, clipping and
# the summary CSV (see src/irv_datapkg/recipes/extracts.py). Outputs:
#
#   data/{ISO3}/{package}/{stem}__{ISO3}.tif|.gpkg|.parquet
#   data/{ISO3}/{package}.csv           (summary of files and item properties)
#   data/{ISO3}/md5sum.txt, data/{ISO3}/datapackage.json
#
T2_PACKAGES = [name for name, pkg in PACKAGES.items() if pkg.in_t2]
T2_SUMMARY_PACKAGES = [name for name in T2_PACKAGES if PACKAGES[name].t2["summary"]]


@lru_cache(maxsize=None)
def t2_plan(package, iso3):
    """{relative path: [(collection, item_id, layer)]} for one country package.

    Cached: only called once checkpoints the T1 items depend on have completed
    (an incomplete checkpoint raises before anything is cached).
    """
    pkg = PACKAGES[package]
    items = [
        (coll.id, item_id, spec)
        for coll in pkg.collections
        for item_id, spec in sorted(t1_items(coll.id).items())
    ]
    return recipes.plan(package, pkg.t2, items, iso3, boundary_adm0_a3(iso3))


def t2_files(package, iso3):
    return [f"data/{iso3}/{rel}" for rel in t2_plan(package, iso3)]


def t2_package_targets(package, iso3):
    """What a country package needs from one T2 package resource."""
    extra = T2_EXTRA.get(package, lambda iso3: [])(iso3)
    if PACKAGES[package].t2["summary"]:
        return [f"data/{iso3}/{package}.csv"] + extra
    return t2_files(package, iso3) + extra


def _t2_members(wildcards):
    rel = f"{wildcards.package}/{wildcards.stem}__{wildcards.ISO3}.{wildcards.ext}"
    try:
        return t2_plan(wildcards.package, wildcards.ISO3)[rel]
    except KeyError:
        raise ValueError(
            f"data/{wildcards.ISO3}/{rel} is not part of the T2 plan"
        ) from None


rule t2_extract:
    """Clip T1 item(s) to a country (bbox, polygon or whole item)"""
    input:
        item_jsons=lambda wildcards: [
            t1_json(coll, item_id) for coll, item_id, _ in _t2_members(wildcards)
        ],
    output:
        path="data/{ISO3}/{package}/{stem}__{ISO3}.{ext}",
    wildcard_constraints:
        package="|".join(T2_PACKAGES),
        stem="[^/]+",
        ext="tif|gpkg|parquet",
    run:
        members = _t2_members(wildcards)
        recipes.extract_region(
            list(input.item_jsons),
            output.path,
            geometry=boundary_geom(wildcards.ISO3),
            clip=PACKAGES[wildcards.package].t2["clip"],
            layers=[layer for _, _, layer in members],
        )


rule t2_summary:
    """List a package resource's files with their T1 item properties"""
    input:
        files=lambda wildcards: t2_files(wildcards.package, wildcards.ISO3),
    output:
        csv="data/{ISO3}/{package}.csv",
    wildcard_constraints:
        package="|".join(T2_SUMMARY_PACKAGES),
    run:
        t2 = PACKAGES[wildcards.package].t2
        entries = []
        for rel, members in t2_plan(wildcards.package, wildcards.ISO3).items():
            for coll, item_id, layer in members:
                props = t1_item(coll, item_id).properties
                entries.append((rel, layer if len(members) > 1 else None, props))
        recipes.write_summary(
            output.csv, entries, t2["summary_columns"], t2["baseline_epoch"]
        )


rule region_boundary:
    """The country boundary each package is clipped to"""
    output:
        json="data/{ISO3}/boundary__{ISO3}.geojson",
    run:
        geom = boundary_geom(wildcards.ISO3)
        feature = '{"type":"Feature","geometry": %s}' % shapely.to_geojson(geom)
        with open(output.json, "w") as fh:
            fh.write(feature)


#
# Data package
#
rule datapackage:
    input:
        checksums="data/{ISO3}/md5sum.txt",
    output:
        json="data/{ISO3}/datapackage.json",
    script:
        "../scripts/generate_datapackage_json.py"


rule checksums:
    # input must require all the data package files
    # - summary CSVs require their files in turn
    input:
        lambda wildcards: [f"data/{wildcards.ISO3}/boundary__{wildcards.ISO3}.geojson"]
        + [
            path
            for package in T2_PACKAGES
            for path in t2_package_targets(package, wildcards.ISO3)
        ],
    output:
        checksums="data/{ISO3}/md5sum.txt",
    shell:
        """
        cd data/{wildcards.ISO3}
        md5sum **/*.* | grep "tif\\|gpkg\\|parquet" | sort -k 2 >md5sum.txt
        """
