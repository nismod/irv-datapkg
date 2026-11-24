#
# WRI Aqueduct Flood Hazard
#
# Collection: aqueduct_flood (metadata/aqueduct_flood.yml, T2 package "aqueduct_flood")
#
# The upstream bucket is mirrored as a whole, so the item list is only known after
# the T0 checkpoint has run: dry runs stop at aqueduct_flood_t0.
#
AQUEDUCT_RAW = raw_dir("aqueduct_flood")
AQUEDUCT_URL = "http://wri-projects.s3.amazonaws.com/AqueductFloodTool/download/v2"


def _aqueduct_rcp(value):
    # rcp4p5 -> "4.5"; "historical" stays
    return (
        value.replace("rcp", "").replace("p", ".") if value.startswith("rcp") else value
    )


def _aqueduct_epoch(value):
    return "baseline" if value in ("hist", "1980") else int(value)


def _aqueduct_rp(digits, tail):
    # "rp0001_5..." is the 1.5 year return period
    if tail and tail[0] == "5":
        return float(f"{int(digits)}.5"), tail[1:]
    return int(digits), tail


def aqueduct_flood_properties(slug):
    """Variant properties from an upstream file name (without .tif), or None.

    inunriver_{rcp}_{gcm}_{epoch}_rp{rp}
    inuncoast_{rcp}_{subsidence}_{epoch}_rp{rp}_{sea level rise...}
    """
    parts = slug.split("_")
    if len(parts) < 5 or not parts[4].startswith("rp") or not parts[4][2:].isdigit():
        return None
    kind, rcp, variant, epoch, rp = parts[:5]
    return_period, tail = _aqueduct_rp(rp[2:], parts[5:])
    try:
        epoch = _aqueduct_epoch(epoch)
    except ValueError:
        return None
    props = {"rcp": _aqueduct_rcp(rcp), "epoch": epoch, "return_period": return_period}
    if kind == "inunriver":
        props.update(hazard="river", gcm=variant.lstrip("0"), subsidence="na")
        props["sea_level_rise"] = "_".join(tail) or "na"
    elif kind == "inuncoast":
        props.update(hazard="coastal", gcm="na", subsidence=variant)
        props["sea_level_rise"] = "_".join(tail) or "na"
    else:
        return None
    props["upstream_name"] = slug
    return props


checkpoint aqueduct_flood_t0:
    """T0: mirror every GeoTIFF in the upstream bucket"""
    output:
        dir=directory(AQUEDUCT_RAW),
    params:
        provenance=provenance_cmd(
            "provenance.json", "*.tif", "aws s3 sync", url_prefix=AQUEDUCT_URL
        ),
    shell:
        """
        mkdir -p {output.dir}
        cd {output.dir}
        aws s3 sync --no-sign-request s3://wri-projects/AqueductFloodTool/download/v2/ . \
            --exclude '*' \
            --include '*.tif'
        {params.provenance}
        """


@lru_cache(maxsize=None)
def aqueduct_flood_items():
    raw = checkpoints.aqueduct_flood_t0.get().output.dir
    items = {}
    for slug in sorted(glob_wildcards(os.path.join(raw, "{slug,[^/]+}.tif")).slug):
        props = aqueduct_flood_properties(slug)
        if props is None:
            print(f"aqueduct_flood: skipping unrecognised file {slug}.tif")
            continue
        items[slug.lower()] = T1Item(
            ext="tif", properties=props, sources=[f"{raw}/{slug}.tif"]
        )
    return items


register_t1("aqueduct_flood", aqueduct_flood_items)


rule aqueduct_flood_t1:
    """T1: COG of each upstream GeoTIFF"""
    input:
        tif=lambda wildcards: t1_item("aqueduct_flood", wildcards.item).sources[0],
    output:
        tif=f"{t1_dir('aqueduct_flood')}/{{item}}.tif",
    wildcard_constraints:
        item="inun(river|coast)_[a-z0-9_.-]+",
    run:
        recipes.to_cog(
            input.tif, output.tif, units="m", descriptions="inundation depth"
        )
