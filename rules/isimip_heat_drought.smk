#
# ISIMIP extreme heat and drought annual expected occurrence
#
# Collection: isimip_heat_drought (metadata/isimip_heat_drought.yml)
#
# Items are listed from the zip downloaded by the T0 checkpoint: dry runs stop at
# isimip_heat_drought_t0.
#
ISIMIP_RAW = raw_dir("isimip_heat_drought")
ISIMIP_ZENODO = "7732393"
ISIMIP_RE = (
    r"lange2020_(?P<model>[^_]+)_(?P<gcm>[^_]+)_ewembi_(?P<rcp>[^_]+)_(?P<soc>[^_]+)"
    r"_(?P<co2>[^_]+)_(?P<variable>[^_]+)_global_annual_\d+_\d+_(?P<epoch>[^_]+)_occurrence$"
)
ISIMIP_HAZARDS = {"leh": "extreme_heat", "led": "drought"}


def isimip_heat_drought_properties(name):
    """Variant properties from an upstream file name (without .tif), or None.

    e.g. lange2020_hwmid-humidex_gfdl-esm2m_ewembi_rcp26_nosoc_co2_leh_global_annual_2006_2099_2030_occurrence
    """
    import re

    m = re.match(ISIMIP_RE, name)
    if m is None or m["variable"] not in ISIMIP_HAZARDS:
        return None
    rcp = m["rcp"]
    if rcp.startswith("rcp") and rcp[3:].isdigit():
        rcp = f"{rcp[3]}.{rcp[4:]}"  # rcp26 -> 2.6, rcp60 -> 6.0
    epoch = m["epoch"]
    return {
        "hazard": ISIMIP_HAZARDS[m["variable"]],
        "rcp": rcp,
        "epoch": int(epoch) if epoch.isdigit() else epoch,
        "gcm": m["gcm"],
        "model": m["model"],
        "upstream_name": name,
    }


checkpoint isimip_heat_drought_t0:
    """T0: the Zenodo record (occurrence and exposure archives)"""
    output:
        zip=f"{ISIMIP_RAW}/lange2020_expected_occurrence.zip",
        provenance=f"{ISIMIP_RAW}/provenance.json",
    params:
        doi=f"10.5281/zenodo.{ISIMIP_ZENODO}",
        provenance=provenance_cmd(
            "provenance.json",
            "$(sed 's#?.*##; s#.*/##' links.txt)",
            "zenodo_get",
            url_prefix=f"https://zenodo.org/records/{ISIMIP_ZENODO}/files",
        ),
    shell:
        """
        mkdir -p $(dirname {output.zip})
        cd $(dirname {output.zip})
        zenodo_get -w links.txt {params.doi}
        wget -nc -i links.txt
        {params.provenance}
        """


@lru_cache(maxsize=None)
def isimip_heat_drought_items():
    archive = checkpoints.isimip_heat_drought_t0.get().output.zip
    items = {}
    for member in recipes.archive_members(archive, "_occurrence.tif"):
        name = recipes.formats.posix_basename(member)[: -len(".tif")]
        props = isimip_heat_drought_properties(name)
        if props is None:
            print(f"isimip_heat_drought: skipping unrecognised file {member}")
            continue
        items[name.lower()] = T1Item(
            ext="tif", properties=props, sources=[archive], extra={"member": member}
        )
    return items


register_t1("isimip_heat_drought", isimip_heat_drought_items)


rule isimip_heat_drought_t1:
    """T1: COG of each occurrence GeoTIFF, read straight from the zip"""
    input:
        zip=f"{ISIMIP_RAW}/lange2020_expected_occurrence.zip",
    output:
        tif=f"{t1_dir('isimip_heat_drought')}/{{item}}.tif",
    wildcard_constraints:
        item="lange2020_[a-z0-9_.-]+_occurrence",
    run:
        member = t1_item("isimip_heat_drought", wildcards.item).extra["member"]
        recipes.to_cog(
            recipes.vsizip(input.zip, member),
            output.tif,
            descriptions="annual expected occurrence (fraction)",
        )
