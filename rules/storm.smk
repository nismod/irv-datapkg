#
# STORM tropical cyclone fixed return period wind speed mosaics
#
# Collection: storm_fixed_rp (metadata/storm.yml, T2 package "storm")
#
STORM_MODELS = [
    "constant",
    "CMCC-CM2-VHR4",
    "CNRM-CM6-1-HR",
    "EC-Earth3P-HR",
    "HadGEM3-GC31-HM",
]
STORM_RPS = (
    list(range(10, 100, 10))
    + list(range(100, 1000, 100))
    + list(range(1000, 10001, 1000))
)
STORM_RAW = raw_dir("storm_fixed_rp")
STORM_ZENODO = "10931452"


@lru_cache(maxsize=None)
def storm_fixed_rp_items():
    items = {}
    for model in STORM_MODELS:
        baseline = model == "constant"
        for rp in STORM_RPS:
            name = f"STORM_FIXED_RETURN_PERIODS_{model}_{rp}_YR_RP"
            items[name.lower()] = T1Item(
                ext="tif",
                properties={
                    "hazard": "cyclone",
                    # "constant" is the present-day baseline; the GCMs are RCP8.5 in 2050
                    "rcp": "historical" if baseline else "8.5",
                    "epoch": "baseline" if baseline else 2050,
                    "gcm": model,
                    "return_period": rp,
                    "upstream_name": name,
                },
                sources=[f"{STORM_RAW}/STORM_FIXED_RETURN_PERIODS.zip"],
            )
    return items


register_t1("storm_fixed_rp", storm_fixed_rp_items)


rule storm_fixed_rp_t0:
    """T0: Zenodo record files, md5-checked"""
    output:
        zip=f"{STORM_RAW}/STORM_FIXED_RETURN_PERIODS.zip",
        provenance=f"{STORM_RAW}/provenance.json",
    params:
        doi=f"10.5281/zenodo.{STORM_ZENODO}",
        provenance=provenance_cmd(
            "provenance.json",
            "$(sed 's#?.*##; s#.*/##' links.txt)",
            "zenodo_get",
            url_prefix=f"https://zenodo.org/records/{STORM_ZENODO}/files",
        ),
    shell:
        """
        mkdir -p $(dirname {output.zip})
        cd $(dirname {output.zip})
        zenodo_get -w links.txt {params.doi}
        wget -nc -i links.txt
        md5sum -c md5sums.txt
        {params.provenance}
        """


rule storm_fixed_rp_t1:
    """T1: one COG per model and return period, read straight from the zip"""
    input:
        zip=rules.storm_fixed_rp_t0.output.zip,
    output:
        tif=f"{t1_dir('storm_fixed_rp')}/{{item}}.tif",
    wildcard_constraints:
        item="storm_fixed_return_periods_[a-z0-9-]+_[0-9]+_yr_rp",
    run:
        name = t1_item("storm_fixed_rp", wildcards.item).properties["upstream_name"]
        recipes.to_cog(
            recipes.vsizip(input.zip, basename=f"{name}.tif"),
            output.tif,
            units="m/s",
            descriptions="maximum sustained wind speed (10-minute mean, 10 m)",
        )
