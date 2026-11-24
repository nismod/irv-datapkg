#
# GEM global seismic hazard map (PGA, 475 year return period, rock)
#
# Collection: gem_seismic_hazard (metadata/gem_earthquake.yml). Not in T2 packages.
#
GEM_RAW = raw_dir("gem_seismic_hazard")
GEM_ZENODO = "8409647"
GEM_ZIP = f"{GEM_RAW}/GEM-GSHM_PGA-475y-rock_v2023.zip"


@lru_cache(maxsize=None)
def gem_seismic_hazard_items():
    return {
        "pga_475y_rock": T1Item(
            ext="tif",
            properties={
                "hazard": "earthquake",
                "imt": "PGA",
                "return_period": 475,
                "site": "rock",
                "upstream_name": "v2023_1_pga_475_rock_3min",
            },
            sources=[GEM_ZIP],
        )
    }


register_t1("gem_seismic_hazard", gem_seismic_hazard_items)


rule gem_seismic_hazard_t0:
    """T0: the Zenodo record"""
    output:
        zip=GEM_ZIP,
        provenance=f"{GEM_RAW}/provenance.json",
    params:
        doi=f"10.5281/zenodo.{GEM_ZENODO}",
        provenance=provenance_cmd(
            "provenance.json",
            os.path.basename(GEM_ZIP),
            "zenodo_get",
            url_prefix=f"https://zenodo.org/records/{GEM_ZENODO}/files",
        ),
    shell:
        """
        mkdir -p $(dirname {output.zip})
        cd $(dirname {output.zip})
        zenodo_get {params.doi}
        {params.provenance}
        """


rule gem_seismic_hazard_t1:
    """T1: COG of the hazard map, read straight from the zip"""
    input:
        zip=GEM_ZIP,
    output:
        tif=f"{t1_dir('gem_seismic_hazard')}/{{item}}.tif",
    wildcard_constraints:
        item="pga_475y_rock",
    run:
        name = t1_item("gem_seismic_hazard", wildcards.item).properties[
            "upstream_name"
        ]
        recipes.to_cog(
            recipes.vsizip(input.zip, basename=f"{name}.tif"),
            output.tif,
            units="g",
            descriptions="peak ground acceleration, 10% probability of exceedance in 50 years",
        )
