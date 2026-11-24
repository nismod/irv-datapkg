#
# JRC global river flood hazard maps (GlobalMaps)
#
# Collection: jrc_flood_globalmaps (metadata/jrc_floods.yml, T2 package "jrc_floods")
#
JRC_FLOOD_RAW = raw_dir("jrc_flood_globalmaps")
JRC_FLOOD_URL = "https://cidportal.jrc.ec.europa.eu/ftp/jrc-opendata/FLOODS/GlobalMaps"
JRC_FLOOD_RPS = [10, 20, 50, 100, 200, 500]


@lru_cache(maxsize=None)
def jrc_flood_globalmaps_items():
    items = {}
    for rp in JRC_FLOOD_RPS:
        name = f"floodMapGL_rp{rp}y"
        items[name.lower()] = T1Item(
            ext="tif",
            properties={"hazard": "river", "return_period": rp, "upstream_name": name},
            sources=[f"{JRC_FLOOD_RAW}/{name}.zip"],
        )
    return items


register_t1("jrc_flood_globalmaps", jrc_flood_globalmaps_items)


rule jrc_flood_globalmaps_t0:
    """T0: one zip per return period"""
    output:
        zip=f"{JRC_FLOOD_RAW}/floodMapGL_rp{{RP}}y.zip",
        provenance=f"{JRC_FLOOD_RAW}/floodMapGL_rp{{RP}}y.zip.provenance.json",
    wildcard_constraints:
        RP="[0-9]+",
    run:
        recipes.download(
            f"{JRC_FLOOD_URL}/floodMapGL_rp{wildcards.RP}y.zip", output.zip
        )


rule jrc_flood_globalmaps_t1:
    """T1: COG of each return period map, read straight from the zip"""
    input:
        zip=f"{JRC_FLOOD_RAW}/floodMapGL_rp{{RP}}y.zip",
    output:
        tif=f"{t1_dir('jrc_flood_globalmaps')}/floodmapgl_rp{{RP}}y.tif",
    wildcard_constraints:
        RP="[0-9]+",
    run:
        recipes.to_cog(
            recipes.vsizip(input.zip, basename=f"floodMapGL_rp{wildcards.RP}y.tif"),
            output.tif,
            units="m",
            descriptions="inundation depth",
        )
