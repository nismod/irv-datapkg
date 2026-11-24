#
# geoBoundaries gbOpen administrative boundaries
#
# Collection: boundaries_geoboundaries (metadata/geoboundaries.yml, T2 package "geoboundaries")
#
# One item per country and admin level, listed in bundled_data/geoboundaries.csv.
# To re-fetch that metadata (and then bump the collection version):
#
#   import pandas
#   df = pandas.read_json("https://www.geoboundaries.org/api/current/gbOpen/ALL/ALL/")
#   df.to_csv("bundled_data/geoboundaries.csv")
#
GEOBOUNDARIES_RAW = raw_dir("boundaries_geoboundaries")


@lru_cache(maxsize=None)
def geoboundaries_meta():
    meta = pandas.read_csv(
        "bundled_data/geoboundaries.csv",
        usecols=[
            "boundaryISO",
            "boundaryType",
            "boundaryYearRepresented",
            "boundarySource",
            "boundaryLicense",
            "staticDownloadLink",
        ],
        dtype=str,
    ).fillna("")
    return meta.set_index(["boundaryISO", "boundaryType"])


def geoboundaries_zip(adm_iso3, adm_level):
    return f"{GEOBOUNDARIES_RAW}/geoBoundaries-{adm_iso3}-{adm_level}-all.zip"


@lru_cache(maxsize=None)
def boundaries_geoboundaries_items():
    items = {}
    for (iso3, level), row in geoboundaries_meta().iterrows():
        items[f"{iso3.lower()}_{level.lower()}"] = T1Item(
            ext="parquet",
            properties={
                "iso3": iso3,
                "adm_level": level,
                "boundary_year": row.boundaryYearRepresented,
                "boundary_source": row.boundarySource,
                "boundary_license": row.boundaryLicense,
            },
            sources=[geoboundaries_zip(iso3, level)],
        )
    return items


register_t1("boundaries_geoboundaries", boundaries_geoboundaries_items)


rule boundaries_geoboundaries_t0:
    """T0: one zip per country and admin level"""
    output:
        zip=f"{GEOBOUNDARIES_RAW}/geoBoundaries-{{ADM_ISO3}}-{{ADM_LEVEL}}-all.zip",
        provenance=f"{GEOBOUNDARIES_RAW}/geoBoundaries-{{ADM_ISO3}}-{{ADM_LEVEL}}-all.zip.provenance.json",
    wildcard_constraints:
        ADM_ISO3="[A-Z]{3}",
        ADM_LEVEL="ADM[0-9]",
    run:
        url = geoboundaries_meta().loc[
            (wildcards.ADM_ISO3, wildcards.ADM_LEVEL), "staticDownloadLink"
        ]
        recipes.download(url, output.zip)


rule boundaries_geoboundaries_t1:
    """T1: the shapefile in each zip to GeoParquet"""
    input:
        zip=lambda wildcards: t1_item(
            "boundaries_geoboundaries", wildcards.item
        ).sources[0],
    output:
        parquet=f"{t1_dir('boundaries_geoboundaries')}/{{item}}.parquet",
    wildcard_constraints:
        item="[a-z]{3}_adm[0-9]",
    run:
        props = t1_item("boundaries_geoboundaries", wildcards.item).properties
        shp = f"geoBoundaries-{props['iso3']}-{props['adm_level']}.shp"
        recipes.to_geoparquet(
            recipes.vsizip(input.zip, basename=shp), output.parquet
        )
