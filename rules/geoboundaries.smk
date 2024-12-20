#
# geoBoundaries
#
GEOBOUNDARIES_META = pandas.read_csv(
    "bundled_data/geoboundaries.csv",
    usecols=["boundaryISO", "boundaryType", "staticDownloadLink"]
    ).set_index("boundaryISO")

def geoboundaries_levels(iso3):
    adm_a3 = boundary_adm0_a3(iso3)
    try:
        levels = GEOBOUNDARIES_META.loc[adm_a3, "boundaryType"].tolist()
    except AttributeError:
        levels = [GEOBOUNDARIES_META.loc[adm_a3, "boundaryType"], ]
    except KeyError:
        levels = []
    return levels

def geoboundaries_url(adm_a3, adm_level):
    try:
        links = GEOBOUNDARIES_META.loc[adm_a3, "staticDownloadLink"].tolist()
    except AttributeError:
        links = [GEOBOUNDARIES_META.loc[adm_a3, "staticDownloadLink"], ]

    return next(link for link in links if adm_level in link)

rule download_geoboundaries:
    """Download an admin level file
    """
    output:
        zip="incoming_data/geoboundaries/{ISO3}/geoBoundaries-{ADM_ISO3}-{ADM_LEVEL}-all.zip",
    run:
        data_url = geoboundaries_url(wildcards.ADM_ISO3, wildcards.ADM_LEVEL)

        filename = Path("incoming_data/geoboundaries") / wildcards.ISO3 / Path(data_url).name
        r = requests.get(data_url, stream=True)
        with open(filename, 'wb') as fd:
            for chunk in r.iter_content(chunk_size=2048):
                fd.write(chunk)

rule download_geoboundaries_region:
    """Download geoBoundaries for a region

    To re-fetch geoBoundaries metadata:

    ```python
    import pandas
    df = pandas.read_json("https://www.geoboundaries.org/api/current/gbOpen/ALL/ALL/")
    df.to_csv("bundled_data/geoboundaries.csv")
    ```
    """
    input:
        zips=expand(
            "incoming_data/geoboundaries/{{ISO3}}/geoBoundaries-{ADM_ISO3}-{ADM_LEVEL}-all.zip",
            ADM_ISO3=lambda wildcards: boundary_adm0_a3(wildcards.ISO3),
            ADM_LEVEL=lambda wildcards: geoboundaries_levels(wildcards.ISO3)
        )
    output:
        csv="incoming_data/geoboundaries/geoboundaries__{ISO3}.csv"
    run:
        adm_a3 = boundary_adm0_a3(wildcards.ISO3)
        try:
            meta = GEOBOUNDARIES_META.loc[adm_a3]
        except KeyError:
            meta = pandas.DataFrame(columns=GEOBOUNDARIES_META.columns)
        meta.to_csv(output.csv)

rule extract_zip:
    input:
        zip="incoming_data/geoboundaries/{ISO3}/geoBoundaries-{ADM_ISO3}-{ADM_LEVEL}-all.zip"
    output:
        gpkg="data/{ISO3}/geoboundaries/geoBoundaries-{ADM_ISO3}-{ADM_LEVEL}.gpkg",
        txt="data/{ISO3}/geoboundaries/geoBoundaries-{ADM_ISO3}-{ADM_LEVEL}-metaData.txt"
    shell:
        """
        extract_dir=incoming_data/geoboundaries/{wildcards.ISO3}/geoBoundaries-{wildcards.ADM_ISO3}-{wildcards.ADM_LEVEL}-all
        unzip -n {input.zip} -d $extract_dir
        ogr2ogr -f GPKG -nlt PROMOTE_TO_MULTI -overwrite {output.gpkg} $extract_dir/geoBoundaries-{wildcards.ADM_ISO3}-{wildcards.ADM_LEVEL}.shp
        cp $extract_dir/geoBoundaries-{wildcards.ADM_ISO3}-{wildcards.ADM_LEVEL}-metaData.txt {output.txt}
        """

rule extract_geoboundaries_region:
    input:
        dirs=expand(
            "data/{{ISO3}}/geoboundaries/geoBoundaries-{ADM_ISO3}-{ADM_LEVEL}.gpkg",
            ADM_ISO3=lambda wildcards: boundary_adm0_a3(wildcards.ISO3),
            ADM_LEVEL=lambda wildcards: geoboundaries_levels(wildcards.ISO3)
        ),
        csv="incoming_data/geoboundaries/geoboundaries__{ISO3}.csv"
    output:
        csv="data/{ISO3}/geoboundaries.csv"
    shell:
        """
        cp {input.csv} {output.csv}
        """
