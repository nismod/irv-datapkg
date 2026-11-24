#
# Copernicus DEM GLO-90
#
# Collection: copernicus_dem_glo90 (metadata/copernicus_dem.yml, T2 package "copernicus_dem")
#
DEM_RAW = raw_dir("copernicus_dem_glo90")
DEM_BUILD = build_dir("copernicus_dem_glo90")
DEM_RELEASE = "COP-DEM_GLO-90-DGED__2023_1"
DEM_LIST_URL = f"https://prism-dem-open.copernicus.eu/pd-desk-open-access/publicDemURLs/{DEM_RELEASE}"
DEM_NODATA = COLLECTIONS["copernicus_dem_glo90"].nodata


@lru_cache(maxsize=None)
def copernicus_dem_glo90_items():
    return {
        "copernicus_dem_glo90": T1Item(
            ext="tif",
            sources=[f"{DEM_RAW}/{DEM_RELEASE}.txt"],
            source_url=DEM_LIST_URL,
        )
    }


register_t1("copernicus_dem_glo90", copernicus_dem_glo90_items)


rule copernicus_dem_glo90_list:
    """T0: list of tile URLs for the release"""
    output:
        txt=f"{DEM_RAW}/{DEM_RELEASE}.txt",
        provenance=f"{DEM_RAW}/{DEM_RELEASE}.txt.provenance.json",
    run:
        recipes.download(DEM_LIST_URL, output.txt)


rule copernicus_dem_glo90_t0:
    """T0: every tile archive (tar), with per-tile provenance"""
    input:
        txt=rules.copernicus_dem_glo90_list.output.txt,
    output:
        dir=directory(f"{DEM_RAW}/archive"),
    params:
        listing=lambda wildcards, input: os.path.abspath(input.txt),
        provenance=provenance_cmd(
            "provenance.json", "*.tar", "wget", urls_from="urls.txt"
        ),
    shell:
        """
        mkdir -p {output.dir}.tmp
        cd {output.dir}.tmp
        # the list may be CSV: keep the URL column
        grep -o 'https://[^,]*' {params.listing} >urls.txt
        parallel -j 8 'wget --no-clobber --quiet {{}}' <urls.txt
        {params.provenance}
        cd ..
        mv archive.tmp archive
        """


rule copernicus_dem_glo90_tiles:
    """Intermediate: the DEM GeoTIFF from each tile archive"""
    input:
        dir=rules.copernicus_dem_glo90_t0.output.dir,
    output:
        dir=directory(f"{DEM_BUILD}/tiles"),
    run:
        os.makedirs(output.dir, exist_ok=True)
        for tar in sorted(Path(input.dir).glob("*.tar")):
            name = tar.name[: -len(".tar")]
            recipes.unzip(
                tar, output.dir, members=[f"{name}_DEM.tif"], flatten=True
            )


rule copernicus_dem_glo90_t1:
    """T1: mosaic the tiles into one global COG"""
    input:
        dir=rules.copernicus_dem_glo90_tiles.output.dir,
    output:
        tif=f"{t1_dir('copernicus_dem_glo90')}/copernicus_dem_glo90.tif",
    run:
        tiles = sorted(Path(input.dir).glob("*_DEM.tif"))
        vrt = recipes.build_vrt(
            tiles, f"{DEM_BUILD}/mosaic.vrt", vrt_nodata=DEM_NODATA
        )
        recipes.to_cog(
            vrt,
            output.tif,
            nodata=DEM_NODATA,
            units="m",
            descriptions="surface elevation (DSM), EGM2008 heights",
            overview_resampling="average",
        )
