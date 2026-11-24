#
# Copernicus Climate Data Store satellite land cover
#
# Collection: copernicus_lulc (metadata/copernicus_lulc.yml, T2 package "copernicus_lulc")
#
# Needs CDSAPI_URL and CDSAPI_KEY, and the dataset licence accepted on the CDS website.
#
LULC_RAW = raw_dir("copernicus_lulc")
LULC_BUILD = build_dir("copernicus_lulc")
LULC_YEARS = [2022]
LULC_CDS_VERSION = "v2_1_1"


def lulc_netcdf(year):
    return f"C3S-LC-L4-LCCS-Map-300m-P1Y-{year}-v2.1.1.nc"


@lru_cache(maxsize=None)
def copernicus_lulc_items():
    return {
        f"lccs_class_{year}": T1Item(
            ext="tif",
            properties={"year": year},
            sources=[f"{LULC_RAW}/satellite-land-cover_{year}_{LULC_CDS_VERSION}.tgz"],
            datetime=f"{year}-01-01T00:00:00Z",
        )
        for year in LULC_YEARS
    }


register_t1("copernicus_lulc", copernicus_lulc_items)


rule copernicus_lulc_t0:
    """T0: CDS API request for one year"""
    output:
        archive=f"{LULC_RAW}/satellite-land-cover_{{year}}_{LULC_CDS_VERSION}.tgz",
        provenance=f"{LULC_RAW}/satellite-land-cover_{{year}}_{LULC_CDS_VERSION}.tgz.provenance.json",
    run:
        from irv_datapkg import download_from_CDS

        request = {
            "variable": "all",
            "year": [wildcards.year],
            "version": [LULC_CDS_VERSION],
            "format": "tgz",
        }
        download_from_CDS("satellite-land-cover", request, output.archive)
        recipes.write_provenance(
            output.provenance,
            [output.archive],
            tool="cdsapi",
            source_url=f"{os.environ['CDSAPI_URL']}/resources/satellite-land-cover {json.dumps(request)}",
        )


rule copernicus_lulc_netcdf:
    """Intermediate: the NetCDF from the archive"""
    input:
        archive=f"{LULC_RAW}/satellite-land-cover_{{year}}_{LULC_CDS_VERSION}.tgz",
    output:
        nc=f"{LULC_BUILD}/C3S-LC-L4-LCCS-Map-300m-P1Y-{{year}}-v2.1.1.nc",
    run:
        recipes.unzip(
            input.archive, LULC_BUILD, members=[Path(output.nc).name], flatten=True
        )


rule copernicus_lulc_t1:
    """T1: land cover class to COG on the source grid"""
    input:
        nc=lambda wildcards: f"{LULC_BUILD}/{lulc_netcdf(wildcards.year)}",
    output:
        tif=f"{t1_dir('copernicus_lulc')}/lccs_class_{{year}}.tif",
    run:
        recipes.netcdf_to_cog(
            input.nc,
            "lccs_class",
            output.tif,
            crs="EPSG:4326",
            nodata=COLLECTIONS["copernicus_lulc"].nodata,
            descriptions="land cover class (UN FAO LCCS)",
        )
