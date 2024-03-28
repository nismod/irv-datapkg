from dataclasses import dataclass
import shlex
import subprocess
from pathlib import Path
from typing import Optional
import argparse
import os

import geopandas
import shapely
import pyproj
import cdsapi

from osgeo import gdal
from shapely.ops import transform


def read_boundaries(base_path: Path) -> geopandas.GeoDataFrame:
    return geopandas.read_file(
        base_path / "bundled_data" / "ne_10m_admin_0_map_units_custom.gpkg"
    )


@dataclass
class RasterMeta:
    pixel_width: float
    pixel_height: float
    crs: pyproj.CRS


def read_raster_meta(fname: Path) -> RasterMeta:
    """Read pixel size and CRS from a raster file"""
    gdal.UseExceptions()
    dataset = gdal.Open(fname)

    geotransform = dataset.GetGeoTransform()
    pixel_width = geotransform[1]
    pixel_height = geotransform[5]
    crs = pyproj.CRS.from_wkt(dataset.GetProjection())

    dataset.Close()

    return RasterMeta(pixel_width=pixel_width, pixel_height=pixel_height, crs=crs)


def crop_raster(
    fname: Path,
    out_fname: Path,
    boundary: shapely.Polygon,
    creation_options: Optional[list[str]] = None,
):
    """Crop a raster using GDAL translate"""

    meta = read_raster_meta(fname)
    boundary_crs = pyproj.CRS("EPSG:4326")
    print("Boundary", boundary.bounds, boundary_crs)

    if boundary_crs != meta.crs:
        # Reproject boundary to raster CRS
        transformer = pyproj.Transformer.from_crs(
            boundary_crs, meta.crs, always_xy=True
        )
        bounds = transform(transformer.transform, boundary).bounds
    else:
        bounds = boundary.bounds

    print("Boundary transformed", bounds, meta.crs)

    # buffer bounds by a pixel - should fix tiny samples
    # minx miny maxx maxy
    ulx, lry, lrx, uly = bounds
    ulx = ulx - meta.pixel_width
    lry = lry - abs(meta.pixel_height)
    lrx = lrx + meta.pixel_width
    uly = uly + abs(meta.pixel_height)

    print("Boundary nudged", ulx, lry, lrx, uly)

    cmd = f"gdal_translate -projwin {ulx} {uly} {lrx} {lry} {fname} {out_fname}"

    if creation_options is None:
        creation_options = ["COMPRESS=ZSTD"]

    # Add Creation Options
    for creation_option in creation_options:
        cmd = cmd + f" -co {creation_option}"

    subprocess.run(shlex.split(cmd), check=True)

def download_from_CDS(dataset_name: str, variable: str, file_format: str, version: str, year: str, output_path: str) -> None:
    """
    Download a resource from the Copernicus CDS API, given appropriate credentials.

    Requires COPERNICUS_CDS_URL and COPERNICUS_CDS_API_KEY to be in the environment.
    For more details see: https://cds.climate.copernicus.eu/api-how-to

    Args:
        dataset_name: Name of dataset to download
        variable: Name of variable to request
        file_format: Desired file format e.g. zip
        version: Version of dataset
        year: Year of dataset applicability
        output_path: Where to save the downloaded file
    """

    client = cdsapi.Client(
        url=os.environ.get("COPERNICUS_CDS_URL"),
        key=os.environ.get("COPERNICUS_CDS_API_KEY")
    )

    # N.B. Files are covered by licences which need to be manually accepted, e.g.
    # https://cds.climate.copernicus.eu/cdsapp/#!/terms/satellite-land-cover
    # https://cds.climate.copernicus.eu/cdsapp/#!/terms/vito-proba-v
    #
    # Ideally we could programmatically accept the necessary licence conditions
    # the below code is an attempt at that, but fails with an HTTP 403, not
    # logged in when trying to simulate a user acceptance 
    #
    #   API_URL = os.environ.get("COPERNICUS_CDS_URL")
    #   payloads = [
    #       [{"terms_id":"vito-proba-v","revision":1}],
    #       [{"terms_id":"satellite-land-cover","revision":1}],
    #   ]
    #   for payload in payloads:
    #       client._api(
    #           url=f"{API_URL.rstrip('/')}.ui/user/me/terms-and-conditions",
    #           request=payload,
    #           method="post"
    #       )
    #
    # See https://github.com/ecmwf/cdsapi/blob/master/cdsapi/api.py

    client.retrieve(
        dataset_name,
        {
            'variable': variable,
            'format': file_format,
            'version': version,
            'year': year,
        },
        output_path
    )
