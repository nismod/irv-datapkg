"""Test raster functions
"""

import pyproj
from irv_datapkg import RasterMeta


def test_raster_meta():
    crs = pyproj.CRS.from_epsg(4326)
    meta = RasterMeta(pixel_width=10, pixel_height=20, crs=crs)
    assert meta.pixel_height == 20
    assert meta.pixel_width == 10
