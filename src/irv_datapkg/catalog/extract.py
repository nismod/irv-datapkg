"""Clip catalogue assets to a bbox or polygon (building block for T2 extracts)."""

import math
from pathlib import Path
from typing import Optional, Sequence, Union

from shapely.geometry import box, mapping, shape

from . import _fs
from .client import Asset, Item

RASTER_SUFFIXES = (".tif", ".tiff")
VECTOR_SUFFIXES = (".parquet",)


def _geometry(bbox, geometry):
    if (bbox is None) == (geometry is None):
        raise ValueError("Give exactly one of bbox or geometry")
    if bbox is not None:
        return box(*bbox), True
    if isinstance(geometry, dict):
        geometry = shape(geometry.get("geometry", geometry))
    return geometry, False


def extract(
    source: Union[Item, Asset, str],
    out_path: Union[str, Path],
    *,
    bbox: Optional[Sequence[float]] = None,
    geometry=None,
    crs: str = "EPSG:4326",
    storage_options: Optional[dict] = None,
) -> Path:
    """Clip a raster (COG) or GeoParquet asset to ``bbox`` or ``geometry``.

    ``source`` is an Item (its primary asset), an Asset, or a location (local
    path or fsspec URL). ``bbox``/``geometry`` are in ``crs`` (default
    EPSG:4326). Rasters are read with a window covering the area (no full
    download); with a polygon, cells outside it are set to nodata. Vector rows
    are filtered on the bbox covering column, then by intersection with the
    polygon (geometries are not cut). Writes ``out_path`` and returns it.
    """
    if isinstance(source, Item):
        source = source.asset
    href = source.href if isinstance(source, Asset) else _fs.normalise(source)
    geom, is_bbox = _geometry(bbox, geometry)
    out_path = Path(out_path)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    suffix = Path(_fs.split(href)[1]).suffix.lower()
    if suffix in RASTER_SUFFIXES:
        return _extract_raster(href, out_path, geom, is_bbox, crs)
    if suffix in VECTOR_SUFFIXES:
        return _extract_vector(href, out_path, geom, crs, storage_options)
    raise ValueError(f"Cannot extract from {href}: unsupported file type")


def _extract_raster(href, out_path, geom, is_bbox, crs):
    import rasterio
    import rasterio.mask
    import rasterio.shutil
    from rasterio.io import MemoryFile
    from rasterio.warp import transform_bounds, transform_geom
    from rasterio.windows import Window, from_bounds

    with rasterio.open(_fs.gdal_path(href)) as src:
        if is_bbox:
            bounds = transform_bounds(crs, src.crs, *geom.bounds, densify_pts=21)
            w = from_bounds(*bounds, transform=src.transform)
            col0, row0 = math.floor(w.col_off + 1e-9), math.floor(w.row_off + 1e-9)
            col1 = math.ceil(w.col_off + w.width - 1e-9)
            row1 = math.ceil(w.row_off + w.height - 1e-9)
            window = Window(col0, row0, col1 - col0, row1 - row0).intersection(
                Window(0, 0, src.width, src.height)
            )
            data = src.read(window=window)
            transform = src.window_transform(window)
        else:
            src_geom = transform_geom(crs, src.crs, mapping(geom))
            data, transform = rasterio.mask.mask(
                src, [src_geom], crop=True, all_touched=True, filled=True
            )
        profile = src.profile.copy()
        tags = src.tags()
        descriptions = src.descriptions
        units = src.units

    profile.update(driver="GTiff", height=data.shape[1], width=data.shape[2], transform=transform)
    for key in ("blockxsize", "blockysize", "tiled", "compress", "interleave"):
        profile.pop(key, None)
    with MemoryFile() as mem:
        with mem.open(**profile) as tmp:
            tmp.write(data)
            tmp.update_tags(**tags)
            for i, (d, u) in enumerate(zip(descriptions, units), start=1):
                if d:
                    tmp.set_band_description(i, d)
                if u:
                    tmp.set_band_unit(i, u)
        with mem.open() as tmp:
            rasterio.shutil.copy(
                tmp, str(out_path), driver="COG", compress="DEFLATE", blocksize=512
            )
    return out_path


def _extract_vector(href, out_path, geom, crs, storage_options):
    import geopandas
    import pyproj
    from shapely.ops import transform as shp_transform

    target = pyproj.CRS("EPSG:4326")
    if pyproj.CRS(crs) != target:
        transformer = pyproj.Transformer.from_crs(crs, target, always_xy=True)
        geom = shp_transform(transformer.transform, geom)
    try:
        gdf = geopandas.read_parquet(href, bbox=tuple(geom.bounds), storage_options=storage_options)
    except ValueError:
        # no bbox covering column: read everything, filter below
        gdf = geopandas.read_parquet(href, storage_options=storage_options)
    gdf = gdf[gdf.intersects(geom)]
    gdf.to_parquet(out_path, write_covering_bbox=True)
    return out_path
