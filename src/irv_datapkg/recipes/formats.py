"""Format standardisation for T1 (ADR 0004): COG, GeoParquet, archives.

T1 keeps the source CRS and grid (ADR 0001): nothing here reprojects or
resamples rasters. ``crs=`` and ``nodata=`` *assign* metadata (like
``gdal_translate -a_srs/-a_nodata``); they never transform values.
"""

import os
import shlex
import shutil
import subprocess
import tarfile
import tempfile
import xml.etree.ElementTree as ET
import zipfile
from pathlib import Path
from typing import Iterable, Optional, Sequence, Union

PathLike = Union[str, Path]

#: Default COG creation options (ADR 0004)
COG_OPTIONS = {
    "COMPRESS": "ZSTD",
    "LEVEL": "9",
    "PREDICTOR": "YES",  # 2 (horizontal) for integers, 3 (floating point) for floats
    "BLOCKSIZE": "512",
    "OVERVIEWS": "AUTO",
    "BIGTIFF": "IF_SAFER",
    "NUM_THREADS": "ALL_CPUS",
}

#: GeoParquet defaults (ADR 0004)
ROW_GROUP_SIZE = 100_000
GEOPARQUET_VERSION = "1.1.0"


def cog_options(
    compress: str = "ZSTD",
    overview_resampling: str = "nearest",
    **overrides,
) -> dict:
    """COG driver creation options: ADR 0004 defaults plus ``overrides``.

    ``compress`` is ZSTD (default) or DEFLATE; both use a predictor.
    ``overview_resampling`` is ``nearest`` for categorical/hazard grids,
    ``average`` (or ``bilinear``) for continuous fields where that is meaningful.
    """
    compress = compress.upper()
    if compress not in ("ZSTD", "DEFLATE", "LZW"):
        raise ValueError("compress must be ZSTD or DEFLATE (LZW tolerated)")
    opts = dict(COG_OPTIONS, COMPRESS=compress)
    if compress == "LZW":
        del opts["LEVEL"]
    opts["RESAMPLING"] = overview_resampling.upper()
    opts.update({k.upper(): str(v) for k, v in overrides.items()})
    return opts


def gdal_translate_cog_args(**kwargs) -> str:
    """``-of COG -co KEY=VALUE ...`` for shell rules that call ``gdal_translate``."""
    return " ".join(["-of COG"] + [f"-co {k}={v}" for k, v in cog_options(**kwargs).items()])


def _abs_source(src: str) -> str:
    """Absolute source path for VRT references (keeps GDAL connection strings)."""
    if src.startswith(("/vsi", "http://", "https://")) or "://" in src:
        return src
    if ":" in src and src.split(":", 1)[0].upper() in ("NETCDF", "HDF5", "HDF4_SDS", "GPKG"):
        driver, rest = src.split(":", 1)
        # NETCDF:"path":variable
        if rest.startswith('"'):
            path, var = rest[1:].split('"', 1)
            return f'{driver}:"{os.path.abspath(path)}"{var}'
        return src
    return os.path.abspath(src)


def _set_child(parent, tag, text):
    el = parent.find(tag)
    if el is None:
        el = ET.SubElement(parent, tag)
    el.text = str(text)
    return el


def _per_band(value, count):
    if value is None or isinstance(value, (str, int, float)):
        return [value] * count
    value = list(value)
    if len(value) != count:
        raise ValueError(f"Expected {count} per-band values, got {len(value)}")
    return value


def to_cog(
    src: PathLike,
    dst: PathLike,
    *,
    nodata=None,
    crs: Optional[str] = None,
    units=None,
    descriptions=None,
    scale=None,
    offset=None,
    tags: Optional[dict] = None,
    compress: str = "ZSTD",
    overview_resampling: str = "nearest",
    **creation_options,
) -> Path:
    """Convert any GDAL-readable raster (GeoTIFF, VRT, ``NETCDF:"f.nc":var``,
    ASCII grid ...) to a Cloud Optimised GeoTIFF on the source grid.

    ``nodata``/``crs``/``units``/``descriptions``/``scale``/``offset`` assign
    band metadata (scalar, or one value per band). The data and grid are copied
    unchanged. Writes via a temporary VRT, so no intermediate full-size copy.
    """
    import rasterio
    import rasterio.shutil

    src, dst = str(src), Path(dst)
    dst.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(dir=dst.parent, prefix=".cog-") as tmp:
        vrt = os.path.join(tmp, "src.vrt")
        with rasterio.open(_abs_source(src)) as ds:
            count = ds.count
            rasterio.shutil.copy(ds, vrt, driver="VRT")
        tree = ET.parse(vrt)
        root = tree.getroot()
        if crs is not None:
            from rasterio.crs import CRS

            _set_child(root, "SRS", CRS.from_user_input(crs).to_wkt())
        bands = root.findall("VRTRasterBand")
        values = {
            "NoDataValue": _per_band(nodata, count),
            "UnitType": _per_band(units, count),
            "Description": _per_band(descriptions, count),
            "Scale": _per_band(scale, count),
            "Offset": _per_band(offset, count),
        }
        for i, band in enumerate(bands):
            for tag, vals in values.items():
                if vals[i] is not None:
                    _set_child(band, tag, vals[i])
        if tags:
            # dataset metadata goes in the VRT so the COG copy carries it in its header
            md = next((m for m in root.findall("Metadata") if not m.get("domain")), None)
            if md is None:
                md = ET.SubElement(root, "Metadata")
            for key, value in tags.items():
                mdi = next((m for m in md.findall("MDI") if m.get("key") == key), None)
                if mdi is None:
                    mdi = ET.SubElement(md, "MDI", key=str(key))
                mdi.text = str(value)
        tree.write(vrt)
        with rasterio.open(vrt) as ds:
            tmp_tif = os.path.join(tmp, "out.tif")
            rasterio.shutil.copy(
                ds,
                tmp_tif,
                driver="COG",
                **cog_options(compress, overview_resampling, **creation_options),
            )
        shutil.move(tmp_tif, dst)
    return dst


def netcdf_to_cog(nc_path: PathLike, variable: str, dst: PathLike, **kwargs) -> Path:
    """Convert one variable of a NetCDF file to a COG (see :func:`to_cog`)."""
    return to_cog(f'NETCDF:"{nc_path}":{variable}', dst, **kwargs)


def build_vrt(
    sources: Sequence[PathLike],
    dst: PathLike,
    *,
    vrt_nodata=None,
    src_nodata=None,
    resolution: str = "highest",
) -> Path:
    """Mosaic ``sources`` into a VRT (GDAL bindings if present, else ``gdalbuildvrt``).

    ``resolution`` is ``highest`` by default so tiles with coarser pixels (e.g.
    Copernicus DEM above 50 degrees) are not downsampled; ``vrt_nodata`` fills
    gaps between tiles.
    """
    dst = Path(dst)
    dst.parent.mkdir(parents=True, exist_ok=True)
    sources = [str(s) for s in sources]
    try:
        from osgeo import gdal

        gdal.UseExceptions()
        options = {"resolution": resolution}
        if vrt_nodata is not None:
            options["VRTNodata"] = vrt_nodata
        if src_nodata is not None:
            options["srcNodata"] = src_nodata
        vrt = gdal.BuildVRT(str(dst), sources, **options)
        vrt = None  # noqa: F841 (flush to disk)
    except ImportError:
        with tempfile.NamedTemporaryFile("w", suffix=".txt", delete=False) as fh:
            fh.write("\n".join(sources))
            listing = fh.name
        cmd = ["gdalbuildvrt", "-resolution", resolution]
        if vrt_nodata is not None:
            cmd += ["-vrtnodata", str(vrt_nodata)]
        if src_nodata is not None:
            cmd += ["-srcnodata", str(src_nodata)]
        try:
            subprocess.run(cmd + ["-input_file_list", listing, str(dst)], check=True)
        finally:
            os.unlink(listing)
    return dst


def is_cog(path: PathLike, blocksize: int = 512) -> list:
    """Problems that stop ``path`` from meeting the ADR 0004 COG profile (empty if fine)."""
    import rasterio

    problems = []
    with rasterio.open(path) as ds:
        structure = ds.tags(ns="IMAGE_STRUCTURE")
        if structure.get("LAYOUT") != "COG":
            problems.append("not COG layout")
        if structure.get("COMPRESSION") not in ("ZSTD", "DEFLATE"):
            problems.append(f"compression {structure.get('COMPRESSION')}")
        if structure.get("PREDICTOR") not in ("2", "3"):
            problems.append(f"predictor {structure.get('PREDICTOR')}")
        if ds.block_shapes[0] != (blocksize, blocksize) and (
            ds.width > blocksize or ds.height > blocksize
        ):
            problems.append(f"block shape {ds.block_shapes[0]}")
        if max(ds.width, ds.height) > blocksize and not ds.overviews(1):
            problems.append("no overviews")
        if any(v is None for v in ds.nodatavals):
            problems.append("no nodata")
    return problems


#
# Vectors
#
def write_geoparquet(
    gdf, dst: PathLike, *, sort: bool = True, row_group_size: int = ROW_GROUP_SIZE
) -> Path:
    """Write a GeoDataFrame as GeoParquet 1.1 in EPSG:4326 with a bbox covering
    column, Hilbert-sorted, ZSTD compressed, ~100k-row groups (ADR 0004)."""
    dst = Path(dst)
    dst.parent.mkdir(parents=True, exist_ok=True)
    if gdf.crs is None:
        raise ValueError("GeoDataFrame has no CRS")
    if gdf.crs.to_epsg() != 4326:
        gdf = gdf.to_crs("EPSG:4326")
    if sort and len(gdf) > 1:
        valid = ~(gdf.geometry.is_empty | gdf.geometry.isna())
        if valid.all():
            gdf = gdf.iloc[gdf.geometry.hilbert_distance().argsort()]
    gdf = gdf.reset_index(drop=True)
    gdf.to_parquet(
        dst,
        schema_version=GEOPARQUET_VERSION,
        write_covering_bbox=True,
        compression="zstd",
        row_group_size=row_group_size,
        index=False,
    )
    return dst


def to_geoparquet(
    src: PathLike,
    dst: PathLike,
    *,
    layer: Optional[str] = None,
    columns: Optional[Sequence[str]] = None,
    where: Optional[str] = None,
    **kwargs,
) -> Path:
    """Convert any OGR-readable vector layer (GPKG, shapefile, zipped shapefile
    via ``/vsizip/``, GeoJSON ...) to GeoParquet (see :func:`write_geoparquet`).

    Reads the whole layer into memory: for very large layers (OSM planet) use
    :func:`ogr2ogr_geoparquet_cmd` instead.
    """
    import geopandas

    gdf = geopandas.read_file(src, layer=layer, columns=columns, where=where, engine="pyogrio")
    return write_geoparquet(gdf, dst, **kwargs)


def points_to_geoparquet(
    table, dst: PathLike, *, x: str = "longitude", y: str = "latitude", crs="EPSG:4326", **kwargs
) -> Path:
    """Convert a table (DataFrame, or a CSV path) of point coordinates to GeoParquet."""
    import geopandas
    import pandas

    df = pandas.read_csv(table, low_memory=False) if isinstance(table, (str, Path)) else table
    gdf = geopandas.GeoDataFrame(df, geometry=geopandas.points_from_xy(df[x], df[y]), crs=crs)
    return write_geoparquet(gdf, dst, **kwargs)


def ogr2ogr_geoparquet_cmd(
    src: PathLike,
    dst: PathLike,
    layer: Optional[str] = None,
    *,
    row_group_size: int = ROW_GROUP_SIZE,
    extra: Iterable[str] = (),
) -> str:
    """Shell command streaming a (large) OGR layer to GeoParquet with ``ogr2ogr``.

    Needs GDAL >= 3.9 built with the Arrow/Parquet driver (conda-forge:
    ``libgdal-arrow-parquet``). Rows are sorted by bbox (``SORT_BY_BBOX``)
    rather than a Hilbert curve; see docs/recipes.md.
    """
    parts = [
        "ogr2ogr",
        "-f Parquet",
        "-t_srs EPSG:4326",
        "-lco COMPRESSION=ZSTD",
        "-lco WRITE_COVERING_BBOX=YES",
        "-lco SORT_BY_BBOX=YES",
        f"-lco ROW_GROUP_SIZE={row_group_size}",
        *extra,
        shlex.quote(str(dst)),
        shlex.quote(str(src)),
    ]
    if layer:
        parts.append(shlex.quote(layer))
    return " ".join(parts)


#
# Archives
#
def archive_member(archive: PathLike, basename: str) -> str:
    """Full path inside a zip/tar archive of the single member called ``basename``."""
    archive = Path(archive)
    if zipfile.is_zipfile(archive):
        with zipfile.ZipFile(archive) as zf:
            names = zf.namelist()
    else:
        with tarfile.open(archive) as tf:
            names = tf.getnames()
    matches = [n for n in names if posix_basename(n) == basename]
    if len(matches) != 1:
        raise FileNotFoundError(f"{archive} has {len(matches)} members called {basename}")
    return matches[0]


def archive_members(archive: PathLike, suffix: str = "") -> list:
    """Member paths of a zip archive ending in ``suffix``."""
    with zipfile.ZipFile(archive) as zf:
        return [n for n in zf.namelist() if n.endswith(suffix) and not n.endswith("/")]


def posix_basename(name: str) -> str:
    return name.rstrip("/").rsplit("/", 1)[-1]


def vsizip(
    archive: PathLike, member: Optional[str] = None, *, basename: Optional[str] = None
) -> str:
    """GDAL path reading ``member`` (or the member called ``basename``) straight
    from a zip, without extracting it: ``/vsizip/{archive}/{member}``."""
    if member is None:
        if basename is None:
            raise ValueError("give member or basename")
        member = archive_member(archive, basename)
    return f"/vsizip/{os.path.abspath(archive)}/{member}"


def _safe_target(dest: Path, name: str) -> Path:
    target = (dest / name).resolve()
    if dest.resolve() not in target.parents and target != dest.resolve():
        raise ValueError(f"Refusing to extract {name!r} outside {dest}")
    return target


def unzip(
    archive: PathLike,
    dest: PathLike,
    members: Optional[Iterable[str]] = None,
    *,
    flatten: bool = False,
    overwrite: bool = False,
) -> list:
    """Extract a .zip, .tar, .tar.gz/.tgz archive into ``dest``; returns extracted paths.

    ``members`` limits extraction to these names (exact archive paths, or base
    names when ``flatten``). ``flatten`` drops directories inside the archive.
    Refuses entries that would land outside ``dest``. Existing files are kept
    unless ``overwrite``.
    """
    archive, dest = Path(archive), Path(dest)
    dest.mkdir(parents=True, exist_ok=True)
    wanted = set(members) if members is not None else None
    out = []

    def want(name):
        if name.endswith("/"):
            return False
        if wanted is None:
            return True
        return name in wanted or (flatten and Path(name).name in wanted)

    if zipfile.is_zipfile(archive):
        with zipfile.ZipFile(archive) as zf:
            for info in zf.infolist():
                if info.is_dir() or not want(info.filename):
                    continue
                target = _safe_target(dest, Path(info.filename).name if flatten else info.filename)
                if target.exists() and not overwrite:
                    out.append(target)
                    continue
                target.parent.mkdir(parents=True, exist_ok=True)
                with zf.open(info) as fin, open(target, "wb") as fout:
                    shutil.copyfileobj(fin, fout)
                out.append(target)
    elif tarfile.is_tarfile(archive):
        with tarfile.open(archive) as tf:
            for info in tf:
                if not info.isfile() or not want(info.name):
                    continue
                target = _safe_target(dest, Path(info.name).name if flatten else info.name)
                if target.exists() and not overwrite:
                    out.append(target)
                    continue
                target.parent.mkdir(parents=True, exist_ok=True)
                with tf.extractfile(info) as fin, open(target, "wb") as fout:
                    shutil.copyfileobj(fin, fout)
                out.append(target)
    else:
        raise ValueError(f"{archive} is not a zip or tar archive")
    if wanted is not None:
        found = {p.name for p in out} | {os.path.relpath(p, dest) for p in out}
        missing = [m for m in wanted if m not in found and Path(m).name not in found]
        if missing:
            raise FileNotFoundError(f"{archive} does not contain {missing}")
    return out
