"""Quality checks beyond the catalogue contract: STAC schemas, file profiles, checksums.

Used by :func:`validate_catalog` (``deep=True``, ``irv-datapkg validate --deep``)
and by :func:`audit` (``irv-datapkg audit``). See docs/qa.md.

- :func:`stac_problems` validates a STAC document against STAC 1.0.0 core and
  every extension it declares, with schemas bundled in ``stac_schemas/`` so it
  works offline. A schema that is neither bundled nor reachable is skipped
  with a warning.
- :func:`cog_problems` checks a raster against ADR 0004 with rio-cogeo's
  validator plus compression, overviews, tiling and the nodata/data type the
  STAC asset declares.
- :func:`geoparquet_problems` checks GeoParquet metadata (version, CRS,
  ``covering.bbox``, geometry types) and row-group sizes, reading the footer only.
- :func:`audit` re-hashes data files (all, or a random sample) and reports
  mismatches, on a local catalogue or any fsspec remote.

Each check returns ``(errors, warnings)``: errors break the contract or ADR 0004,
warnings are deviations worth a look (e.g. a missing predictor).
"""

import csv
import json
import math
import posixpath
import random
from concurrent.futures import ThreadPoolExecutor
from dataclasses import asdict, dataclass, fields
from functools import lru_cache
from importlib import resources
from typing import Callable, Iterable, Iterator, List, Optional, Tuple

from . import _fs

ADR_BLOCKSIZE = 512
ROW_GROUP_TARGET = 100_000
# Row groups outside these bounds are worth a warning (ADR 0004: ~100k rows)
ROW_GROUP_MIN = 10_000
ROW_GROUP_MAX = 1_000_000
GEOPARQUET_VERSIONS = ("1.0.0", "1.1.0")
GEOMETRY_TYPES = {
    "Point",
    "LineString",
    "Polygon",
    "MultiPoint",
    "MultiLineString",
    "MultiPolygon",
    "GeometryCollection",
}
STAC_CORE = {
    "Feature": "https://schemas.stacspec.org/v{v}/item-spec/json-schema/item.json",
    "Collection": "https://schemas.stacspec.org/v{v}/collection-spec/json-schema/collection.json",
    "Catalog": "https://schemas.stacspec.org/v{v}/catalog-spec/json-schema/catalog.json",
}
FETCH_TIMEOUT = 10

Problems = Tuple[List[str], List[str]]


#
# STAC JSON schemas
#
class SchemaUnavailable(Exception):
    pass


@lru_cache(maxsize=None)
def _bundled() -> dict:
    """{uri: schema} for every JSON file under stac_schemas/ (keyed by ``$id``)."""
    out = {}

    def walk(node):
        for child in node.iterdir():
            if child.is_dir():
                walk(child)
            elif child.name.endswith(".json"):
                doc = json.loads(child.read_text())
                out[doc["$id"].rstrip("#")] = doc

    walk(resources.files(__package__).joinpath("stac_schemas"))
    return out


_remote_cache: dict = {}


def _fetch_schema(uri: str) -> dict:
    """A schema that is not bundled: fetch it over HTTPS once per process."""
    uri = uri.rstrip("#")
    if uri in _bundled():
        return _bundled()[uri]
    if uri not in _remote_cache:
        try:
            import requests

            r = requests.get(uri, timeout=FETCH_TIMEOUT)
            r.raise_for_status()
            _remote_cache[uri] = r.json()
        except Exception as e:  # network refused, offline, 404, not JSON ...
            _remote_cache[uri] = SchemaUnavailable(f"{uri}: {type(e).__name__}: {e}")
    value = _remote_cache[uri]
    if isinstance(value, Exception):
        raise value
    return value


@lru_cache(maxsize=None)
def _validator(uri: str):
    import jsonschema
    from referencing import Registry, Resource
    from referencing.jsonschema import DRAFT7

    def retrieve(ref):
        return Resource.from_contents(_fetch_schema(ref), default_specification=DRAFT7)

    registry = Registry(retrieve=retrieve).with_resources(
        (u, Resource.from_contents(s, default_specification=DRAFT7)) for u, s in _bundled().items()
    )
    schema = _fetch_schema(uri)
    cls = jsonschema.validators.validator_for(schema, default=jsonschema.Draft7Validator)
    return cls(schema, registry=registry)


def stac_problems(doc: dict) -> Problems:
    """Validate ``doc`` against its STAC core schema and each ``stac_extensions``
    schema. Schemas that cannot be loaded are skipped with a warning."""
    errors, warnings = [], []
    kind, version = doc.get("type"), doc.get("stac_version")
    if kind not in STAC_CORE or not version:
        return [f"not a STAC Catalog/Collection/Item (type={kind!r})"], []
    for uri in [STAC_CORE[kind].format(v=version)] + list(doc.get("stac_extensions") or []):
        name = uri if "stac-extensions" in uri else f"STAC {version} {kind}"
        try:
            validator = _validator(uri)
            found = sorted(validator.iter_errors(doc), key=lambda e: list(e.absolute_path))
        except Exception as e:
            reason = e if isinstance(e, SchemaUnavailable) else f"{type(e).__name__}: {e}"
            if isinstance(e, SchemaUnavailable) or "Unresolvable" in type(e).__name__:
                warnings.append(f"schema {name} skipped (not bundled, cannot fetch: {reason})")
                continue
            raise
        for e in found:
            where = "/".join(str(p) for p in e.absolute_path) or "<root>"
            errors.append(f"{name}: {where}: {e.message}")
    return errors, warnings


#
# Data files
#
def _same_number(a, b) -> bool:
    if a is None or b is None:
        return a is None and b is None
    try:
        a, b = float(a), float(b)
    except (TypeError, ValueError):
        return str(a) == str(b)
    return (math.isnan(a) and math.isnan(b)) or a == b


def cog_problems(href: str, asset: Optional[dict] = None, item: Optional[dict] = None) -> Problems:
    """ADR 0004 checks of a (local or GDAL-readable) COG against its STAC asset."""
    import rasterio

    errors, warnings = [], []
    asset, item = asset or {}, item or {}
    path = _fs.gdal_path(href)
    try:
        from rio_cogeo.cogeo import cog_validate
    except ImportError:
        cog_validate = None
        warnings.append("rio-cogeo not installed: COG layout not checked (pip install rio-cogeo)")
    if cog_validate is not None:
        _, cog_errors, _ = cog_validate(path, quiet=True)
        errors.extend(f"COG: {e}" for e in cog_errors)

    with rasterio.open(path) as ds:
        structure = ds.tags(ns="IMAGE_STRUCTURE")
        compression = structure.get("COMPRESSION")
        if not compression:
            errors.append("not compressed (ADR 0004: ZSTD or DEFLATE)")
        elif compression not in ("ZSTD", "DEFLATE"):
            warnings.append(f"compression {compression} (ADR 0004: ZSTD or DEFLATE)")
        if compression and structure.get("PREDICTOR") not in ("2", "3"):
            warnings.append("no predictor (ADR 0004: ZSTD/DEFLATE + predictor)")
        large = max(ds.width, ds.height) > ADR_BLOCKSIZE
        block = ds.block_shapes[0]
        if large and block != (ADR_BLOCKSIZE, ADR_BLOCKSIZE):
            if block[1] == ds.width:  # strips, not tiles
                errors.append(f"not tiled (block shape {block})")
            else:
                warnings.append(f"block shape {block} (ADR 0004: 512 px tiles)")
        if large and not ds.overviews(1):
            errors.append("no overviews (ADR 0004)")

        shape = item.get("properties", {}).get("proj:shape")
        if shape and list(shape) != [ds.height, ds.width]:
            errors.append(f"proj:shape {shape} but the file is {[ds.height, ds.width]}")
        bands = asset.get("raster:bands")
        if bands is not None and len(bands) != ds.count:
            errors.append(f"raster:bands lists {len(bands)} bands but the file has {ds.count}")
        for i, band in enumerate((bands or [])[: ds.count]):
            actual = ds.nodatavals[i]
            if band.get("nodata") is not None:
                if actual is None:
                    errors.append(f"band {i + 1}: nodata {band['nodata']} declared but not set")
                elif not _same_number(actual, band["nodata"]):
                    errors.append(
                        f"band {i + 1}: nodata is {actual} but STAC declares {band['nodata']}"
                    )
            elif actual is None:
                warnings.append(f"band {i + 1}: no nodata (ADR 0004: explicit nodata)")
            if band.get("data_type") and band["data_type"] != ds.dtypes[i]:
                errors.append(
                    f"band {i + 1}: data type {ds.dtypes[i]} but STAC declares {band['data_type']}"
                )
        if bands is None and any(v is None for v in ds.nodatavals):
            warnings.append("no nodata (ADR 0004: explicit nodata)")
    return errors, warnings


def _is_epsg4326(crs) -> bool:
    """GeoParquet ``crs``: absent means OGC:CRS84 (= EPSG:4326 in lon/lat order)."""
    from pyproj import CRS

    try:
        parsed = CRS.from_json_dict(crs) if isinstance(crs, dict) else CRS.from_user_input(crs)
    except Exception:
        return False
    if parsed.to_epsg() == 4326:
        return True
    return parsed.equals(CRS.from_user_input("OGC:CRS84"), ignore_axis_order=True)


def geoparquet_problems(
    href: str, asset: Optional[dict] = None, storage_options: Optional[dict] = None
) -> Problems:
    """GeoParquet metadata and row-group checks (footer only; no data is read)."""
    import pyarrow.parquet as pq

    errors, warnings = [], []
    asset = asset or {}
    fs, path = _fs.fs_for(href, storage_options)
    with fs.open(path, "rb") as fh:
        meta = pq.ParquetFile(fh).metadata
    schema = meta.schema.to_arrow_schema()
    raw = (meta.metadata or {}).get(b"geo")
    if raw is None:
        return ["no GeoParquet 'geo' metadata"], warnings
    try:
        geo = json.loads(raw)
    except ValueError as e:
        return [f"'geo' metadata is not JSON: {e}"], warnings

    version = geo.get("version")
    if version not in GEOPARQUET_VERSIONS:
        errors.append(f"GeoParquet version {version!r} (expected 1.1.0)")
    elif version != "1.1.0":
        warnings.append(f"GeoParquet {version} (ADR 0004: 1.1)")
    primary = geo.get("primary_column")
    columns = geo.get("columns") or {}
    if not primary or primary not in columns:
        errors.append(f"primary_column {primary!r} is not described in 'columns'")
    if primary and primary not in schema.names:
        errors.append(f"primary_column {primary!r} is not a column of the file")
    stac_primary = asset.get("table:primary_geometry")
    if stac_primary and primary and stac_primary != primary:
        errors.append(f"table:primary_geometry {stac_primary!r} but the file says {primary!r}")

    for name, col in columns.items():
        label = f"column {name}"
        if col.get("encoding") not in (
            "WKB",
            "point",
            "linestring",
            "polygon",
            "multipoint",
            "multilinestring",
            "multipolygon",
        ):
            errors.append(f"{label}: encoding {col.get('encoding')!r}")
        if "crs" in col and col["crs"] is None:
            errors.append(f"{label}: crs is null (unknown); ADR 0004 needs EPSG:4326")
        elif "crs" in col and not _is_epsg4326(col["crs"]):
            errors.append(f"{label}: crs is not EPSG:4326")
        types = col.get("geometry_types")
        if types is None:
            errors.append(f"{label}: geometry_types missing")
        else:
            bad = [t for t in types if t.removesuffix(" Z") not in GEOMETRY_TYPES]
            if bad:
                errors.append(f"{label}: unknown geometry_types {bad}")
            if not types:
                warnings.append(f"{label}: geometry_types is empty (any type)")
        bbox_cov = (col.get("covering") or {}).get("bbox")
        if not bbox_cov:
            errors.append(f"{label}: no covering.bbox (ADR 0004: bbox covering column)")
        else:
            for part in ("xmin", "ymin", "xmax", "ymax"):
                ref = bbox_cov.get(part)
                if not (isinstance(ref, list) and ref and ref[0] in schema.names):
                    errors.append(f"{label}: covering.bbox.{part} {ref!r} is not a column")

    rows = asset.get("table:row_count")
    if rows is not None and rows != meta.num_rows:
        errors.append(f"table:row_count {rows} but the file has {meta.num_rows} rows")
    groups = [meta.row_group(i).num_rows for i in range(meta.num_row_groups)]
    if any(g > ROW_GROUP_MAX for g in groups):
        warnings.append(
            f"row groups up to {max(groups)} rows (ADR 0004: ~{ROW_GROUP_TARGET} per group)"
        )
    small = [g for g in groups[:-1] if g < ROW_GROUP_MIN]
    if small:
        warnings.append(
            f"{len(small)} of {len(groups)} row groups have under {ROW_GROUP_MIN} rows "
            f"(ADR 0004: ~{ROW_GROUP_TARGET} per group)"
        )
    return errors, warnings


def file_problems(
    href: str, asset: dict, item: Optional[dict] = None, storage_options: Optional[dict] = None
) -> Problems:
    """Format checks for one data asset, chosen by file suffix / media type."""
    name = posixpath.basename(_fs.split(href)[1]).lower()
    media = asset.get("type") or ""
    try:
        if name.endswith((".tif", ".tiff")) or "geotiff" in media:
            # read through GDAL: remote credentials come from GDAL/AWS_* variables
            return cog_problems(href, asset, item)
        if name.endswith(".parquet") or "parquet" in media:
            return geoparquet_problems(href, asset, storage_options)
    except Exception as e:
        return [f"could not read: {type(e).__name__}: {e}"], []
    return [], [f"no format checks for {name}"]


#
# Asset walk, sampling and checksum audit
#
@dataclass
class AssetRef:
    access: str
    dataset: str
    version: str
    item_id: str
    key: str
    href: str
    size: Optional[int]
    checksum: Optional[str]  # multihash, as in file:checksum
    retired: bool = False


def sample(refs: list, n: Optional[int], seed: Optional[int] = None) -> list:
    """``n`` refs chosen at random (all of them when ``n`` is None or >= len)."""
    if n is None or n >= len(refs):
        return list(refs)
    picked = sorted(random.Random(seed).sample(range(len(refs)), n))
    return [refs[i] for i in picked]


def iter_assets(
    base: str,
    private_base: Optional[str] = None,
    storage_options: Optional[dict] = None,
    datasets: Optional[Iterable[str]] = None,
    release: Optional[str] = None,
) -> Iterator[AssetRef]:
    """Every asset of every item linked from the public and private roots.

    ``datasets`` limits the walk; ``release`` limits it to the versions the
    release pins. Collections or items that are missing are skipped (that is
    :func:`validate_catalog`'s job).
    """
    so = storage_options or {}
    base = _fs.normalise(base)
    roots = {
        "public": base,
        "private": _fs.normalise(private_base) if private_base else _fs.private_base(base),
    }
    wanted = set(datasets) if datasets else None
    pins = None
    if release:
        manifest = _fs.read_json(_fs.join(base, "releases", release, "catalog.json"), so)
        pins = {ds: p["version"] for ds, p in manifest.get("irv:datasets", {}).items()}
    for access, root in roots.items():
        root_url = _fs.join(root, "catalog.json")
        if not _fs.exists(root_url, so):
            continue
        for link in _fs.read_json(root_url, so).get("links", []):
            if link["rel"] != "child":
                continue
            coll_url = _fs.resolve(link["href"], root_url)
            if not _fs.exists(coll_url, so):
                continue
            coll = _fs.read_json(coll_url, so)
            dataset = coll.get("id")
            if wanted is not None and dataset not in wanted:
                continue
            if pins is not None and dataset not in pins:
                continue
            retired = set(coll.get("irv:retired") or {})
            for ilink in coll.get("links", []):
                if ilink["rel"] != "item":
                    continue
                item_url = _fs.resolve(ilink["href"], coll_url)
                version = posixpath.basename(posixpath.dirname(_fs.split(item_url)[1]))
                if pins is not None and pins[dataset] != version:
                    continue
                if not _fs.exists(item_url, so):
                    continue
                item = _fs.read_json(item_url, so)
                for key, asset in item.get("assets", {}).items():
                    yield AssetRef(
                        access,
                        dataset,
                        version,
                        item.get("id"),
                        key,
                        _fs.resolve(asset.get("href", ""), item_url),
                        asset.get("file:size"),
                        asset.get("file:checksum"),
                        version in retired,
                    )


def iter_lock_assets(
    lock_path: str, base: Optional[str] = None, private_base: Optional[str] = None
) -> Iterator[AssetRef]:
    """Every asset recorded in a ``datapkg.lock.yaml`` (``base`` overrides its base)."""
    from .lock import LockFile

    lock = LockFile.load(lock_path)
    if base is None:
        base, private_base = lock.base, private_base or lock.private_base
    roots = {"public": _fs.normalise(base)}
    roots["private"] = _fs.normalise(private_base) if private_base else _fs.private_base(base)
    for dataset, entry in lock.datasets.items():
        for item_id, assets in (entry.get("items") or {}).items():
            for key, a in assets.items():
                yield AssetRef(
                    entry["access"],
                    dataset,
                    entry["version"],
                    item_id,
                    key,
                    _fs.join(roots[entry["access"]], a["href"]),
                    a.get("size"),
                    _fs.to_multihash(a["sha256"]) if a.get("sha256") else None,
                )


@dataclass
class AuditResult:
    access: str
    dataset: str
    version: str
    item: str
    asset: str
    href: str
    status: str  # ok, mismatch, size, missing, retired, unchecked, error
    expected_size: Optional[int] = None
    actual_size: Optional[int] = None
    expected_sha256: Optional[str] = None
    actual_sha256: Optional[str] = None
    detail: str = ""

    @property
    def failed(self) -> bool:
        return self.status in ("mismatch", "size", "missing", "error")


AUDIT_FIELDS = [f.name for f in fields(AuditResult)]


def audit_asset(ref: AssetRef, storage_options: Optional[dict] = None) -> AuditResult:
    """Re-hash one asset file and compare with its recorded size and sha256."""
    expected = _fs.from_multihash(ref.checksum) if ref.checksum else None
    result = AuditResult(
        ref.access,
        ref.dataset,
        ref.version,
        ref.item_id,
        ref.key,
        ref.href,
        "ok",
        expected_size=ref.size,
        expected_sha256=expected,
    )
    try:
        fs, path = _fs.fs_for(ref.href, storage_options)
        if not fs.exists(path):
            result.status = "retired" if ref.retired else "missing"
            if ref.retired:
                result.detail = "retired version: files may be in cold storage"
            return result
        result.actual_size = fs.size(path)
        if ref.size is not None and result.actual_size != ref.size:
            result.status = "size"
            result.detail = f"size {result.actual_size}, expected {ref.size}"
            return result
        if expected is None:
            result.status = "unchecked"
            result.detail = "no file:checksum recorded"
            return result
        result.actual_sha256 = _fs.sha256_file(ref.href, storage_options)
        if result.actual_sha256 != expected:
            result.status = "mismatch"
            result.detail = "sha256 differs"
    except Exception as e:  # permissions, network ...
        result.status = "error"
        result.detail = f"{type(e).__name__}: {e}"
    return result


def audit(
    refs: Iterable[AssetRef],
    storage_options: Optional[dict] = None,
    sample_size: Optional[int] = None,
    seed: Optional[int] = None,
    jobs: int = 4,
    progress: Optional[Callable[[AuditResult], None]] = None,
) -> List[AuditResult]:
    """Re-hash assets (all, or ``sample_size`` chosen at random) with ``jobs`` threads."""
    chosen = sample(list(refs), sample_size, seed)

    def one(ref):
        result = audit_asset(ref, storage_options)
        if progress:
            progress(result)
        return result

    if jobs <= 1:
        return [one(r) for r in chosen]
    with ThreadPoolExecutor(max_workers=jobs) as pool:
        return list(pool.map(one, chosen))


def write_audit(results: List[AuditResult], path: str) -> str:
    with open(path, "w", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=AUDIT_FIELDS)
        writer.writeheader()
        for r in results:
            writer.writerow(asdict(r))
    return path
