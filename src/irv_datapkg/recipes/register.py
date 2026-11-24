"""Register T1 outputs as STAC items in the local workspace catalogue.

T1 files are written straight into their catalogue location
(``{catalogue_base}/{dataset}/{version}/{item}.tif|.parquet``) and then
registered: :func:`register_item` builds the item (size, sha256, raster/table
metadata, provenance) and adds it with :class:`~irv_datapkg.catalog.CatalogWriter`.
Because the file is already in place, the writer only checks its checksum.
"""

import datetime as dt
import fcntl
import os
from contextlib import contextmanager
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional, Union

from .metadata import CollectionMeta
from .provenance import find_provenance

PathLike = Union[str, Path]
EXTENSIONS = ("tif", "parquet")


@dataclass
class T1Item:
    """Declaration of one T1 item, made by each dataset recipe.

    ``properties`` holds the variant dimensions (hazard, rcp, epoch, gcm,
    return_period, ...) and must include every name in the collection's
    ``dimensions``. ``sources`` are the T0 raw files (with provenance sidecars)
    the item is derived from; ``source_url`` overrides the URL taken from them
    (e.g. for a mosaic of thousands of tiles). ``extra`` holds recipe-private
    values (archive member names ...) that are not published.
    """

    ext: str
    properties: dict = field(default_factory=dict)
    sources: list = field(default_factory=list)
    source_url: Optional[str] = None
    datetime: Optional[str] = None
    units: Optional[Union[str, list]] = None
    nodata: Optional[float] = None
    title: Optional[str] = None
    extra: dict = field(default_factory=dict)

    def __post_init__(self):
        if self.ext not in EXTENSIONS:
            raise ValueError(f"T1 item ext must be one of {EXTENSIONS}, not {self.ext!r}")


def collection_root(base: PathLike, coll: CollectionMeta) -> str:
    """``{base}`` for public collections, ``{base}-private`` for private ones."""
    base = str(base).rstrip("/")
    return base + "-private" if coll.access == "private" else base


def item_dir(base: PathLike, coll: CollectionMeta) -> str:
    return f"{collection_root(base, coll)}/{coll.id}/{coll.version}"


def item_data_path(base: PathLike, coll: CollectionMeta, item_id: str, ext: str) -> str:
    return f"{item_dir(base, coll)}/{item_id}.{ext}"


def item_json_path(base: PathLike, coll: CollectionMeta, item_id: str) -> str:
    return f"{item_dir(base, coll)}/{item_id}.json"


@contextmanager
def catalogue_lock(base: PathLike):
    """Exclusive lock on a local catalogue (``{base}.lock``), so parallel
    Snakemake jobs do not clobber ``collection.json``/``catalog.json``."""
    lock_path = Path(str(base).rstrip("/") + ".lock")
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    with open(lock_path, "w") as fh:
        fcntl.flock(fh, fcntl.LOCK_EX)
        try:
            yield
        finally:
            fcntl.flock(fh, fcntl.LOCK_UN)


def source_info(spec: T1Item) -> tuple:
    """``(source_url, retrieved)`` for an item from its sources' provenance."""
    urls, dates = [], []
    for src in spec.sources:
        _, entry, doc = find_provenance(src)
        if entry["source_url"] not in urls:
            urls.append(entry["source_url"])
        dates.append(doc["retrieved"])
    source_url = spec.source_url or " ".join(urls)
    if not source_url:
        raise ValueError("Item has no sources with provenance and no source_url")
    retrieved = max(dates) if dates else dt.date.today().strftime("%Y-%m-%d")
    return source_url, retrieved


def register_item(
    base: PathLike,
    coll: CollectionMeta,
    item_id: str,
    spec: T1Item,
    *,
    data_path: Optional[PathLike] = None,
    commit: Optional[str] = None,
) -> str:
    """Build the STAC item for a T1 file and add it (and its collection) to the
    local catalogue at ``base``. Returns the item JSON path."""
    from ..catalog import CatalogWriter, build_item

    missing = [d for d in coll.dimensions if d not in spec.properties]
    if missing:
        raise ValueError(f"{coll.id}/{item_id} lacks dimension properties {missing}")
    data_path = Path(data_path or item_data_path(base, coll, item_id, spec.ext))
    expected = Path(item_data_path(base, coll, item_id, spec.ext))
    source_url, retrieved = source_info(spec)
    item = build_item(
        data_path,
        coll.id,
        coll.version,
        item_id,
        source_url=source_url,
        retrieved=retrieved,
        properties=spec.properties,
        commit=commit,
        datetime=spec.datetime,
        units=spec.units if spec.units is not None else coll.units,
        nodata=spec.nodata if spec.nodata is not None else coll.nodata,
        title=spec.title,
    )
    if any(v != v for v in item["bbox"]):  # NaN: no features or no valid pixels
        raise ValueError(f"{data_path} is empty: drop {item_id} from the recipe's items")
    if data_path.name != expected.name:
        item["assets"]["data"]["href"] = "./" + expected.name
    with catalogue_lock(base):
        writer = CatalogWriter(os.path.abspath(str(base)))
        writer.add_collection(coll.stac())
        return writer.add_item(item, data_path)
