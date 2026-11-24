"""Read-side catalogue client (see docs/catalogue-spec.md)."""

import os
import posixpath
import shutil
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional, Union

import fsspec
import yaml

from . import _fs

LOCK_VERSION = 1


class CatalogError(Exception):
    pass


class ChecksumError(CatalogError):
    pass


@dataclass
class Asset:
    key: str
    href: str  # absolute location (local path or fsspec URL)
    dataset: str
    version: str
    item_id: str
    access: str
    fields: dict = field(repr=False, default_factory=dict)

    @property
    def sha256(self) -> Optional[str]:
        checksum = self.fields.get("file:checksum")
        return _fs.from_multihash(checksum) if checksum else None

    @property
    def size(self) -> Optional[int]:
        return self.fields.get("file:size")

    @property
    def type(self) -> Optional[str]:
        return self.fields.get("type")

    @property
    def filename(self) -> str:
        return posixpath.basename(_fs.split(self.href)[1])


@dataclass
class Item:
    id: str
    dataset: str
    version: str
    url: str
    properties: dict
    assets: dict
    stac: dict = field(repr=False, default_factory=dict)

    @property
    def asset(self) -> Asset:
        """The primary asset: the one with role ``data`` (or the only asset)."""
        data = [a for a in self.assets.values() if "data" in a.fields.get("roles", [])]
        if len(data) == 1:
            return data[0]
        if len(self.assets) == 1:
            return next(iter(self.assets.values()))
        raise CatalogError(f"Item {self.id} has no single primary 'data' asset")


def _matches(properties: dict, filters: dict) -> bool:
    for key, wanted in filters.items():
        if key not in properties:
            return False
        value = properties[key]
        options = wanted if isinstance(wanted, (list, tuple, set)) else [wanted]
        if str(value) not in {str(o) for o in options}:
            return False
    return True


def default_cache_dir() -> str:
    return os.environ.get(
        "IRV_DATAPKG_CACHE", os.path.join(os.path.expanduser("~"), ".cache", "irv-datapkg")
    )


class Catalog:
    """A view of an irv-datapkg STAC catalogue, optionally pinned to a release.

    Use :meth:`open` or :meth:`from_lock` rather than the constructor.
    """

    def __init__(
        self,
        base: str,
        release: Optional[str] = None,
        pins: Optional[dict] = None,
        private_base: Optional[str] = None,
        storage_options: Optional[dict] = None,
        cache_dir: Optional[str] = None,
        locked: Optional[dict] = None,
    ):
        self.base = _fs.normalise(base)
        self.private_base = _fs.normalise(private_base) if private_base else _fs.private_base(base)
        self.release = release
        self.pins = pins  # {dataset: {"version": ..., "access": ...}} or None for latest
        self.storage_options = storage_options or {}
        self.cache_dir = cache_dir or default_cache_dir()
        self._locked = locked or {}
        self._collections: dict = {}  # dataset -> (collection dict, url, access)

    # -- construction -------------------------------------------------------

    @classmethod
    def open(cls, base: str, release: Optional[str] = None, **kwargs) -> "Catalog":
        """Open the catalogue at ``base``; if ``release`` is given, pin dataset
        versions to that release manifest, otherwise use the latest versions."""
        catalog = cls(base, release=release, **kwargs)
        if release is not None:
            url = _fs.join(catalog.base, "releases", release, "catalog.json")
            try:
                manifest = _fs.read_json(url, catalog.storage_options)
            except FileNotFoundError:
                raise CatalogError(f"Release {release} not found at {url}") from None
            catalog.pins = manifest["irv:datasets"]
        return catalog

    @classmethod
    def from_lock(cls, lock_path: Union[str, Path], **kwargs) -> "Catalog":
        """Open the catalogue exactly as recorded in a ``datapkg.lock.yaml``."""
        with open(lock_path) as f:
            lock = yaml.safe_load(f)
        if lock.get("lock_version") != LOCK_VERSION:
            raise CatalogError(f"Unsupported lock_version in {lock_path}")
        kwargs.setdefault("private_base", lock.get("private_base"))
        pins = {
            ds: {"version": d["version"], "access": d["access"]}
            for ds, d in lock["datasets"].items()
        }
        locked = {ds: d.get("items", {}) for ds, d in lock["datasets"].items()}
        return cls(lock["base"], release=lock.get("release"), pins=pins, locked=locked, **kwargs)

    # -- discovery ----------------------------------------------------------

    def _root(self, access: str) -> str:
        return self.private_base if access == "private" else self.base

    def _load_collection(self, dataset: str, access: Optional[str] = None):
        if dataset in self._collections:
            return self._collections[dataset]
        if self.pins is not None:
            if dataset not in self.pins:
                raise CatalogError(f"Dataset {dataset} is not in release {self.release}")
            access = self.pins[dataset]["access"]
        candidates = [access] if access else ["public", "private"]
        for acc in candidates:
            url = _fs.join(self._root(acc), dataset, "collection.json")
            try:
                collection = _fs.read_json(url, self.storage_options)
            except (FileNotFoundError, PermissionError):
                continue
            self._collections[dataset] = (collection, url, acc)
            return self._collections[dataset]
        where = " or ".join(self._root(a) for a in candidates)
        raise CatalogError(f"Collection {dataset} not found (or not accessible) under {where}")

    def _root_children(self, access: str) -> list:
        url = _fs.join(self._root(access), "catalog.json")
        try:
            root = _fs.read_json(url, self.storage_options)
        except (FileNotFoundError, PermissionError):
            if access == "public":
                raise CatalogError(f"No catalog.json at {url}") from None
            return []  # private prefix absent or not accessible: skip silently
        return [
            posixpath.basename(posixpath.dirname(_fs.split(_fs.resolve(lk["href"], url))[1]))
            for lk in root.get("links", [])
            if lk["rel"] == "child"
        ]

    def datasets(self) -> list:
        """Dataset ids visible in this view (release pins, or both roots)."""
        if self.pins is not None:
            return sorted(self.pins)
        names = []
        for access in ("public", "private"):
            for name in self._root_children(access):
                if name not in names:
                    names.append(name)
                    self._collections.pop(name, None)
                    self._load_collection(name, access)
        return names

    def collections(self) -> list:
        """STAC collection dicts for every dataset in this view."""
        return [self.collection(d) for d in self.datasets()]

    def collection(self, dataset: str) -> dict:
        return self._load_collection(dataset)[0]

    def access(self, dataset: str) -> str:
        return self._load_collection(dataset)[2]

    def version(self, dataset: str) -> str:
        """Pinned version (release/lock) or the latest published version."""
        if self.pins is not None and dataset in self.pins:
            return self.pins[dataset]["version"]
        versions = self.collection(dataset).get("irv:versions", [])
        if not versions:
            raise CatalogError(f"Collection {dataset} lists no versions")
        return versions[-1]

    def items(self, dataset: str, version: Optional[str] = None, **filters) -> list:
        """Items of one dataset version whose properties match all ``filters``.

        A filter value may be a scalar or a list of accepted values; values are
        compared as strings, so ``return_period=100`` and ``"100"`` both match.
        """
        collection, url, access = self._load_collection(dataset)
        version = version or self.version(dataset)
        locked = self._locked.get(dataset)
        result = []
        for link in collection.get("links", []):
            if link["rel"] != "item":
                continue
            item_url = _fs.resolve(link["href"], url)
            if posixpath.basename(posixpath.dirname(_fs.split(item_url)[1])) != version:
                continue
            stac = _fs.read_json(item_url, self.storage_options)
            if not _matches(stac["properties"], filters):
                continue
            item = self._make_item(stac, item_url, dataset, version, access)
            if locked is not None:
                self._check_locked(item, locked)
            result.append(item)
        return result

    def item(self, dataset: str, item_id: str, version: Optional[str] = None) -> Item:
        for item in self.items(dataset, version):
            if item.id == item_id:
                return item
        raise CatalogError(f"Item {item_id} not found in {dataset}")

    @staticmethod
    def _make_item(stac, item_url, dataset, version, access) -> Item:
        assets = {
            key: Asset(
                key, _fs.resolve(a["href"], item_url), dataset, version, stac["id"], access, a
            )
            for key, a in stac.get("assets", {}).items()
        }
        return Item(stac["id"], dataset, version, item_url, stac["properties"], assets, stac)

    @staticmethod
    def _check_locked(item: Item, locked: dict) -> None:
        if item.id not in locked:
            raise CatalogError(f"Item {item.id} of {item.dataset} is not in the lock file")
        for key, entry in locked[item.id].items():
            asset = item.assets.get(key)
            if asset is None or asset.sha256 != entry["sha256"]:
                raise ChecksumError(
                    f"{item.dataset}/{item.version}/{item.id}:{key} differs from the lock file"
                )

    # -- access -------------------------------------------------------------

    @staticmethod
    def _asset(obj: Union[Item, Asset]) -> Asset:
        return obj.asset if isinstance(obj, Item) else obj

    def href(self, obj: Union[Item, Asset], gdal: bool = True) -> str:
        """Location of an item's primary asset (or an asset).

        With ``gdal=True`` return a GDAL path (``/vsis3/...``, ``/vsicurl/...`` or
        a local path) suitable for windowed reads; otherwise the fsspec URL.
        """
        asset = self._asset(obj)
        return _fs.gdal_path(asset.href) if gdal else asset.href

    def fetch(self, obj: Union[Item, Asset], dest: Optional[Union[str, Path]] = None) -> Path:
        """Download an asset into the local cache, verifying sha256, and
        optionally copy it to ``dest`` (a file path or an existing directory).
        Returns the path of the local copy."""
        asset = self._asset(obj)
        cached = Path(self.cache_dir, asset.access, asset.dataset, asset.version, asset.filename)
        expected = asset.sha256
        if not self._cache_valid(cached, asset):
            cached.parent.mkdir(parents=True, exist_ok=True)
            tmp = cached.with_name(cached.name + ".part")
            with (
                fsspec.open(asset.href, "rb", **self.storage_options) as src,
                open(tmp, "wb") as out,
            ):
                shutil.copyfileobj(src, out, _fs.CHUNK)
            actual = _fs.sha256_file(str(tmp))
            if expected is not None and actual != expected:
                tmp.unlink()
                raise ChecksumError(f"sha256 mismatch for {asset.href}: {actual} != {expected}")
            os.replace(tmp, cached)
            Path(str(cached) + ".sha256").write_text(actual + "\n")
        if dest is None:
            return cached
        dest = Path(dest)
        if dest.is_dir():
            dest = dest / asset.filename
        dest.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(cached, dest)
        return dest

    @staticmethod
    def _cache_valid(cached: Path, asset: Asset) -> bool:
        sidecar = Path(str(cached) + ".sha256")
        if not cached.exists() or not sidecar.exists():
            return False
        if asset.size is not None and cached.stat().st_size != asset.size:
            return False
        recorded = sidecar.read_text().strip()
        return asset.sha256 is None or recorded == asset.sha256

    # -- locking ------------------------------------------------------------

    def lock(self, datasets: Optional[list] = None) -> dict:
        """Lock file content pinning every (or the given) dataset of this release."""
        if self.release is None:
            raise CatalogError("Lock files pin a release: open the catalogue with release=...")
        out = {
            "lock_version": LOCK_VERSION,
            "base": self.base,
            "private_base": self.private_base,
            "release": self.release,
            "datasets": {},
        }
        for dataset in datasets or self.datasets():
            access = self.access(dataset)
            root = self._root(access)
            items = {}
            for item in self.items(dataset):
                items[item.id] = {
                    key: {
                        "href": posixpath.relpath(_fs.split(a.href)[1], _fs.split(root)[1]),
                        "sha256": a.sha256,
                        "size": a.size,
                    }
                    for key, a in item.assets.items()
                }
            out["datasets"][dataset] = {
                "version": self.version(dataset),
                "access": access,
                "items": items,
            }
        return out

    def write_lock(self, path: Union[str, Path] = "datapkg.lock.yaml", datasets=None) -> Path:
        with open(path, "w") as f:
            yaml.safe_dump(self.lock(datasets), f, sort_keys=False)
        return Path(path)
