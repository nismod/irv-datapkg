"""Read-side catalogue client (see docs/catalogue-spec.md)."""

import hashlib
import os
import posixpath
import shutil
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional, Union

import fsspec
import yaml

from . import _fs
from .lock import (
    ABSENT,
    LOCK_VERSION,
    CatalogError,
    ChecksumError,
    LockDiff,
    LockFile,
    RetiredError,
    matches as _matches,
)

LINK_MODES = ("copy", "hardlink", "symlink")


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
        """File name in the catalogue (basename of ``href``)."""
        return posixpath.basename(_fs.split(self.href)[1])

    @property
    def source_url(self) -> Optional[str]:
        """Upstream URL of the T0 file(s) this asset was derived from."""
        return self.fields.get("irv:source_url")

    @property
    def source_filename(self) -> Optional[str]:
        """Upstream file name: ``irv:source_filename``, else the basename of
        ``irv:source_url`` if that looks like a file, else None."""
        return self.fields.get("irv:source_filename") or _fs.source_filename(self.source_url)


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


def default_cache_dir() -> str:
    return os.environ.get(
        "IRV_DATAPKG_CACHE", os.path.join(os.path.expanduser("~"), ".cache", "irv-datapkg")
    )


def _place(src: Path, dest: Path, link: str) -> None:
    """Put ``src`` at ``dest`` by copy, hardlink (copy across filesystems) or
    symlink, replacing ``dest`` atomically."""
    dest.parent.mkdir(parents=True, exist_ok=True)
    if link == "hardlink" and dest.exists() and not dest.is_symlink():
        try:
            if os.path.samefile(src, dest):
                return
        except OSError:
            pass
    tmp = dest.with_name(f".{dest.name}.{os.getpid()}.tmp")
    if tmp.exists() or tmp.is_symlink():
        tmp.unlink()
    if link == "symlink":
        os.symlink(os.path.abspath(src), tmp)
    elif link == "hardlink":
        try:
            os.link(src, tmp)
        except OSError:  # e.g. a different filesystem: fall back to copying
            shutil.copyfile(src, tmp)
    else:
        shutil.copyfile(src, tmp)
    os.replace(tmp, dest)


def _normalise_selection(spec) -> list:
    """A lock selection as a list of item ids (str) and filter dicts."""
    if spec is None:
        return [{}]
    if isinstance(spec, (str, dict)):
        return [spec]
    return list(spec)


class Catalog:
    """A view of an irv-datapkg STAC catalogue, optionally pinned to a release.

    Use :meth:`open` or :meth:`from_lock` rather than the constructor.
    """

    ABSENT = ABSENT  # filter value: property absent or null

    def __init__(
        self,
        base: str,
        release: Optional[str] = None,
        pins: Optional[dict] = None,
        private_base: Optional[str] = None,
        storage_options: Optional[dict] = None,
        cache_dir: Optional[str] = None,
        locked: Optional[LockFile] = None,
    ):
        self._base_given = str(base).rstrip("/") if not str(base).endswith("://") else str(base)
        self._private_given = str(private_base) if private_base else None
        self.base = _fs.normalise(base)
        self.private_base = _fs.normalise(private_base) if private_base else _fs.private_base(base)
        self.release = release
        self.pins = pins  # {dataset: {"version": ..., "access": ...}} or None for latest
        self.storage_options = storage_options or {}
        self.cache_dir = cache_dir or default_cache_dir()
        self.lockfile = locked
        self._manifest_pins = False  # True when pins were read from the release manifest
        self._collections: dict = {}  # dataset -> (collection dict, url, access)

    # -- construction -------------------------------------------------------

    @classmethod
    def open(cls, base: str, release: Optional[str] = None, **kwargs) -> "Catalog":
        """Open the catalogue at ``base``; if ``release`` is given, pin dataset
        versions to that release manifest, otherwise use the latest versions."""
        catalog = cls(base, release=release, **kwargs)
        if release is not None:
            catalog.pins = catalog._release_pins(release)
            catalog._manifest_pins = True
        return catalog

    def _release_pins(self, release: str) -> dict:
        url = _fs.join(self.base, "releases", str(release), "catalog.json")
        try:
            manifest = _fs.read_json(url, self.storage_options)
        except FileNotFoundError:
            raise CatalogError(f"Release {release} not found at {url}") from None
        return manifest["irv:datasets"]

    @classmethod
    def from_lock(
        cls,
        lock_path: Union[str, Path],
        base: Optional[str] = None,
        private_base: Optional[str] = None,
        **kwargs,
    ) -> "Catalog":
        """Open the catalogue exactly as recorded in a ``datapkg.lock.yaml``.

        ``base`` overrides the recorded base, e.g. to read from a mirror; the
        private base is then ``private_base`` or ``{base}-private``. Without
        ``base``, the recorded ``base`` (relative paths are relative to the
        lock file) and ``private_base`` are used.
        """
        lock = LockFile.load(lock_path)
        if base is None:
            base = lock.base
            if private_base is None:
                private_base = lock.private_base
        catalog = cls(
            base,
            release=lock.release,
            pins=lock.pins(),
            private_base=private_base,
            locked=lock,
            **kwargs,
        )
        if base == lock.base and _fs.is_relative_local(lock.raw_base):
            # keep it relative, so write_lock() records it relative again
            catalog._base_given = os.path.relpath(lock.base)
        return catalog

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

    def dimensions(self, dataset: str) -> list:
        """The collection's ``irv:dimensions``: property names that distinguish items."""
        return list(self.collection(dataset).get("irv:dimensions", []))

    def retirement(self, dataset: str, version: Optional[str] = None) -> Optional[dict]:
        """The ``irv:retired`` entry (``date``, ``note``, ``location``) of a
        dataset version, or None if its files are online."""
        version = version or self.version(dataset)
        return (self.collection(dataset).get("irv:retired") or {}).get(version)

    def _retired_error(self, dataset: str, version: str) -> Optional[RetiredError]:
        try:
            info = self.retirement(dataset, version)
        except CatalogError:
            return None
        if not info:
            return None
        detail = "; ".join(str(info[k]) for k in ("note", "location") if info.get(k))
        return RetiredError(
            f"{dataset} {version} was retired to cold storage on {info.get('date')}"
            + (f" ({detail})" if detail else "")
            + ". Its metadata is still in the catalogue but its files are offline: "
            "move to a newer release, or ask the irv-datapkg maintainers for a restore."
        )

    def _check_dimensions(self, dataset: str, filters: dict) -> None:
        dims = self.dimensions(dataset)
        unknown = [k for k in filters if k not in dims]
        if unknown:
            raise CatalogError(
                f"{dataset}: filter keys {unknown} are not dimensions of the collection "
                f"(irv:dimensions: {dims})"
            )

    def items(
        self, dataset: str, version: Optional[str] = None, strict: bool = False, **filters
    ) -> list:
        """Items of one dataset version whose properties match all ``filters``.

        A filter value may be a scalar or a list of accepted values; values are
        compared as strings, so ``return_period=100`` and ``"100"`` both match.
        ``None`` or :attr:`Catalog.ABSENT` matches a property that is absent or
        null (``rcp=[None, "4.5"]`` accepts either). With ``strict=True``, a
        filter key that is not in the collection's ``irv:dimensions`` is an error.

        From a lock file, only the locked items are listed, each read directly.
        """
        collection, url, access = self._load_collection(dataset)
        version = version or self.version(dataset)
        if strict:
            self._check_dimensions(dataset, filters)
        lock = self.lockfile
        if lock is not None and dataset in lock.datasets:
            if version != lock.version(dataset):
                raise CatalogError(f"{dataset} is locked at {lock.version(dataset)}, not {version}")
            result = []
            for item_id in lock.item_ids(dataset):
                props = lock.properties(dataset, item_id)
                if props is not None and all(k in props for k in filters):
                    if not _matches(props, filters):
                        continue  # skip the read: recorded properties already rule it out
                item = self.item(dataset, item_id, version)
                if _matches(item.properties, filters):
                    result.append(item)
            return result
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
            result.append(self._make_item(stac, item_url, dataset, version, access))
        return result

    def item(self, dataset: str, item_id: str, version: Optional[str] = None) -> Item:
        """One item, read directly from ``{dataset}/{version}/{item_id}.json``
        (falling back to a scan of the collection's item links)."""
        if "/" in item_id or item_id in ("", ".", ".."):
            raise CatalogError(f"Invalid item id {item_id!r}")
        collection, url, access = self._load_collection(dataset)
        version = version or self.version(dataset)
        item_url = _fs.join(url, "..", version, item_id + ".json")
        try:
            stac = _fs.read_json(item_url, self.storage_options)
        except FileNotFoundError:
            stac = None
            for link in collection.get("links", []):
                if link["rel"] != "item":
                    continue
                candidate = _fs.resolve(link["href"], url)
                if posixpath.basename(posixpath.dirname(_fs.split(candidate)[1])) != version:
                    continue
                doc = _fs.read_json(candidate, self.storage_options)
                if doc.get("id") == item_id:
                    stac, item_url = doc, candidate
                    break
            if stac is None:
                raise CatalogError(f"Item {item_id} not found in {dataset} {version}") from None
        item = self._make_item(stac, item_url, dataset, version, access)
        lock = self.lockfile
        if lock is not None and dataset in lock.datasets:
            self._check_locked(item, lock)
        return item

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
    def _check_locked(item: Item, lock: LockFile) -> None:
        if item.id not in lock.item_ids(item.dataset):
            raise CatalogError(f"Item {item.id} of {item.dataset} is not in the lock file")
        for key, entry in lock.datasets[item.dataset]["items"][item.id].items():
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
        Raises :class:`RetiredError` for a retired version.
        """
        asset = self._asset(obj)
        retired = self._retired_error(asset.dataset, asset.version)
        if retired:
            raise retired
        return _fs.gdal_path(asset.href) if gdal else asset.href

    def relative_href(self, obj: Union[Item, Asset]) -> str:
        """An asset's location relative to its root (``{BASE}`` or ``{BASE}-private``),
        e.g. ``flood/v1-r1/flood_rp10.tif``: the same on every mirror."""
        asset = self._asset(obj)
        root_prefix, root_path = _fs.split(self._root(asset.access))
        prefix, path = _fs.split(asset.href)
        root_path = root_path.rstrip("/")
        if prefix != root_prefix or not path.startswith(root_path + "/"):
            raise CatalogError(f"{asset.href} is not under {self._root(asset.access)}")
        return path[len(root_path) + 1 :]

    def cache_path(self, obj: Union[Item, Asset]) -> Path:
        """Where :meth:`fetch` caches an asset."""
        asset = self._asset(obj)
        return Path(self.cache_dir, asset.access, asset.dataset, asset.version, asset.filename)

    def fetch(
        self,
        obj: Union[Item, Asset],
        dest: Optional[Union[str, Path]] = None,
        link: str = "copy",
        cache: bool = True,
    ) -> Path:
        """Download an asset, verifying size and sha256, and return its local path.

        With ``cache=True`` (default) the file is downloaded once into the
        cache; with ``dest`` (a file path or an existing directory) it is then
        put there by ``link``: ``"copy"``, ``"hardlink"`` (no second copy on
        disk; falls back to copying across filesystems) or ``"symlink"``.
        Cached files are read-only, so a hardlinked ``dest`` must not be
        edited in place. With ``cache=False`` the file is streamed straight
        to ``dest`` (required), verified while streaming, and renamed into
        place atomically; the cache is not used.

        Raises :class:`RetiredError` if the version was retired.
        """
        asset = self._asset(obj)
        retired = self._retired_error(asset.dataset, asset.version)
        if retired:
            raise retired
        return self._fetch_asset(asset, dest, link, cache)

    def _fetch_asset(self, asset: Asset, dest, link: str, cache: bool) -> Path:
        if link not in LINK_MODES:
            raise ValueError(f"link must be one of {LINK_MODES}, not {link!r}")
        if dest is not None:
            dest = Path(dest)
            if dest.is_dir():
                dest = dest / asset.filename
        if not cache:
            if dest is None:
                raise ValueError("fetch(cache=False) needs dest")
            self._download(asset, dest)
            return dest
        cached = self.cache_path(asset)
        if not self._cache_valid(cached, asset):
            actual = self._download(asset, cached)
            Path(str(cached) + ".sha256").write_text(actual + "\n")
            try:
                os.chmod(cached, 0o444)  # protect hardlinked copies from edits in place
            except OSError:
                pass
        if dest is None:
            return cached
        _place(cached, dest, link)
        return dest

    def _download(self, asset: Asset, target: Path) -> str:
        """Stream ``asset`` to ``target`` (atomic rename), checking size and sha256."""
        target.parent.mkdir(parents=True, exist_ok=True)
        tmp = target.with_name(f"{target.name}.{os.getpid()}.part")
        digest, size = hashlib.sha256(), 0
        try:
            with (
                fsspec.open(asset.href, "rb", **self.storage_options) as src,
                open(tmp, "wb") as out,
            ):
                for chunk in iter(lambda: src.read(_fs.CHUNK), b""):
                    digest.update(chunk)
                    size += len(chunk)
                    out.write(chunk)
        except FileNotFoundError:
            tmp.unlink(missing_ok=True)
            retired = self._retired_error(asset.dataset, asset.version)
            if retired:
                raise retired from None
            raise CatalogError(f"{asset.href} not found") from None
        except BaseException:
            tmp.unlink(missing_ok=True)
            raise
        actual = digest.hexdigest()
        if asset.size is not None and size != asset.size:
            tmp.unlink()
            raise ChecksumError(f"size mismatch for {asset.href}: {size} != {asset.size}")
        if asset.sha256 is not None and actual != asset.sha256:
            tmp.unlink()
            raise ChecksumError(f"sha256 mismatch for {asset.href}: {actual} != {asset.sha256}")
        os.replace(tmp, target)
        return actual

    @staticmethod
    def _cache_valid(cached: Path, asset: Asset) -> bool:
        sidecar = Path(str(cached) + ".sha256")
        if not cached.exists() or not sidecar.exists():
            return False
        if asset.size is not None and cached.stat().st_size != asset.size:
            return False
        recorded = sidecar.read_text().strip()
        return asset.sha256 is None or recorded == asset.sha256

    # -- locked access (no catalogue reads) ---------------------------------

    def locked_asset(self, dataset: str, item_id: str, key: Optional[str] = None) -> Asset:
        """An :class:`Asset` built from the lock file entry alone (href, sha256,
        size, source_filename): no STAC documents are read."""
        lock = self.lockfile
        if lock is None:
            raise CatalogError("This catalogue was not opened from a lock file (from_lock)")
        key, entry = lock.entry(dataset, item_id, key)
        access = lock.access(dataset)
        fields = {"file:checksum": _fs.to_multihash(entry["sha256"]), "file:size": entry["size"]}
        for name in ("source_filename", "source_url"):
            if entry.get(name):
                fields[f"irv:{name}"] = entry[name]
        href = _fs.join(self._root(access), entry["href"])
        return Asset(key, href, dataset, lock.version(dataset), item_id, access, fields)

    def fetch_locked(
        self,
        dataset: str,
        item_id: str,
        dest: Optional[Union[str, Path]] = None,
        key: Optional[str] = None,
        link: str = "copy",
        cache: bool = True,
    ) -> Path:
        """:meth:`fetch` a locked asset using only the lock file entry (href +
        sha256 + size). The catalogue is read only to explain a missing file
        (e.g. a retired version)."""
        return self._fetch_asset(self.locked_asset(dataset, item_id, key), dest, link, cache)

    # -- locking ------------------------------------------------------------

    def _select_items(self, dataset: str, spec) -> list:
        """Items selected by a lock selection: None (all), an item id, a filter
        dict, or a list of ids and/or filter dicts (union)."""
        chosen = {}
        for entry in _normalise_selection(spec):
            if isinstance(entry, str):
                found = [self.item(dataset, entry)]
            elif isinstance(entry, dict):
                found = self.items(dataset, **entry)
                if not found and entry:
                    raise CatalogError(f"{dataset}: no items match {entry}")
            else:
                raise TypeError(f"{dataset}: selection entries are item ids or filter dicts")
            for item in found:
                chosen.setdefault(item.id, item)
        return [chosen[k] for k in sorted(chosen)]

    def lock(
        self,
        datasets: Optional[list] = None,
        select: Optional[dict] = None,
        base: Optional[str] = None,
        private_base: Optional[str] = None,
    ) -> dict:
        """Lock file content pinning datasets of this release.

        ``datasets`` defaults to the keys of ``select``, or every dataset.
        ``select`` maps a dataset to the items to lock: an item id, a filter
        dict (as for :meth:`items`), or a list of ids and/or filter dicts;
        unselected datasets lock all their items. ``base``/``private_base``
        are recorded instead of the bases this catalogue was opened with.
        """
        if self.release is None:
            raise CatalogError("Lock files pin a release: open the catalogue with release=...")
        select = dict(select or {})
        names = list(datasets) if datasets else ([] if select else self.datasets())
        names += [d for d in select if d not in names]
        out = {"lock_version": LOCK_VERSION, "base": base or self._base_given}
        recorded_private = private_base or self._private_given
        if recorded_private:
            out["private_base"] = recorded_private
        out["release"] = self.release
        out["datasets"] = {}
        for dataset in names:
            version = self.version(dataset)
            retired = self._retired_error(dataset, version)
            if retired:
                raise retired
            dims = self.dimensions(dataset)
            items, properties = {}, {}
            for item in self._select_items(dataset, select.get(dataset)):
                entries = {}
                for key, a in item.assets.items():
                    entry = {"href": self.relative_href(a), "sha256": a.sha256, "size": a.size}
                    if a.source_filename:
                        entry["source_filename"] = a.source_filename
                    entries[key] = entry
                items[item.id] = entries
                if dims:
                    properties[item.id] = {d: item.properties.get(d) for d in dims}
            out["datasets"][dataset] = {
                "version": version,
                "access": self.access(dataset),
                "items": items,
            }
            if dims:
                out["datasets"][dataset]["properties"] = properties
        return out

    def write_lock(
        self,
        path: Union[str, Path] = "datapkg.lock.yaml",
        datasets: Optional[list] = None,
        select: Optional[dict] = None,
        base: Optional[str] = None,
        private_base: Optional[str] = None,
    ) -> Path:
        """Write :meth:`lock` to ``path``. A relative local base is recorded
        relative to the lock file's directory."""
        lock = self.lock(datasets, select=select, base=base, private_base=private_base)
        lock_dir = os.path.dirname(os.path.abspath(path))
        for key in ("base", "private_base"):
            if key in lock and _fs.is_relative_local(lock[key]):
                lock[key] = os.path.relpath(os.path.abspath(lock[key]), lock_dir)
        with open(path, "w") as f:
            yaml.safe_dump(lock, f, sort_keys=False)
        return Path(path)

    def lock_matches(
        self,
        lock_path: Union[str, Path],
        datasets: Optional[list] = None,
        release: Optional[str] = None,
        select: Optional[dict] = None,
    ) -> LockDiff:
        """Compare a lock file with this catalogue: truthy if it is up to date.

        Checks the release (default: this catalogue's), that every dataset in
        ``datasets`` (and ``select``) is locked, that locked versions and
        access match the release manifest, and that no locked version is
        retired. With ``select`` (as for :meth:`lock`), also checks that every
        selected item is locked with the current checksum (reads those items).
        Bases are not compared, so a lock used with a mirror still matches.
        """
        try:
            lock = LockFile.load(lock_path)
        except FileNotFoundError:
            return LockDiff([f"{lock_path} does not exist"])
        except CatalogError as e:
            return LockDiff([str(e)])
        release = str(release or self.release or "")
        if not release:
            raise CatalogError("lock_matches needs a release (or a catalogue opened with one)")
        if lock.release != release:
            return LockDiff([f"release {lock.release} != {release}"])
        ref = self
        if not (self._manifest_pins and str(self.release) == release):
            ref = Catalog.open(
                self.base,
                release=release,
                private_base=self.private_base,
                storage_options=self.storage_options,
                cache_dir=self.cache_dir,
            )
        select = dict(select or {})
        wanted = list(datasets or []) + [d for d in select if d not in (datasets or [])]
        problems = []
        missing = [d for d in wanted if d not in lock.datasets]
        if missing:
            problems.append(f"datasets not locked: {', '.join(missing)}")
        for dataset in wanted or list(lock.datasets):
            if dataset not in lock.datasets:
                continue
            pin = ref.pins.get(dataset)
            locked_version = lock.version(dataset)
            if pin is None:
                problems.append(f"{dataset} is not in release {release}")
                continue
            if pin["version"] != locked_version:
                problems.append(
                    f"{dataset} locked at {locked_version}, release pins {pin['version']}"
                )
                continue
            if pin["access"] != lock.access(dataset):
                problems.append(f"{dataset} access {lock.access(dataset)} != {pin['access']}")
            if ref._retired_error(dataset, locked_version):
                problems.append(f"{dataset} {locked_version} is retired")
            if dataset in select:
                locked_ids = set(lock.item_ids(dataset))
                for item in ref._select_items(dataset, select[dataset]):
                    if item.id not in locked_ids:
                        problems.append(f"{dataset}/{item.id} is not locked")
                        continue
                    for key, a in item.assets.items():
                        entry = lock.datasets[dataset]["items"][item.id].get(key)
                        if entry is None or entry.get("sha256") != a.sha256:
                            problems.append(f"{dataset}/{item.id}:{key} checksum differs")
        return LockDiff(problems)
