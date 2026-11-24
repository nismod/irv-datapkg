"""Consumer lock files (``datapkg.lock.yaml``, docs/catalogue-spec.md section 6).

:class:`LockFile` reads a lock file without touching the catalogue, so locked
assets can be fetched from ``href`` + ``sha256`` + ``size`` alone
(:meth:`Catalog.fetch_locked`).
"""

import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional, Union

import yaml

from . import _fs

LOCK_VERSION = 1


class CatalogError(Exception):
    pass


class ChecksumError(CatalogError):
    pass


class RetiredError(CatalogError):
    """The requested dataset version was retired to cold storage (``irv:retired``)."""


class _Absent:
    """Filter value matching a property that is absent or null."""

    def __repr__(self):
        return "Catalog.ABSENT"

    def __copy__(self):
        return self

    def __deepcopy__(self, memo):
        return self


ABSENT = _Absent()


def _options(wanted) -> list:
    return list(wanted) if isinstance(wanted, (list, tuple, set, frozenset)) else [wanted]


def matches(properties: dict, filters: dict) -> bool:
    """True if ``properties`` match every filter.

    A filter value is a scalar or a list of accepted values, compared as
    strings (``100`` matches ``"100"``). ``None`` or :data:`ABSENT` among the
    accepted values matches a property that is absent or null.
    """
    for key, wanted in filters.items():
        options = _options(wanted)
        value = properties.get(key)
        if value is None:
            if any(o is None or o is ABSENT for o in options):
                continue
            return False
        if str(value) not in {str(o) for o in options if o is not None and o is not ABSENT}:
            return False
    return True


def _resolve_location(location: Optional[str], lock_dir: str) -> Optional[str]:
    """A base recorded in a lock file: relative local paths are relative to the lock file."""
    if location is None:
        return None
    location = str(location)
    if _fs.is_relative_local(location):
        return os.path.normpath(os.path.join(lock_dir, location))
    return location


@dataclass
class LockDiff:
    """Result of :meth:`Catalog.lock_matches`: truthy when the lock is up to date."""

    problems: list = field(default_factory=list)

    def __bool__(self) -> bool:
        return not self.problems

    @property
    def matches(self) -> bool:
        return not self.problems

    def __str__(self) -> str:
        return "lock is up to date" if not self.problems else "; ".join(self.problems)


class LockFile:
    """A parsed ``datapkg.lock.yaml``.

    ``base`` and ``private_base`` are resolved: a relative local path in the
    file is taken relative to the lock file's directory. ``raw_base`` is the
    value as written.
    """

    def __init__(self, data: dict, path: Optional[Union[str, Path]] = None):
        if not isinstance(data, dict) or data.get("lock_version") != LOCK_VERSION:
            raise CatalogError(f"Unsupported lock_version in {path or 'lock file'}")
        self.data = data
        self.path = Path(path) if path is not None else None
        lock_dir = str(self.path.parent.resolve()) if self.path is not None else os.getcwd()
        self.raw_base = str(data["base"])
        self.base = _resolve_location(self.raw_base, lock_dir)
        self.private_base = _resolve_location(data.get("private_base"), lock_dir)
        self.release = str(data["release"]) if data.get("release") is not None else None
        self.datasets = data.get("datasets") or {}

    @classmethod
    def load(cls, path: Union[str, Path]) -> "LockFile":
        with open(path) as f:
            return cls(yaml.safe_load(f), path)

    def _dataset(self, dataset: str) -> dict:
        if dataset not in self.datasets:
            where = self.path or "the lock file"
            raise CatalogError(f"Dataset {dataset} is not in {where}")
        return self.datasets[dataset]

    def pins(self) -> dict:
        return {
            ds: {"version": d["version"], "access": d["access"]} for ds, d in self.datasets.items()
        }

    def version(self, dataset: str) -> str:
        return self._dataset(dataset)["version"]

    def access(self, dataset: str) -> str:
        return self._dataset(dataset)["access"]

    def item_ids(self, dataset: str) -> list:
        return list((self._dataset(dataset).get("items") or {}).keys())

    def properties(self, dataset: str, item_id: str) -> Optional[dict]:
        """Dimension properties recorded for an item, or None if not recorded."""
        recorded = self._dataset(dataset).get("properties")
        if recorded is None:
            return None
        return recorded.get(item_id)

    def select(self, dataset: str, item_ids=(), **filters) -> list:
        """Locked item ids of ``dataset`` with the given ids and/or matching ``filters``
        on the recorded dimension properties (no catalogue reads)."""
        ids = self.item_ids(dataset)
        if item_ids:
            unknown = [i for i in item_ids if i not in ids]
            if unknown:
                raise CatalogError(f"{dataset}: items not in the lock file: {', '.join(unknown)}")
            ids = [i for i in ids if i in set(item_ids)]
        if filters:
            if self._dataset(dataset).get("properties") is None:
                raise CatalogError(
                    f"{dataset}: the lock file records no item properties; re-lock to "
                    "filter without reading the catalogue"
                )
            for i in ids:
                props = self.properties(dataset, i) or {}
                missing = [k for k in filters if k not in props]
                if missing:
                    raise CatalogError(
                        f"{dataset}: filter keys {missing} are not recorded in the lock file "
                        f"(recorded: {sorted(props)})"
                    )
            ids = [i for i in ids if matches(self.properties(dataset, i) or {}, filters)]
        return ids

    def entry(self, dataset: str, item_id: str, key: Optional[str] = None) -> tuple:
        """``(asset key, {href, sha256, size, ...})`` for one locked asset. Without
        ``key``, the ``data`` asset, or the only asset."""
        items = self._dataset(dataset).get("items") or {}
        if item_id not in items:
            raise CatalogError(f"Item {item_id} of {dataset} is not in the lock file")
        assets = items[item_id]
        if key is None:
            if "data" in assets:
                key = "data"
            elif len(assets) == 1:
                key = next(iter(assets))
            else:
                raise CatalogError(f"{dataset}/{item_id}: give the asset key ({sorted(assets)})")
        if key not in assets:
            raise CatalogError(f"{dataset}/{item_id} has no locked asset {key!r}")
        return key, assets[key]

    def write(self, path: Union[str, Path]) -> Path:
        with open(path, "w") as f:
            yaml.safe_dump(self.data, f, sort_keys=False)
        return Path(path)
