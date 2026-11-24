"""Publish a local catalogue to remote storage, and report versions to retire.

Implements docs/storage.md (ADR 0002, 0003). ``publish`` copies a local
``{BASE}`` (and ``{BASE}-private``) to remote roots with fsspec, in an order
that never leaves a dangling link for readers of the remote:

1. data files (``.tif``, ``.parquet``, zarr chunks, ...)
2. item JSON (``{dataset}/{version}/{item}.json``)
3. collection JSON (``{dataset}/collection.json``)
4. release manifests (``releases/YYYY.N/catalog.json``)
5. root ``catalog.json``

Each tier is done for the private root and then the public root before the
next tier starts, so a public release never points at a private collection
that has not arrived yet.

Everything except collection JSON and root catalogues is immutable. A file
already on the remote with the same size and sha256 is skipped; an immutable
file with different content is a conflict, and ``publish`` refuses to write
anything if there is any conflict. Nothing is ever deleted from the remote.
"""

import csv
import datetime as dt
import fnmatch
import json
import posixpath
from dataclasses import asdict, dataclass, field
from typing import Iterable, List, Optional

from fsspec.implementations.local import LocalFileSystem

from .catalog import _fs
from .catalog.producer import ImmutableError

DATA, ITEM, COLLECTION, RELEASE, ROOT = range(5)
TIER_NAMES = {
    DATA: "data",
    ITEM: "item",
    COLLECTION: "collection",
    RELEASE: "release",
    ROOT: "root",
}
MUTABLE_TIERS = (COLLECTION, ROOT)
DEFAULT_EXCLUDE = (".DS_Store", "*.tmp", "*.part", ".snakemake*", "*~", "Thumbs.db")


class PublishError(Exception):
    """Raised when a publish would break the storage rules (conflicts, access mix-up)."""


@dataclass
class Action:
    access: str  # "public" or "private"
    path: str  # path relative to the root
    tier: int
    action: str  # "upload" (new), "update" (mutable, changed), "skip" (identical), "conflict"
    size: int
    reason: str = ""

    @property
    def tier_name(self):
        return TIER_NAMES[self.tier]


@dataclass
class Summary:
    dry_run: bool
    roots: dict = field(default_factory=dict)  # access -> (local, remote)
    actions: List[Action] = field(default_factory=list)

    def count(self, action: str) -> int:
        return sum(1 for a in self.actions if a.action == action)

    def bytes(self, action: str) -> int:
        return sum(a.size for a in self.actions if a.action == action)

    @property
    def conflicts(self) -> List[Action]:
        return [a for a in self.actions if a.action == "conflict"]

    def to_dict(self) -> dict:
        return {
            "dry_run": self.dry_run,
            "roots": {k: {"local": v[0], "remote": v[1]} for k, v in self.roots.items()},
            "counts": {a: self.count(a) for a in ("upload", "update", "skip", "conflict")},
            "bytes": {a: self.bytes(a) for a in ("upload", "update")},
            "actions": [
                {**asdict(a), "tier": a.tier_name} for a in self.actions if a.action != "skip"
            ],
        }

    def format(self, verbose: bool = False) -> str:
        verb = "Would" if self.dry_run else "Did"
        lines = []
        for access, (local, remote) in self.roots.items():
            lines.append(f"{access}: {local} -> {remote}")
        for a in self.actions:
            if a.action != "skip" or verbose:
                extra = f" ({a.reason})" if a.reason else ""
                lines.append(f"  {a.action:8} {a.access:7} {a.tier_name:10} {a.path}{extra}")
        lines.append(
            f"{verb} upload {self.count('upload')} new ({_human(self.bytes('upload'))}), "
            f"update {self.count('update')} mutable ({_human(self.bytes('update'))}), "
            f"skip {self.count('skip')} identical; {self.count('conflict')} conflicts"
        )
        return "\n".join(lines)


def _human(n: float) -> str:
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if n < 1000 or unit == "TB":
            return f"{n:.0f} {unit}" if unit == "B" else f"{n:.1f} {unit}"
        n /= 1000


def classify(relpath: str) -> int:
    """Publishing tier of a path relative to a catalogue root."""
    parts = relpath.split("/")
    if relpath == "catalog.json":
        return ROOT
    if len(parts) == 3 and parts[0] == "releases" and parts[2] == "catalog.json":
        return RELEASE
    if len(parts) == 2 and parts[1] == "collection.json":
        return COLLECTION
    if len(parts) == 3 and parts[2].endswith(".json") and parts[0] not in _RESERVED:
        return ITEM
    return DATA


# Top-level directories whose JSON is not STAC items (ADR 0003): treat it as data
_RESERVED = {"releases", "partitions", "extracts", "results"}


def _excluded(relpath: str, exclude: Iterable[str]) -> bool:
    name = posixpath.basename(relpath)
    return any(fnmatch.fnmatch(name, pat) for pat in exclude)


def _list(root: str, storage_options: Optional[dict]) -> dict:
    """{relative path: size} of every file under ``root`` (empty if it does not exist)."""
    fs, path = _fs.fs_for(root, storage_options)
    if not fs.exists(path):
        return {}
    path = path.rstrip("/")
    out = {}
    for name, info in fs.find(path, detail=True).items():
        if info.get("type", "file") == "directory":
            continue
        name = name.rstrip("/")
        if not name.startswith(path + "/"):
            continue
        out[name[len(path) + 1 :]] = int(info.get("size") or 0)
    return out


def _sha256(location: str, storage_options: Optional[dict]) -> str:
    return _fs.sha256_file(location, storage_options)


def _check_access(local: str, access: str, storage_options: Optional[dict], files: dict):
    """Refuse a root holding collections whose irv:access does not match it."""
    for rel in files:
        if classify(rel) == COLLECTION:
            coll = _fs.read_json(_fs.join(local, rel), storage_options)
            if coll.get("irv:access") != access:
                raise PublishError(
                    f"{_fs.join(local, rel)} has irv:access={coll.get('irv:access')!r} "
                    f"but is under the {access} root"
                )


def _item_checksums(local: str, files: dict, storage_options) -> dict:
    """{data relpath: (item relpath, sha256 hex)} from the local item JSON."""
    out = {}
    for rel in files:
        if classify(rel) != ITEM:
            continue
        item = _fs.read_json(_fs.join(local, rel), storage_options)
        for asset in item.get("assets", {}).values():
            href = asset.get("href", "")
            checksum = asset.get("file:checksum")
            if "://" in href or href.startswith("/") or not checksum:
                continue
            data_rel = posixpath.normpath(posixpath.join(posixpath.dirname(rel), href))
            try:
                out[data_rel] = (rel, _fs.from_multihash(checksum))
            except ValueError:
                continue
    return out


def plan_root(
    local: str,
    remote: str,
    access: str,
    local_options: Optional[dict] = None,
    remote_options: Optional[dict] = None,
    exclude: Iterable[str] = DEFAULT_EXCLUDE,
    verify_remote: bool = False,
) -> List[Action]:
    """Compare one local root with its remote and return the actions to take."""
    local_files = {
        rel: size
        for rel, size in _list(local, local_options).items()
        if not _excluded(rel, exclude)
    }
    _check_access(local, access, local_options, local_files)
    remote_files = _list(remote, remote_options)
    checksums = _item_checksums(local, local_files, local_options)
    local_hash, actions = {}, []

    def lhash(rel):
        if rel not in local_hash:
            local_hash[rel] = _sha256(_fs.join(local, rel), local_options)
        return local_hash[rel]

    def same_bytes(rel):
        if not verify_remote and rel in checksums:
            # Trust the remote item JSON as a commit marker: items are uploaded
            # only after their data, so an identical remote item means this
            # data file was uploaded completely with the recorded checksum.
            item_rel, declared = checksums[rel]
            if (
                item_rel in remote_files
                and remote_files[item_rel] == local_files[item_rel]
                and lhash(item_rel) == _sha256(_fs.join(remote, item_rel), remote_options)
                and lhash(rel) == declared
            ):
                return True
        return lhash(rel) == _sha256(_fs.join(remote, rel), remote_options)

    for rel in sorted(local_files, key=lambda r: (classify(r), r)):
        tier, size = classify(rel), local_files[rel]
        mutable = tier in MUTABLE_TIERS
        if rel not in remote_files:
            actions.append(Action(access, rel, tier, "upload", size))
        elif remote_files[rel] == size and same_bytes(rel):
            actions.append(Action(access, rel, tier, "skip", size))
        elif mutable:
            actions.append(Action(access, rel, tier, "update", size))
        else:
            reason = (
                f"remote size {remote_files[rel]} != local {size}"
                if remote_files[rel] != size
                else "same size, different sha256"
            )
            actions.append(Action(access, rel, tier, "conflict", size, reason))
    return actions


def _same_or_nested(a: str, b: str) -> bool:
    a, b = a.rstrip("/") + "/", b.rstrip("/") + "/"
    return a.startswith(b) or b.startswith(a)


def publish(
    local_base: str,
    remote_base: str,
    local_private: Optional[str] = None,
    remote_private: Optional[str] = None,
    include_private: bool = True,
    dry_run: bool = False,
    remote_options: Optional[dict] = None,
    local_options: Optional[dict] = None,
    exclude: Iterable[str] = DEFAULT_EXCLUDE,
    verify_remote: bool = False,
    log=None,
) -> Summary:
    """Sync local public (and private) roots to their remotes.

    ``local_private``/``remote_private`` default to ``{BASE}-private`` of the
    local and remote bases. With ``include_private=False`` the private root is
    not touched. Raises :class:`PublishError` (before writing anything) on
    conflicts unless ``dry_run``, in which case conflicts are only reported.
    """
    local_base, remote_base = _fs.normalise(local_base), _fs.normalise(remote_base)
    roots = {}
    if include_private:
        lp = _fs.normalise(local_private) if local_private else _fs.private_base(local_base)
        rp = _fs.normalise(remote_private) if remote_private else _fs.private_base(remote_base)
        if _same_or_nested(rp, remote_base):
            raise PublishError(f"private remote {rp} overlaps public remote {remote_base}")
        if _same_or_nested(lp, local_base):
            raise PublishError(f"private root {lp} overlaps public root {local_base}")
        if _fs.exists(lp, local_options):
            roots["private"] = (lp, rp)
    roots["public"] = (local_base, remote_base)
    if not _fs.exists(_fs.join(local_base, "catalog.json"), local_options):
        raise PublishError(f"No root catalog.json under {local_base}")

    summary = Summary(dry_run=dry_run, roots=roots)
    for access, (local, remote) in roots.items():
        summary.actions.extend(
            plan_root(local, remote, access, local_options, remote_options, exclude, verify_remote)
        )
    # tier-major, private before public within a tier
    order = {"private": 0, "public": 1}
    summary.actions.sort(key=lambda a: (a.tier, order[a.access], a.path))

    if summary.conflicts and not dry_run:
        details = "\n".join(f"  {a.access} {a.path}: {a.reason}" for a in summary.conflicts)
        raise ImmutableError(
            f"Refusing to publish: {len(summary.conflicts)} immutable file(s) differ "
            f"on the remote; publish a new version instead.\n{details}"
        )
    if dry_run:
        return summary

    for a in summary.actions:
        if a.action not in ("upload", "update"):
            continue
        local, remote = roots[a.access]
        src, dest = _fs.join(local, a.path), _fs.join(remote, a.path)
        _copy(src, dest, local_options, remote_options)
        if log:
            log(f"{a.action} {dest}")
    return summary


def _copy(src: str, dest: str, local_options, remote_options):
    rfs, rpath = _fs.fs_for(dest, remote_options)
    parent = posixpath.dirname(rpath)
    if parent:
        rfs.makedirs(parent, exist_ok=True)
    lfs, lpath = _fs.fs_for(src, local_options)
    if isinstance(lfs, LocalFileSystem):
        rfs.put_file(lpath, rpath)
    else:
        with lfs.open(lpath, "rb") as fin, rfs.open(rpath, "wb") as fout:
            for chunk in iter(lambda: fin.read(_fs.CHUNK), b""):
                fout.write(chunk)
    size = rfs.size(rpath)
    if size != lfs.size(lpath):
        raise PublishError(f"Upload of {dest} is incomplete ({size} bytes)")


# --- retirement report --------------------------------------------------------------

RETIRE_FIELDS = [
    "dataset",
    "access",
    "version",
    "latest",
    "pinned_by",
    "last_pinned",
    "newest_retrieved",
    "files",
    "bytes",
    "eligible",
    "reason",
]


def _parse_date(value: Optional[str]) -> Optional[dt.date]:
    if not value:
        return None
    try:
        return dt.date.fromisoformat(str(value)[:10])
    except ValueError:
        return None


def retire_report(
    base: str,
    private_base: Optional[str] = None,
    now: Optional[dt.date] = None,
    keep_days: int = 365,
    storage_options: Optional[dict] = None,
) -> List[dict]:
    """One row per dataset version saying whether it may move to cold storage.

    A version is eligible when all of these hold:

    - it is not the latest version of its collection;
    - it is not pinned by the most recent release (whatever that release's age);
    - it is not pinned by any release created within ``keep_days`` of ``now``;
    - its newest asset ``irv:retrieved`` date is more than ``keep_days`` ago
      (a grace period for versions superseded before any release pinned them).
    """
    so = storage_options or {}
    now = now or dt.date.today()
    cutoff = now - dt.timedelta(days=keep_days)
    base = _fs.normalise(base)
    roots = {
        "public": base,
        "private": _fs.normalise(private_base) if private_base else _fs.private_base(base),
    }

    releases = []  # (name, created date, {dataset: version})
    public_root = _fs.join(base, "catalog.json")
    for link in _fs.read_json(public_root, so).get("links", []):
        if link["rel"] == "irv:release":
            manifest = _fs.read_json(_fs.resolve(link["href"], public_root), so)
            pins = {ds: p["version"] for ds, p in manifest.get("irv:datasets", {}).items()}
            releases.append(
                (manifest.get("irv:release"), _parse_date(manifest.get("irv:created")), pins)
            )
    releases.sort(key=lambda r: (r[1] or dt.date.min, r[0]))
    newest = releases[-1] if releases else None

    rows = []
    for access, root in roots.items():
        root_url = _fs.join(root, "catalog.json")
        if not _fs.exists(root_url, so):
            continue
        for link in _fs.read_json(root_url, so).get("links", []):
            if link["rel"] != "child":
                continue
            coll_url = _fs.resolve(link["href"], root_url)
            coll = _fs.read_json(coll_url, so)
            versions = coll.get("irv:versions", [])
            stats = {v: {"files": 0, "bytes": 0, "retrieved": None} for v in versions}
            for ilink in coll.get("links", []):
                if ilink["rel"] != "item":
                    continue
                item_url = _fs.resolve(ilink["href"], coll_url)
                item = _fs.read_json(item_url, so)
                v = item.get("properties", {}).get("irv:version")
                s = stats.setdefault(v, {"files": 0, "bytes": 0, "retrieved": None})
                for asset in item.get("assets", {}).values():
                    s["files"] += 1
                    s["bytes"] += int(asset.get("file:size") or 0)
                    r = _parse_date(asset.get("irv:retrieved"))
                    if r and (s["retrieved"] is None or r > s["retrieved"]):
                        s["retrieved"] = r
            for v in versions:
                pinned = [r for r in releases if r[2].get(coll["id"]) == v]
                last = max((r[1] for r in pinned if r[1]), default=None)
                s = stats[v]
                if versions and v == versions[-1]:
                    eligible, reason = False, "latest version"
                elif newest and newest[2].get(coll["id"]) == v:
                    eligible, reason = False, f"pinned by most recent release {newest[0]}"
                elif last and last > cutoff:
                    eligible, reason = False, f"pinned by a release within {keep_days} days"
                elif s["retrieved"] and s["retrieved"] > cutoff:
                    eligible, reason = False, f"retrieved within {keep_days} days"
                else:
                    eligible = True
                    reason = (
                        f"last pinned {last.isoformat()}" if last else "never pinned by a release"
                    )
                rows.append(
                    {
                        "dataset": coll["id"],
                        "access": access,
                        "version": v,
                        "latest": v == versions[-1],
                        "pinned_by": ";".join(r[0] for r in pinned),
                        "last_pinned": last.isoformat() if last else "",
                        "newest_retrieved": s["retrieved"].isoformat() if s["retrieved"] else "",
                        "files": s["files"],
                        "bytes": s["bytes"],
                        "eligible": eligible,
                        "reason": reason,
                    }
                )
    return rows


def write_csv(rows: List[dict], path: str, fields: List[str]) -> str:
    with open(path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)
    return path


def write_summary(summary: Summary, path: str) -> str:
    with open(path, "w") as f:
        json.dump(summary.to_dict(), f, indent=2)
        f.write("\n")
    return path


__all__ = [
    "Action",
    "PublishError",
    "Summary",
    "classify",
    "plan_root",
    "publish",
    "retire_report",
    "write_csv",
    "write_summary",
]
