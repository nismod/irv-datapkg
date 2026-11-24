"""URL helpers shared by the catalogue client and producer.

Locations are plain strings: either an absolute local path or an fsspec URL
with a protocol (``s3://bucket/key``, ``https://host/path``). ``file://`` URLs
are normalised to local paths.
"""

import hashlib
import json
import os
import posixpath
from typing import Any, Optional

import fsspec

CHUNK = 1 << 20


def normalise(location: str) -> str:
    """Normalise a base or file location: strip ``file://``, make local paths absolute."""
    location = str(location)
    if location.startswith("file://"):
        location = location[len("file://") :]
    if "://" not in location:
        return os.path.abspath(location)
    return location.rstrip("/") if not location.endswith("://") else location


def split(location: str) -> tuple:
    if "://" in location:
        protocol, path = location.split("://", 1)
        return protocol + "://", path
    return "", location


def join(location: str, *parts: str) -> str:
    """Join path parts onto a location, resolving ``.`` and ``..``."""
    prefix, path = split(location)
    joined = posixpath.normpath(posixpath.join(path, *parts))
    return prefix + joined


def resolve(href: str, relative_to: str) -> str:
    """Resolve an href found in the JSON document at ``relative_to``."""
    if "://" in href or href.startswith("/"):
        return normalise(href)
    prefix, path = split(relative_to)
    return prefix + posixpath.normpath(posixpath.join(posixpath.dirname(path), href))


def relative(target: str, start_doc: str) -> str:
    """Relative href from the document at ``start_doc`` to ``target`` (same root)."""
    _, t = split(target)
    _, s = split(start_doc)
    rel = posixpath.relpath(t, posixpath.dirname(s))
    return rel if rel.startswith(".") else "./" + rel


def private_base(base: str) -> str:
    """``{BASE}-private`` for a base location (ADR 0002)."""
    return normalise(base).rstrip("/") + "-private"


def gdal_path(location: str) -> str:
    """Convert a location to a GDAL virtual file system path for windowed reads."""
    prefix, path = split(normalise(location))
    if prefix == "":
        return path
    if prefix == "s3://":
        return "/vsis3/" + path
    if prefix in ("gs://", "gcs://"):
        return "/vsigs/" + path
    if prefix in ("az://", "abfs://"):
        return "/vsiaz/" + path
    if prefix in ("http://", "https://"):
        return "/vsicurl/" + location
    raise ValueError(f"No GDAL virtual file system for {location}")


def fs_for(location: str, storage_options: Optional[dict] = None):
    fs, path = fsspec.core.url_to_fs(location, **(storage_options or {}))
    return fs, path


def read_json(location: str, storage_options: Optional[dict] = None) -> Any:
    with fsspec.open(location, "r", **(storage_options or {})) as f:
        return json.load(f)


def write_json(location: str, data: Any, storage_options: Optional[dict] = None) -> None:
    fs, path = fs_for(location, storage_options)
    parent = posixpath.dirname(path)
    if parent:
        fs.makedirs(parent, exist_ok=True)
    with fs.open(path, "w") as f:
        json.dump(data, f, indent=2)
        f.write("\n")


def exists(location: str, storage_options: Optional[dict] = None) -> bool:
    fs, path = fs_for(location, storage_options)
    return fs.exists(path)


def sha256_file(location: str, storage_options: Optional[dict] = None) -> str:
    h = hashlib.sha256()
    with fsspec.open(location, "rb", **(storage_options or {})) as f:
        for chunk in iter(lambda: f.read(CHUNK), b""):
            h.update(chunk)
    return h.hexdigest()


# STAC file extension v2 stores checksums as multihash: 0x12 (sha2-256), 0x20 (32 bytes)
MULTIHASH_SHA256 = "1220"


def to_multihash(sha256_hex: str) -> str:
    return MULTIHASH_SHA256 + sha256_hex


def from_multihash(checksum: str) -> str:
    if not checksum.startswith(MULTIHASH_SHA256) or len(checksum) != 68:
        raise ValueError(f"Not a sha2-256 multihash: {checksum}")
    return checksum[len(MULTIHASH_SHA256) :]
