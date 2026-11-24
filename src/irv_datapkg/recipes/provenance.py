"""T0 provenance sidecars: what was downloaded, from where, when and with what.

Every T0 (raw mirror) download writes a JSON sidecar next to what it fetched:

- ``<file>.provenance.json`` for a single downloaded file, or
- ``<dir>/provenance.json`` for a bulk download into a directory
  (``aws s3 sync``, ``zenodo_get``, a list of tiles).

Sidecar format (``irv:provenance`` version 1)::

    {
      "irv:provenance": 1,
      "retrieved": "2026-11-24",
      "tool": "wget 1.21.4",
      "source": "optional free-text description of the upstream",
      "files": [
        {"path": "floodMapGL_rp10y.zip",          # relative to the sidecar
         "source_url": "https://.../floodMapGL_rp10y.zip",
         "sha256": "...", "size": 123}
      ]
    }

T1 registration reads ``source_url`` and ``retrieved`` from here, so the STAC
asset fields ``irv:source_url``/``irv:retrieved`` trace back to the raw mirror.
"""

import datetime as dt
import hashlib
import json
import os
import shutil
import subprocess
from pathlib import Path
from typing import Iterable, Optional, Union

PROVENANCE_VERSION = 1
SIDECAR_SUFFIX = ".provenance.json"
DIR_SIDECAR = "provenance.json"
CHUNK = 1 << 20

PathLike = Union[str, Path]


def sha256_file(path: PathLike) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(CHUNK), b""):
            h.update(chunk)
    return h.hexdigest()


def tool_version(tool: str) -> str:
    """``tool`` plus the first line of ``tool --version`` if it runs, else just ``tool``."""
    exe = shutil.which(tool)
    if not exe:
        return tool
    try:
        out = subprocess.run(
            [exe, "--version"], capture_output=True, text=True, timeout=30, check=False
        )
    except (OSError, subprocess.SubprocessError):
        return tool
    first = (out.stdout or out.stderr).strip().splitlines()
    return first[0].strip() if first else tool


def file_entry(path: PathLike, source_url: str, relative_to: PathLike) -> dict:
    path = Path(path)
    return {
        "path": os.path.relpath(path, relative_to),
        "source_url": source_url,
        "sha256": sha256_file(path),
        "size": path.stat().st_size,
    }


def write_provenance(
    sidecar: PathLike,
    files: Iterable[PathLike],
    *,
    tool: str,
    source_url: Optional[str] = None,
    url_prefix: Optional[str] = None,
    urls: Optional[dict] = None,
    retrieved: Union[str, dt.date, None] = None,
    source: Optional[str] = None,
) -> Path:
    """Write a provenance sidecar for ``files``.

    Each file's upstream URL is, in order of preference: ``urls[name]`` (by
    file name or path), ``url_prefix`` + path relative to the sidecar
    directory, or ``source_url`` (one URL for everything, e.g. an API request).
    ``retrieved`` defaults to today (UTC).
    """
    sidecar = Path(sidecar)
    base = sidecar.parent
    retrieved = retrieved or dt.datetime.now(dt.timezone.utc).date()
    if isinstance(retrieved, (dt.date, dt.datetime)):
        retrieved = retrieved.strftime("%Y-%m-%d")
    urls = urls or {}
    entries = []
    for f in sorted(Path(p) for p in files):
        rel = os.path.relpath(f, base)
        if str(f) in urls or f.name in urls or rel in urls:
            url = urls.get(str(f)) or urls.get(rel) or urls[f.name]
        elif url_prefix is not None:
            url = url_prefix.rstrip("/") + "/" + Path(rel).as_posix()
        elif source_url is not None:
            url = source_url
        else:
            raise ValueError(f"No source URL for {f}: give urls, url_prefix or source_url")
        entries.append(file_entry(f, url, base))
    if not entries:
        raise ValueError(f"No files to record in {sidecar}")
    doc = {
        "irv:provenance": PROVENANCE_VERSION,
        "retrieved": retrieved,
        "tool": tool,
        "files": entries,
    }
    if source:
        doc["source"] = source
    sidecar.parent.mkdir(parents=True, exist_ok=True)
    with open(sidecar, "w") as fh:
        json.dump(doc, fh, indent=2)
        fh.write("\n")
    return sidecar


def sidecar_for(path: PathLike) -> Path:
    """Sidecar path for a single downloaded file."""
    return Path(str(path) + SIDECAR_SUFFIX)


def read_provenance(sidecar: PathLike) -> dict:
    with open(sidecar) as fh:
        doc = json.load(fh)
    if doc.get("irv:provenance") != PROVENANCE_VERSION:
        raise ValueError(f"{sidecar} is not an irv provenance sidecar (version 1)")
    return doc


def find_provenance(path: PathLike) -> tuple:
    """Find the provenance of a raw file: ``(sidecar_path, file_entry, sidecar_doc)``.

    Looks for ``<path>.provenance.json``, then ``provenance.json`` in each
    parent directory (nearest first) listing the file.
    """
    path = Path(path)
    own = sidecar_for(path)
    candidates = [own] + [p / DIR_SIDECAR for p in path.absolute().parents]
    for sidecar in candidates:
        if not sidecar.exists():
            continue
        doc = read_provenance(sidecar)
        rel = os.path.relpath(path.absolute(), sidecar.parent.absolute())
        for entry in doc["files"]:
            if Path(entry["path"]).as_posix() == Path(rel).as_posix():
                return sidecar, entry, doc
        if sidecar == own and len(doc["files"]) == 1:
            return sidecar, doc["files"][0], doc
    raise FileNotFoundError(f"No provenance sidecar records {path}")


def verify_provenance(sidecar: PathLike) -> list:
    """Files in a sidecar whose size or sha256 no longer match (empty if all good)."""
    sidecar = Path(sidecar)
    doc = read_provenance(sidecar)
    bad = []
    for entry in doc["files"]:
        f = sidecar.parent / entry["path"]
        if not f.exists() or f.stat().st_size != entry["size"] or sha256_file(f) != entry["sha256"]:
            bad.append(entry["path"])
    return bad


def download(url: str, dest: PathLike, *, sidecar: bool = True, timeout: int = 600) -> Path:
    """Stream ``url`` to ``dest`` over HTTP(S) and write ``<dest>.provenance.json``."""
    import requests

    dest = Path(dest)
    dest.parent.mkdir(parents=True, exist_ok=True)
    tmp = dest.with_name(dest.name + ".part")
    with requests.get(url, stream=True, timeout=timeout) as r:
        r.raise_for_status()
        with open(tmp, "wb") as fh:
            for chunk in r.iter_content(chunk_size=CHUNK):
                fh.write(chunk)
    tmp.replace(dest)
    if sidecar:
        write_provenance(
            sidecar_for(dest),
            [dest],
            tool=f"python-requests {requests.__version__}",
            source_url=url,
        )
    return dest
