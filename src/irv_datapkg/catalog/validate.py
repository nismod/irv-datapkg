"""Validate a catalogue (or lock file) against docs/catalogue-spec.md."""

import json
import posixpath
from functools import lru_cache
from importlib import resources
from typing import Optional

import jsonschema

from . import _fs


@lru_cache(maxsize=None)
def schema() -> dict:
    text = resources.files(__package__).joinpath("irv-catalogue-schema.json").read_text()
    return json.loads(text)


def check(doc: dict, kind: str) -> list:
    """Schema errors for ``doc`` validated as ``kind`` (collection, item, release, lock)."""
    full = schema()
    validator = jsonschema.Draft202012Validator({"$ref": f"#/$defs/{kind}", "$defs": full["$defs"]})
    return [
        f"{'/'.join(str(p) for p in e.absolute_path) or '<root>'}: {e.message}"
        for e in sorted(validator.iter_errors(doc), key=lambda e: list(e.absolute_path))
    ]


def _dirname(url: str) -> str:
    return posixpath.basename(posixpath.dirname(_fs.split(url)[1]))


def validate_catalog(
    base: str,
    private_base: Optional[str] = None,
    check_files: bool = False,
    storage_options: Optional[dict] = None,
) -> list:
    """Walk ``{BASE}`` (and ``{BASE}-private`` if present) and return a list of
    problems; empty means valid. ``check_files`` also verifies sha256 of every
    asset (reads all data)."""
    so = storage_options or {}
    base = _fs.normalise(base)
    roots = {
        "public": base,
        "private": _fs.normalise(private_base) if private_base else _fs.private_base(base),
    }
    errors, collections = [], {}

    def err(where, msg):
        errors.append(f"{where}: {msg}")

    for access, root in roots.items():
        root_url = _fs.join(root, "catalog.json")
        if not _fs.exists(root_url, so):
            if access == "public":
                err(root_url, "missing root catalog")
            continue
        root_doc = _fs.read_json(root_url, so)
        for link in root_doc.get("links", []):
            if link["rel"] != "child":
                continue
            coll_url = _fs.resolve(link["href"], root_url)
            if not _fs.exists(coll_url, so):
                err(coll_url, "linked collection missing")
                continue
            coll = _fs.read_json(coll_url, so)
            errors.extend(f"{coll_url}: {e}" for e in check(coll, "collection"))
            if coll.get("id") != _dirname(coll_url):
                err(coll_url, "collection id does not match its directory")
            if coll.get("irv:access") != access:
                err(
                    coll_url,
                    f"irv:access is {coll.get('irv:access')!r} but it lives under the {access} root",
                )
            collections[coll.get("id")] = coll
            for ilink in coll.get("links", []):
                if ilink["rel"] == "item":
                    _validate_item(
                        coll, _fs.resolve(ilink["href"], coll_url), check_files, so, err, errors
                    )

    public_root = _fs.join(base, "catalog.json")
    release_links = (
        _fs.read_json(public_root, so).get("links", []) if _fs.exists(public_root, so) else []
    )
    for link in release_links:
        if link["rel"] != "irv:release":
            continue
        rel_url = _fs.resolve(link["href"], public_root)
        if not _fs.exists(rel_url, so):
            err(rel_url, "linked release missing")
            continue
        manifest = _fs.read_json(rel_url, so)
        errors.extend(f"{rel_url}: {e}" for e in check(manifest, "release"))
        if manifest.get("irv:release") != _dirname(rel_url):
            err(rel_url, "irv:release does not match its directory")
        for dataset, pin in manifest.get("irv:datasets", {}).items():
            coll = collections.get(dataset)
            if coll is None:
                err(rel_url, f"pins unknown dataset {dataset}")
            elif pin.get("version") not in coll.get("irv:versions", []):
                err(rel_url, f"pins {dataset} {pin.get('version')}, which is not published")
            elif pin.get("access") != coll.get("irv:access"):
                err(rel_url, f"access of {dataset} does not match its collection")
    return errors


def _validate_item(coll, item_url, check_files, so, err, errors):
    if not _fs.exists(item_url, so):
        err(item_url, "linked item missing")
        return
    item = _fs.read_json(item_url, so)
    errors.extend(f"{item_url}: {e}" for e in check(item, "item"))
    props = item.get("properties", {})
    version = props.get("irv:version")
    if item.get("collection") != coll.get("id") or props.get("irv:dataset") != coll.get("id"):
        err(item_url, "item collection/irv:dataset does not match its collection")
    if version != _dirname(item_url):
        err(item_url, "irv:version does not match its directory")
    if version not in coll.get("irv:versions", []):
        err(item_url, f"version {version} not listed in collection irv:versions")
    for dim in coll.get("irv:dimensions", []):
        if dim not in props:
            err(item_url, f"missing dimension property {dim!r}")
    for key, asset in item.get("assets", {}).items():
        href = _fs.resolve(asset.get("href", ""), item_url)
        if not _fs.exists(href, so):
            err(href, f"asset {key} file missing")
            continue
        fs, path = _fs.fs_for(href, so)
        if fs.size(path) != asset.get("file:size"):
            err(href, "file:size does not match")
        if check_files and _fs.to_multihash(_fs.sha256_file(href, so)) != asset.get(
            "file:checksum"
        ):
            err(href, "file:checksum does not match")
