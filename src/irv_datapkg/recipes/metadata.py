"""Dataset metadata (``metadata/*.yml``): one file per T2 package resource.

Each file generates both the T2 ``datapackage.json`` resource (top-level
``data_*`` fields, unchanged) and one or more STAC collections
(``collections:``), plus the T2 extract settings (``t2:``). See
``metadata/irv-datapkg-schema.json`` and docs/recipes.md.
"""

import csv
import json
from dataclasses import dataclass, field
from glob import glob
from pathlib import Path
from typing import Optional, Union

import yaml

PathLike = Union[str, Path]

T2_DEFAULTS = {
    "enabled": True,
    "stem": "{item_id}",
    "layer": None,
    "vector_format": "gpkg",
    "clip": "bbox",
    "filter": {},
    "items": None,
    "exclude_patterns": [],
    "summary": False,
    "summary_columns": None,
    "baseline_epoch": None,
}


@dataclass
class CollectionMeta:
    """Everything needed to build one STAC collection and its item paths."""

    id: str
    version: str
    package: str
    title: str
    description: str
    license: str
    citation: str
    upstream_url: str
    upstream_version: str
    access: str = "public"
    processing: list = field(default_factory=list)
    dimensions: list = field(default_factory=list)
    doi: Optional[str] = None
    keywords: list = field(default_factory=list)
    units: Optional[str] = None
    nodata: Optional[float] = None
    license_url: Optional[str] = None

    @property
    def upstream_dir(self) -> str:
        """Directory name for the T0 raw mirror: the version without ``-rN``."""
        return self.version.rsplit("-r", 1)[0]

    def stac(self) -> dict:
        """The STAC collection document (no items or versions yet)."""
        from ..catalog import build_collection

        doc = build_collection(
            self.id,
            title=self.title,
            description=self.description,
            license=self.license,
            citation=self.citation,
            upstream_url=self.upstream_url,
            upstream_version=self.upstream_version,
            access=self.access,
            processing=self.processing,
            dimensions=self.dimensions,
            doi=self.doi,
            keywords=self.keywords,
        )
        if self.license_url:
            doc["links"].append({"rel": "license", "href": self.license_url, "type": "text/html"})
        return doc


@dataclass
class PackageMeta:
    """One ``metadata/*.yml`` file."""

    name: str
    raw: dict
    collections: list
    t2: dict
    path: Optional[Path] = None

    @property
    def in_t2(self) -> bool:
        return bool(self.t2.get("enabled"))


def _collection(entry: dict, raw: dict) -> CollectionMeta:
    licence = raw.get("data_license", {})
    return CollectionMeta(
        id=entry["id"],
        version=entry["version"],
        package=raw["name"],
        title=entry.get("title") or raw.get("data_title_long") or raw["data_title"],
        description=(entry.get("description") or raw["data_summary"]).strip(),
        license=entry["license"],
        citation=(entry.get("citation") or raw["data_citation"]).strip(),
        upstream_url=entry["upstream_url"],
        upstream_version=str(entry["upstream_version"]),
        access=entry.get("access", "public"),
        processing=list(entry.get("processing", [])),
        dimensions=list(entry.get("dimensions", [])),
        doi=entry.get("doi"),
        keywords=list(entry.get("keywords", [])),
        units=entry.get("units"),
        nodata=entry.get("nodata"),
        license_url=entry.get("license_url") or licence.get("path"),
    )


def read_package(path: PathLike) -> PackageMeta:
    path = Path(path)
    with open(path) as fh:
        raw = yaml.safe_load(fh)
    collections = [_collection(c, raw) for c in raw.get("collections", [])]
    t2 = dict(T2_DEFAULTS, **(raw.get("t2") or {}))
    return PackageMeta(raw["name"], raw, collections, t2, path)


def load_metadata(metadata_dir: PathLike = "metadata") -> dict:
    """All packages in ``metadata_dir``, by package name (sorted)."""
    packages = {}
    for fname in sorted(glob(str(Path(metadata_dir) / "*.yml"))):
        pkg = read_package(fname)
        if pkg.name in packages:
            raise ValueError(f"Duplicate package name {pkg.name} in {fname}")
        packages[pkg.name] = pkg
    return packages


def collections_by_id(packages: dict) -> dict:
    out = {}
    for pkg in packages.values():
        for coll in pkg.collections:
            if coll.id in out:
                raise ValueError(f"Collection {coll.id} defined twice")
            out[coll.id] = coll
    return out


def validate_metadata(
    metadata_dir: PathLike = "metadata", datasets_csv: Optional[PathLike] = None
) -> list:
    """Schema errors in every ``*.yml``, plus collection ids missing from docs/datasets.csv."""
    import jsonschema

    metadata_dir = Path(metadata_dir)
    with open(metadata_dir / "irv-datapkg-schema.json") as fh:
        schema = json.load(fh)
    validator = jsonschema.Draft202012Validator(schema)
    errors = []
    known = None
    if datasets_csv is not None:
        with open(datasets_csv) as fh:
            known = {row["id"]: row for row in csv.DictReader(fh)}
    for fname in sorted(metadata_dir.glob("*.yml")):
        with open(fname) as fh:
            doc = yaml.safe_load(fh)
        for e in validator.iter_errors(doc):
            loc = "/".join(str(p) for p in e.absolute_path) or "<root>"
            errors.append(f"{fname.name}: {loc}: {e.message}")
        for coll in doc.get("collections", []):
            if known is not None and coll.get("id") not in known:
                errors.append(f"{fname.name}: collection {coll.get('id')} not in datasets.csv")
            elif known is not None and coll.get("access", "public") != known[coll["id"]]["access"]:
                errors.append(f"{fname.name}: access of {coll['id']} differs from datasets.csv")
    return errors
