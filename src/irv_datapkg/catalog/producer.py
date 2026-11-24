"""Producer-side helpers: describe local files and write the static catalogue."""

import datetime as dt
import posixpath
import re
import subprocess
from pathlib import Path
from typing import Iterable, Optional, Sequence, Union


from . import _fs

STAC_VERSION = "1.0.0"
EXT = {
    "file": "https://stac-extensions.github.io/file/v2.1.0/schema.json",
    "projection": "https://stac-extensions.github.io/projection/v1.1.0/schema.json",
    "raster": "https://stac-extensions.github.io/raster/v1.1.0/schema.json",
    "table": "https://stac-extensions.github.io/table/v1.2.0/schema.json",
    "scientific": "https://stac-extensions.github.io/scientific/v1.0.0/schema.json",
}
MEDIA_TYPES = {
    ".tif": "image/tiff; application=geotiff; profile=cloud-optimized",
    ".tiff": "image/tiff; application=geotiff; profile=cloud-optimized",
    ".parquet": "application/vnd.apache.parquet",
    ".zarr": "application/vnd+zarr",
}
DATASET_RE = re.compile(r"^[a-z][a-z0-9_]*$")
VERSION_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]*-r[1-9][0-9]*$")
ITEM_RE = re.compile(r"^[a-z0-9][a-z0-9_.-]*$")
RELEASE_RE = re.compile(r"^[0-9]{4}\.[1-9][0-9]*$")


class ImmutableError(Exception):
    """Raised when writing would change already-published content."""


def _check(pattern, value, what):
    if not pattern.match(value):
        raise ValueError(f"Invalid {what} {value!r}: must match {pattern.pattern}")


def git_commit(path: Union[str, Path, None] = None) -> Optional[str]:
    """The irv-datapkg git commit (HEAD) of the checkout containing ``path``."""
    path = Path(path or Path(__file__).parent)
    try:
        out = subprocess.run(
            ["git", "rev-parse", "HEAD"], cwd=path, capture_output=True, text=True, check=True
        )
    except (OSError, subprocess.CalledProcessError):
        return None
    return out.stdout.strip()


def bbox_polygon(bbox: Sequence[float]) -> dict:
    w, s, e, n = bbox
    return {"type": "Polygon", "coordinates": [[[w, s], [e, s], [e, n], [w, n], [w, s]]]}


def describe_raster(path: Union[str, Path], units=None, nodata=None) -> dict:
    """Item-level (bbox, proj:*) and asset-level (raster:bands) metadata of a raster."""
    import rasterio
    from rasterio.warp import transform_bounds

    with rasterio.open(path) as src:
        bbox = list(transform_bounds(src.crs, "EPSG:4326", *src.bounds, densify_pts=21))
        epsg = src.crs.to_epsg()
        if isinstance(units, str) or units is None:
            units = [units] * src.count
        bands = []
        for i in range(src.count):
            band_nodata = nodata if nodata is not None else src.nodatavals[i]
            band = {
                "data_type": src.dtypes[i],
                "nodata": band_nodata,
                "unit": units[i] or src.units[i] or None,
                "scale": src.scales[i],
                "offset": src.offsets[i],
            }
            if src.descriptions[i]:
                band["description"] = src.descriptions[i]
            bands.append(band)
        proj = {
            "proj:epsg": epsg,
            "proj:shape": [src.height, src.width],
            "proj:transform": list(src.transform)[:6],
        }
        if epsg is None:
            proj["proj:wkt2"] = src.crs.to_wkt()
    return {"bbox": bbox, "proj": proj, "asset": {"raster:bands": bands}}


def describe_vector(path: Union[str, Path]) -> dict:
    """bbox and table:* metadata of a GeoParquet file."""
    import json

    import geopandas
    import pyarrow.parquet as pq

    pf = pq.ParquetFile(path)
    geo = json.loads(pf.metadata.metadata.get(b"geo", b"{}") or b"{}")
    primary = geo.get("primary_column", "geometry")
    bbox = geo.get("columns", {}).get(primary, {}).get("bbox")
    if not bbox:
        bbox = list(geopandas.read_parquet(path, columns=[primary]).total_bounds)
    columns = [{"name": f.name, "type": str(f.type)} for f in pf.schema_arrow]
    return {
        "bbox": [float(v) for v in bbox],
        "proj": {"proj:epsg": 4326},
        "asset": {
            "table:row_count": pf.metadata.num_rows,
            "table:columns": columns,
            "table:primary_geometry": primary,
        },
    }


def build_item(
    path: Union[str, Path],
    dataset: str,
    version: str,
    item_id: str,
    *,
    source_url: str,
    retrieved: Union[str, dt.date],
    properties: Optional[dict] = None,
    commit: Optional[str] = None,
    datetime: Optional[str] = None,
    units=None,
    nodata=None,
    title: Optional[str] = None,
    asset_key: str = "data",
) -> dict:
    """Build a STAC item dict for a local COG or GeoParquet file.

    ``properties`` holds the variant dimensions (hazard, rcp, epoch, gcm,
    return_period, ...). Computes size and sha256 of the file.
    """
    _check(DATASET_RE, dataset, "dataset id")
    _check(VERSION_RE, version, "version")
    _check(ITEM_RE, item_id, "item id")
    path = Path(path)
    suffix = path.suffix.lower()
    if suffix in (".tif", ".tiff"):
        meta, extensions = describe_raster(path, units=units, nodata=nodata), ["raster"]
    elif suffix == ".parquet":
        meta, extensions = describe_vector(path), ["table"]
    else:
        raise ValueError(f"Unsupported file type {suffix}: expected .tif or .parquet")
    commit = commit or git_commit()
    if not commit:
        raise ValueError("Could not determine the producer git commit; pass commit=")
    if isinstance(retrieved, (dt.date, dt.datetime)):
        retrieved = retrieved.strftime("%Y-%m-%d")

    props = {
        "datetime": datetime,
        "irv:dataset": dataset,
        "irv:version": version,
        **(properties or {}),
    }
    if datetime is None:
        # STAC requires a datetime: default to the retrieval date (scenario
        # epochs belong in the "epoch" property, not here)
        props["datetime"] = retrieved + "T00:00:00Z"
    asset = {
        "href": "./" + path.name,
        "type": MEDIA_TYPES[suffix],
        "roles": ["data"],
        "file:size": path.stat().st_size,
        "file:checksum": _fs.to_multihash(_fs.sha256_file(str(path))),
        "irv:source_url": source_url,
        "irv:retrieved": retrieved,
        "irv:commit": commit,
        **meta["asset"],
    }
    if title:
        asset["title"] = title
    return {
        "type": "Feature",
        "stac_version": STAC_VERSION,
        "stac_extensions": [EXT["file"], EXT["projection"]] + [EXT[e] for e in extensions],
        "id": item_id,
        "collection": dataset,
        "bbox": meta["bbox"],
        "geometry": bbox_polygon(meta["bbox"]),
        "properties": {**props, **meta["proj"]},
        "links": [],
        "assets": {asset_key: asset},
    }


def build_collection(
    dataset: str,
    *,
    title: str,
    description: str,
    license: str,
    citation: str,
    upstream_url: str,
    upstream_version: str,
    access: str = "public",
    processing: Iterable[str] = (),
    dimensions: Iterable[str] = (),
    doi: Optional[str] = None,
    keywords: Iterable[str] = (),
) -> dict:
    """Build a STAC collection dict for a dataset (all versions)."""
    _check(DATASET_RE, dataset, "dataset id")
    if access not in ("public", "private"):
        raise ValueError("access must be 'public' or 'private'")
    collection = {
        "type": "Collection",
        "stac_version": STAC_VERSION,
        "stac_extensions": [EXT["scientific"]],
        "id": dataset,
        "title": title,
        "description": description,
        "license": license,
        "keywords": list(keywords),
        "extent": {
            "spatial": {"bbox": [[-180.0, -90.0, 180.0, 90.0]]},
            "temporal": {"interval": [[None, None]]},
        },
        "sci:citation": citation,
        "irv:access": access,
        "irv:upstream_url": upstream_url,
        "irv:upstream_version": upstream_version,
        "irv:processing": list(processing),
        "irv:dimensions": list(dimensions),
        "irv:versions": [],
        "links": [],
    }
    if doi:
        collection["sci:doi"] = doi
    return collection


def _union(a, b):
    return [min(a[0], b[0]), min(a[1], b[1]), max(a[2], b[2]), max(a[3], b[3])]


class CatalogWriter:
    """Write collections, items, files and release manifests under ``{BASE}``
    (and ``{BASE}-private`` for private collections)."""

    def __init__(self, base, private_base=None, storage_options=None, title="irv-datapkg"):
        self.base = _fs.normalise(base)
        self.private_base = _fs.normalise(private_base) if private_base else _fs.private_base(base)
        self.storage_options = storage_options or {}
        self.title = title

    def _root(self, access):
        return self.private_base if access == "private" else self.base

    def _read(self, url):
        return _fs.read_json(url, self.storage_options)

    def _write(self, url, data):
        _fs.write_json(url, data, self.storage_options)

    def _exists(self, url):
        return _fs.exists(url, self.storage_options)

    def _ensure_root(self, access) -> str:
        url = _fs.join(self._root(access), "catalog.json")
        if not self._exists(url):
            suffix = " (private)" if access == "private" else ""
            self._write(
                url,
                {
                    "type": "Catalog",
                    "stac_version": STAC_VERSION,
                    "id": "irv-datapkg" + ("-private" if access == "private" else ""),
                    "title": self.title + suffix,
                    "description": "irv-datapkg analysis-ready global datasets" + suffix,
                    "links": [
                        {"rel": "root", "href": "./catalog.json", "type": "application/json"}
                    ],
                },
            )
        return url

    @staticmethod
    def _add_link(doc, link):
        if not any(lk["rel"] == link["rel"] and lk["href"] == link["href"] for lk in doc["links"]):
            doc["links"].append(link)

    def _find_collection(self, dataset):
        for access in ("public", "private"):
            url = _fs.join(self._root(access), dataset, "collection.json")
            if self._exists(url):
                return url
        raise FileNotFoundError(f"No collection {dataset}: call add_collection first")

    def add_collection(self, collection: dict) -> str:
        """Create a collection, or update its metadata keeping links, versions and extent."""
        access = collection["irv:access"]
        root_url = self._ensure_root(access)
        url = _fs.join(self._root(access), collection["id"], "collection.json")
        other = _fs.join(
            self._root("public" if access == "private" else "private"),
            collection["id"],
            "collection.json",
        )
        if self._exists(other):
            raise ImmutableError(
                f"{collection['id']} already exists with different access at {other}"
            )
        collection = dict(collection)
        if self._exists(url):
            old = self._read(url)
            for key in ("links", "irv:versions", "extent"):
                collection[key] = old[key]
        collection["links"] = list(collection.get("links", []))
        self._add_link(
            collection, {"rel": "root", "href": "../catalog.json", "type": "application/json"}
        )
        self._add_link(
            collection, {"rel": "parent", "href": "../catalog.json", "type": "application/json"}
        )
        self._write(url, collection)
        root = self._read(root_url)
        self._add_link(
            root,
            {
                "rel": "child",
                "href": f"./{collection['id']}/collection.json",
                "type": "application/json",
                "title": collection.get("title"),
            },
        )
        self._write(root_url, root)
        return url

    def add_item(self, item: dict, file_path: Union[str, Path], asset_key: str = "data") -> str:
        """Copy ``file_path`` into ``{dataset}/{version}/`` and write the item JSON.

        Idempotent for identical content; refuses to replace a published
        file or item with different content (published versions are immutable).
        """
        dataset, version = item["properties"]["irv:dataset"], item["properties"]["irv:version"]
        coll_url = self._find_collection(dataset)
        version_dir = _fs.join(coll_url, "..", version)
        asset = item["assets"][asset_key]
        file_url = _fs.join(version_dir, posixpath.basename(asset["href"]))
        item_url = _fs.join(version_dir, item["id"] + ".json")

        item = dict(item)
        item["links"] = [
            {"rel": "root", "href": "../../catalog.json", "type": "application/json"},
            {"rel": "parent", "href": "../collection.json", "type": "application/json"},
            {"rel": "collection", "href": "../collection.json", "type": "application/json"},
        ]
        if self._exists(item_url):
            old = self._read(item_url)
            if old != item:
                raise ImmutableError(f"{item_url} exists with different content")
        if self._exists(file_url):
            if (
                _fs.to_multihash(_fs.sha256_file(file_url, self.storage_options))
                != asset["file:checksum"]
            ):
                raise ImmutableError(f"{file_url} exists with different content")
        else:
            fs, path = _fs.fs_for(file_url, self.storage_options)
            fs.makedirs(posixpath.dirname(path), exist_ok=True)
            fs.put_file(str(file_path), path)
        self._write(item_url, item)

        collection = self._read(coll_url)
        first = not any(lk["rel"] == "item" for lk in collection["links"])
        self._add_link(
            collection,
            {
                "rel": "item",
                "href": f"./{version}/{item['id']}.json",
                "type": "application/geo+json",
            },
        )
        if version not in collection["irv:versions"]:
            collection["irv:versions"].append(version)
        spatial = collection["extent"]["spatial"]["bbox"]
        if first:
            spatial[0] = list(item["bbox"])  # first item replaces the placeholder extent
        else:
            spatial[0] = _union(spatial[0], item["bbox"])
        self._write(coll_url, collection)
        return item_url

    def releases(self) -> list:
        root = self._read(self._ensure_root("public"))
        return [
            posixpath.basename(posixpath.dirname(lk["href"]))
            for lk in root["links"]
            if lk["rel"] == "irv:release"
        ]

    def release(
        self,
        name: str,
        pins: Optional[dict] = None,
        from_release: Optional[str] = None,
        description: Optional[str] = None,
    ) -> str:
        """Write ``releases/{name}/catalog.json`` pinning dataset versions by reference.

        Without ``from_release``, every collection is pinned to its latest version
        (``pins`` overrides). With ``from_release``, start from that release's
        pins and add or override with ``pins``. Nothing but the manifest is written.
        """
        _check(RELEASE_RE, name, "release name")
        root_url = self._ensure_root("public")
        url = _fs.join(self.base, "releases", name, "catalog.json")
        if self._exists(url):
            raise ImmutableError(f"Release {name} already exists")
        chosen = {}
        if from_release:
            previous = self._read(_fs.join(self.base, "releases", from_release, "catalog.json"))
            chosen = {ds: d["version"] for ds, d in previous["irv:datasets"].items()}
        else:
            for access in ("public", "private"):
                r_url = _fs.join(self._root(access), "catalog.json")
                if not self._exists(r_url):
                    continue
                for link in self._read(r_url)["links"]:
                    if link["rel"] == "child":
                        coll = self._read(_fs.resolve(link["href"], r_url))
                        if coll["irv:versions"]:
                            chosen[coll["id"]] = coll["irv:versions"][-1]
        chosen.update(pins or {})

        datasets, links = {}, [
            {"rel": "root", "href": "../../catalog.json", "type": "application/json"},
            {"rel": "parent", "href": "../../catalog.json", "type": "application/json"},
        ]
        for dataset in sorted(chosen):
            version = chosen[dataset]
            coll_url = self._find_collection(dataset)
            coll = self._read(coll_url)
            if version not in coll["irv:versions"]:
                raise ValueError(f"{dataset} has no published version {version}")
            datasets[dataset] = {"version": version, "access": coll["irv:access"]}
            if coll["irv:access"] == "public":
                links.append(
                    {
                        "rel": "child",
                        "href": f"../../{dataset}/collection.json",
                        "type": "application/json",
                        "title": f"{dataset} {version}",
                    }
                )
        manifest = {
            "type": "Catalog",
            "stac_version": STAC_VERSION,
            "id": f"irv-datapkg-{name}",
            "description": description or f"irv-datapkg release {name}",
            "irv:release": name,
            "irv:created": dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
            "irv:datasets": datasets,
            "links": links,
        }
        self._write(url, manifest)
        root = self._read(root_url)
        self._add_link(
            root,
            {
                "rel": "irv:release",
                "href": f"./releases/{name}/catalog.json",
                "type": "application/json",
                "title": name,
            },
        )
        self._write(root_url, root)
        return url
