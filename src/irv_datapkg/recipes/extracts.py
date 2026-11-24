"""T2 per-region extracts from T1 catalogue items (today's Zenodo country packages).

Which items go in a package, what the files are called and how they are
clipped is set by the ``t2:`` block of each ``metadata/*.yml``:

``enabled``
    include this package in the T2 country packages.
``items`` / ``filter`` / ``exclude_patterns``
    item ids to include, and/or ``{property: value or [values]}`` (values may
    use ``{ISO3}`` and ``{ADM0_A3}``, the region's parent country code), and
    regular expressions on item ids to leave out.
``stem``, ``layer``
    output name ``{stem}__{ISO3}.{ext}`` and, for GeoPackages, layer name;
    format strings over the item properties plus ``item_id``. Several vector
    items with the same stem become layers of one GeoPackage.
``vector_format``
    ``gpkg`` (default, as published so far) or ``parquet``.
``clip``
    ``bbox`` (default: rectangular window of the region's bounds), ``geometry``
    (mask rasters / keep intersecting features), or ``none`` (whole item).
``summary``, ``summary_columns``, ``baseline_epoch``
    write ``{package}.csv`` listing the files with their item properties
    (``summary_columns``: list of properties, or ``{csv column: property}``);
    ``baseline_epoch`` replaces ``epoch == "baseline"`` in the CSV.
"""

import csv
import json
import os
import re
import shutil
import tempfile
from pathlib import Path
from typing import Iterable, Optional, Union

PathLike = Union[str, Path]
VECTOR_FORMATS = ("gpkg", "parquet")
CLIP_MODES = ("bbox", "geometry", "none")


class _Props(dict):
    """Format mapping that fails loudly on unknown keys."""

    def __missing__(self, key):
        raise KeyError(f"t2 pattern uses {{{key}}}, which is not an item property")


def _fmt(pattern: str, item_id: str, props: dict, iso3: str = "", adm0_a3: str = "") -> str:
    return pattern.format_map(_Props(props, item_id=item_id, ISO3=iso3, ADM0_A3=adm0_a3))


def selected(t2: dict, item_id: str, props: dict, iso3: str, adm0_a3: str) -> bool:
    """Whether an item goes into the region's T2 package."""
    if t2.get("items") is not None and item_id not in t2["items"]:
        return False
    if any(re.search(p, item_id) for p in t2.get("exclude_patterns") or []):
        return False
    for key, wanted in (t2.get("filter") or {}).items():
        options = wanted if isinstance(wanted, list) else [wanted]
        options = {_fmt(str(o), item_id, props, iso3, adm0_a3) for o in options}
        if str(props.get(key)) not in options:
            return False
    return True


def t2_ext(t2: dict, item_ext: str) -> str:
    if item_ext == "tif":
        return "tif"
    fmt = t2.get("vector_format", "gpkg")
    if fmt not in VECTOR_FORMATS:
        raise ValueError(f"vector_format must be one of {VECTOR_FORMATS}")
    return fmt


def plan(package: str, t2: dict, items: Iterable, iso3: str, adm0_a3: str) -> dict:
    """T2 files for one region: ``{relative path: [(collection, item_id, layer), ...]}``.

    ``items`` yields ``(collection_id, item_id, T1Item)``. Paths are relative to
    the region's package directory (``data/{ISO3}/``).
    """
    out = {}
    for coll_id, item_id, spec in items:
        props = spec.properties
        if not selected(t2, item_id, props, iso3, adm0_a3):
            continue
        stem = _fmt(t2.get("stem") or "{item_id}", item_id, props, iso3, adm0_a3)
        ext = t2_ext(t2, spec.ext)
        layer = _fmt(t2["layer"], item_id, props, iso3, adm0_a3) if t2.get("layer") else stem
        rel = f"{package}/{stem}__{iso3}.{ext}"
        out.setdefault(rel, []).append((coll_id, item_id, layer))
    for rel, members in out.items():
        if len(members) > 1 and not rel.endswith(".gpkg"):
            raise ValueError(f"{len(members)} items map to {rel}; only GeoPackages hold layers")
        layers = [m[2] for m in members]
        if len(set(layers)) != len(layers):
            raise ValueError(f"Duplicate layer names in {rel}: {layers}")
    return out


def _item_asset(item_json: PathLike) -> tuple:
    with open(item_json) as fh:
        item = json.load(fh)
    asset = item["assets"].get("data") or next(iter(item["assets"].values()))
    href = asset["href"]
    if "://" not in href and not href.startswith("/"):
        href = os.path.normpath(os.path.join(os.path.dirname(os.path.abspath(item_json)), href))
    return item, href


def extract_region(
    item_jsons: list,
    out_path: PathLike,
    *,
    geometry=None,
    clip: str = "bbox",
    layers: Optional[list] = None,
) -> Path:
    """Write one T2 file from T1 items using the catalogue client ``extract``.

    ``item_jsons`` are local item JSON paths; ``geometry`` is a shapely geometry
    in EPSG:4326 (the region). Rasters give one ``.tif``; vectors give
    ``.parquet`` (one item) or ``.gpkg`` (one layer per item, named by ``layers``).
    """
    from ..catalog import extract

    if clip not in CLIP_MODES:
        raise ValueError(f"clip must be one of {CLIP_MODES}")
    if clip != "none" and geometry is None:
        raise ValueError("geometry is required unless clip='none'")
    out_path = Path(out_path)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    layers = layers or [out_path.stem] * len(item_jsons)
    kwargs = {}
    if clip == "bbox":
        kwargs["bbox"] = tuple(geometry.bounds)
    elif clip == "geometry":
        kwargs["geometry"] = geometry
    suffix = out_path.suffix.lower()

    if suffix == ".tif":
        if len(item_jsons) != 1:
            raise ValueError("A raster T2 file comes from exactly one item")
        _, href = _item_asset(item_jsons[0])
        if clip == "none":
            shutil.copyfile(href, out_path)
            return out_path
        return extract(href, out_path, **kwargs)

    import geopandas

    with tempfile.TemporaryDirectory(dir=out_path.parent, prefix=".t2-") as tmp:
        parts = []
        for i, item_json in enumerate(item_jsons):
            _, href = _item_asset(item_json)
            if clip == "none":
                part = href
            else:
                part = extract(href, Path(tmp) / f"part{i}.parquet", **kwargs)
            parts.append(part)
        if suffix == ".parquet":
            if len(parts) != 1:
                raise ValueError("A GeoParquet T2 file comes from exactly one item")
            shutil.copyfile(parts[0], out_path)
            return out_path
        if suffix != ".gpkg":
            raise ValueError(f"Unsupported T2 output {out_path}")
        tmp_gpkg = Path(tmp) / out_path.name
        for part, layer in zip(parts, layers):
            gdf = geopandas.read_parquet(part)
            if "bbox" in gdf.columns:
                gdf = gdf.drop(columns="bbox")  # GeoParquet covering column, not data
            gdf.to_file(tmp_gpkg, driver="GPKG", layer=layer, engine="pyogrio")
        shutil.move(tmp_gpkg, out_path)
    return out_path


def summary_rows(entries: Iterable, columns, baseline_epoch=None) -> tuple:
    """CSV header and rows from ``(relative path, layer, properties)`` entries.

    ``columns``: list of property names, or ``{csv column: property}``; a
    ``path`` column (and ``layer`` for multi-layer files) is appended.
    """
    entries = list(entries)
    if isinstance(columns, dict):
        mapping = dict(columns)
    else:
        mapping = {c: c for c in (columns or [])}
    with_layer = any(layer is not None for _, layer, _ in entries)
    header = list(mapping) + (["layer"] if with_layer else []) + ["path"]
    rows = []
    for path, layer, props in entries:
        row = []
        for prop in mapping.values():
            value = props.get(prop, "")
            if prop == "epoch" and value == "baseline" and baseline_epoch is not None:
                value = baseline_epoch
            row.append(value)
        if with_layer:
            row.append(layer or "")
        row.append(path)
        rows.append(row)
    rows.sort(key=lambda r: [str(v) for v in r])
    return header, rows


def write_summary(out_csv: PathLike, entries: Iterable, columns, baseline_epoch=None) -> Path:
    header, rows = summary_rows(entries, columns, baseline_epoch)
    out_csv = Path(out_csv)
    out_csv.parent.mkdir(parents=True, exist_ok=True)
    with open(out_csv, "w", newline="") as fh:
        writer = csv.writer(fh)
        writer.writerow(header)
        writer.writerows(rows)
    return out_csv
