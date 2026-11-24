"""``irv-datapkg`` command line: fetch, lock, validate, extract, release, plus
producer helpers for shell rules in recipes (provenance, to-cog)."""

import json
import sys
from pathlib import Path

import click
import yaml

from .catalog import Catalog, CatalogWriter, extract as extract_asset, check, validate_catalog


def _parse_props(values):
    props = {}
    for v in values:
        if "=" not in v:
            raise click.BadParameter(f"expected KEY=VALUE, got {v!r}")
        key, val = v.split("=", 1)
        props.setdefault(key, []).append(val)
    return props


def catalog_options(f):
    f = click.option(
        "--cache-dir",
        default=None,
        help="Local cache (default $IRV_DATAPKG_CACHE or ~/.cache/irv-datapkg).",
    )(f)
    f = click.option("--private-base", default=None, help="Override {BASE}-private.")(f)
    f = click.option(
        "--lock",
        "lock_file",
        type=click.Path(exists=True),
        default=None,
        help="Use base, release and checksums from a datapkg.lock.yaml.",
    )(f)
    f = click.option(
        "--release", "-r", default=None, help="Release, e.g. 2026.1 (default: latest versions)."
    )(f)
    f = click.option(
        "--base", "-b", envvar="IRV_DATAPKG_BASE", default=None, help="Catalogue base URI."
    )(f)
    return f


def _open(base, release, lock_file, private_base, cache_dir):
    kwargs = {"cache_dir": cache_dir}
    if private_base:
        kwargs["private_base"] = private_base
    if lock_file:
        return Catalog.from_lock(lock_file, **kwargs)
    if not base:
        raise click.UsageError("give --base (or IRV_DATAPKG_BASE) or --lock")
    return Catalog.open(base, release=release, **kwargs)


def _select(cat, dataset, version, props, item_ids):
    items = cat.items(dataset, version=version, **_parse_props(props))
    if item_ids:
        items = [i for i in items if i.id in item_ids]
    if not items:
        raise click.ClickException(f"No matching items in {dataset}")
    return items


@click.group()
def main():
    """irv-datapkg catalogue client."""


@main.command()
@click.argument("dataset")
@click.argument("item_ids", nargs=-1)
@catalog_options
@click.option("--version", "-v", default=None, help="Dataset version (default: pinned or latest).")
@click.option(
    "--property", "-p", "props", multiple=True, help="Filter items, KEY=VALUE (repeatable)."
)
@click.option(
    "--dest", "-o", type=click.Path(file_okay=False), default=".", help="Output directory."
)
def fetch(
    dataset, item_ids, base, release, lock_file, private_base, cache_dir, version, props, dest
):
    """Download DATASET items (verified by sha256) into DEST."""
    cat = _open(base, release, lock_file, private_base, cache_dir)
    Path(dest).mkdir(parents=True, exist_ok=True)
    for item in _select(cat, dataset, version, props, item_ids):
        for asset in item.assets.values():
            click.echo(cat.fetch(asset, dest))


@main.command()
@click.argument("datasets", nargs=-1)
@click.option("--base", "-b", envvar="IRV_DATAPKG_BASE", required=True, help="Catalogue base URI.")
@click.option("--release", "-r", required=True, help="Release to lock to, e.g. 2026.1.")
@click.option("--private-base", default=None, help="Override {BASE}-private.")
@click.option("--output", "-o", default="datapkg.lock.yaml", show_default=True)
def lock(datasets, base, release, private_base, output):
    """Write a lock file pinning DATASETS (default: all) of a release."""
    kwargs = {"private_base": private_base} if private_base else {}
    cat = Catalog.open(base, release=release, **kwargs)
    click.echo(cat.write_lock(output, list(datasets) or None))


@main.command()
@click.argument("target")
@click.option("--private-base", default=None, help="Override {BASE}-private.")
@click.option("--check-files", is_flag=True, help="Also verify every asset's sha256.")
def validate(target, private_base, check_files):
    """Validate a catalogue at TARGET (a base URI) or a lock file (*.yaml)."""
    if target.endswith((".yaml", ".yml")):
        with open(target) as f:
            errors = check(yaml.safe_load(f), "lock")
    else:
        errors = validate_catalog(target, private_base=private_base, check_files=check_files)
    for e in errors:
        click.echo(e, err=True)
    if errors:
        sys.exit(1)
    click.echo("valid")


@main.command()
@click.argument("dataset")
@click.argument("item_ids", nargs=-1)
@catalog_options
@click.option("--version", "-v", default=None)
@click.option(
    "--property", "-p", "props", multiple=True, help="Filter items, KEY=VALUE (repeatable)."
)
@click.option("--bbox", default=None, help="minx,miny,maxx,maxy in EPSG:4326.")
@click.option(
    "--geometry", type=click.Path(exists=True), default=None, help="GeoJSON file (EPSG:4326)."
)
@click.option(
    "--dest", "-o", type=click.Path(file_okay=False), default=".", help="Output directory."
)
def extract(
    dataset,
    item_ids,
    base,
    release,
    lock_file,
    private_base,
    cache_dir,
    version,
    props,
    bbox,
    geometry,
    dest,
):
    """Clip DATASET items to a bbox or polygon, writing DEST/{dataset}/{version}/."""
    cat = _open(base, release, lock_file, private_base, cache_dir)
    kwargs = {}
    if bbox:
        kwargs["bbox"] = [float(v) for v in bbox.split(",")]
    if geometry:
        with open(geometry) as f:
            gj = json.load(f)
        if gj.get("type") == "FeatureCollection":
            from shapely.geometry import shape
            from shapely.ops import unary_union

            gj = unary_union([shape(feat["geometry"]) for feat in gj["features"]])
        kwargs["geometry"] = gj
    if len(kwargs) != 1:
        raise click.UsageError("give exactly one of --bbox or --geometry")
    for item in _select(cat, dataset, version, props, item_ids):
        asset = item.asset
        out = Path(dest, dataset, item.version, asset.filename)
        click.echo(extract_asset(asset, out, storage_options=cat.storage_options, **kwargs))


@main.command()
@click.argument("base")
@click.argument("name")
@click.option(
    "--pin", multiple=True, help="DATASET=VERSION (repeatable); overrides latest/previous."
)
@click.option("--from", "from_release", default=None, help="Start from a previous release's pins.")
@click.option("--description", default=None)
@click.option("--private-base", default=None, help="Override {BASE}-private.")
def release(base, name, pin, from_release, description, private_base):
    """Write release manifest NAME (YYYY.N) under BASE/releases/."""
    pins = {k: v[-1] for k, v in _parse_props(pin).items()}
    writer = CatalogWriter(base, private_base=private_base)
    click.echo(writer.release(name, pins=pins, from_release=from_release, description=description))


@main.command()
@click.argument("files", nargs=-1, required=True, type=click.Path(exists=True, dir_okay=False))
@click.option(
    "--output",
    "-o",
    required=True,
    help="Sidecar to write (<file>.provenance.json or <dir>/provenance.json).",
)
@click.option(
    "--tool", required=True, help="Download tool, e.g. 'aws s3 sync' (its version is recorded)."
)
@click.option(
    "--url-prefix", default=None, help="Upstream URL = prefix + path relative to the sidecar."
)
@click.option("--source-url", default=None, help="One upstream URL (or request) for all files.")
@click.option(
    "--urls-from",
    type=click.Path(exists=True, dir_okay=False),
    default=None,
    help="File listing upstream URLs, one per line, matched to FILES by file name.",
)
@click.option("--source", default=None, help="Free-text description of the upstream.")
@click.option("--retrieved", default=None, help="Retrieval date YYYY-MM-DD (default today).")
def provenance(files, output, tool, url_prefix, source_url, urls_from, source, retrieved):
    """Write a T0 provenance sidecar (source URL, sha256, size, date, tool) for FILES."""
    from .recipes.provenance import tool_version, write_provenance

    urls = {}
    if urls_from:
        with open(urls_from) as fh:
            for line in fh:
                url = line.strip()
                if url:
                    urls[url.split("?", 1)[0].rstrip("/").rsplit("/", 1)[-1]] = url
    exe = tool.split()[0]
    label = tool_version(exe)
    if tool.split()[1:]:
        label = f"{label} ({tool})"
    click.echo(
        write_provenance(
            output,
            files,
            tool=label,
            url_prefix=url_prefix,
            source_url=source_url,
            urls=urls,
            source=source,
            retrieved=retrieved,
        )
    )


@main.command("to-cog")
@click.argument("src")
@click.argument("dst", type=click.Path(dir_okay=False))
@click.option("--nodata", type=float, default=None, help="Assign nodata (no value change).")
@click.option("--crs", default=None, help="Assign CRS (no reprojection).")
@click.option("--units", default=None, help="Band unit.")
@click.option("--compress", default="ZSTD", type=click.Choice(["ZSTD", "DEFLATE"]))
@click.option("--resampling", default="nearest", help="Overview resampling.")
def to_cog_cmd(src, dst, nodata, crs, units, compress, resampling):
    """Convert SRC (any GDAL raster) to a COG on the source grid (ADR 0004)."""
    from .recipes.formats import to_cog

    click.echo(
        to_cog(
            src,
            dst,
            nodata=nodata,
            crs=crs,
            units=units,
            compress=compress,
            overview_resampling=resampling,
        )
    )


if __name__ == "__main__":
    main()
