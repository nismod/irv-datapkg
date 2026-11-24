"""``irv-datapkg`` command line: fetch, lock, validate, extract, release,
publish, retire, linkcheck."""

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


def _storage_options(pairs, endpoint_url=None, profile=None, anon=False):
    """fsspec storage options from KEY=VALUE pairs (values parsed as JSON if possible)."""
    opts = {}
    for pair in pairs:
        if "=" not in pair:
            raise click.BadParameter(f"expected KEY=VALUE, got {pair!r}")
        key, val = pair.split("=", 1)
        try:
            opts[key] = json.loads(val)
        except ValueError:
            opts[key] = val
    if endpoint_url:
        opts["endpoint_url"] = endpoint_url
    if profile:
        opts["profile"] = profile
    if anon:
        opts["anon"] = True
    return opts


@main.command()
@click.argument("local_base")
@click.argument("remote_base")
@click.option(
    "--private-base", default=None, help="Local private root (default LOCAL_BASE-private)."
)
@click.option(
    "--private-remote", default=None, help="Remote private root (default REMOTE_BASE-private)."
)
@click.option("--no-private", is_flag=True, help="Do not publish the private root.")
@click.option("--dry-run", "-n", is_flag=True, help="Report what would change; write nothing.")
@click.option(
    "--storage-option", "-S", multiple=True, help="Remote fsspec option KEY=VALUE (repeatable)."
)
@click.option(
    "--endpoint-url",
    envvar="IRV_DATAPKG_ENDPOINT_URL",
    default=None,
    help="S3 endpoint, e.g. https://data.source.coop.",
)
@click.option("--profile", default=None, help="AWS credentials profile for the remote.")
@click.option(
    "--verify-remote",
    is_flag=True,
    help="Hash every same-size remote data file instead of trusting identical remote items.",
)
@click.option("--report", default=None, type=click.Path(), help="Write a JSON summary here.")
@click.option("--verbose", "-v", is_flag=True, help="List skipped (identical) files too.")
def publish(
    local_base,
    remote_base,
    private_base,
    private_remote,
    no_private,
    dry_run,
    storage_option,
    endpoint_url,
    profile,
    verify_remote,
    report,
    verbose,
):
    """Sync a local catalogue to REMOTE_BASE: data, then items, collections, releases, root.

    Identical files are skipped; changed immutable files are refused.
    """
    from .catalog.producer import ImmutableError
    from .publish import PublishError, publish as do_publish, write_summary

    opts = _storage_options(storage_option, endpoint_url, profile)
    try:
        summary = do_publish(
            local_base,
            remote_base,
            local_private=private_base,
            remote_private=private_remote,
            include_private=not no_private,
            dry_run=dry_run,
            remote_options=opts,
            verify_remote=verify_remote,
            log=None if dry_run else lambda m: click.echo(m, err=True),
        )
    except (ImmutableError, PublishError) as e:
        raise click.ClickException(str(e))
    if report:
        write_summary(summary, report)
    click.echo(summary.format(verbose=verbose))
    if summary.conflicts:
        sys.exit(1)


@main.command()
@click.argument("base")
@click.option("--private-base", default=None, help="Override {BASE}-private.")
@click.option("--keep-days", default=365, show_default=True, help="Retention before cold storage.")
@click.option("--now", default=None, help="Evaluate as of this date (YYYY-MM-DD).")
@click.option("--output", "-o", default=None, type=click.Path(), help="Write the report as CSV.")
@click.option("--all", "show_all", is_flag=True, help="Print every version, not just eligible.")
def retire(base, private_base, keep_days, now, output, show_all):
    """Report dataset versions eligible to move to cold (tape) storage."""
    import datetime as dt

    from .publish import RETIRE_FIELDS, retire_report, write_csv

    rows = retire_report(
        base,
        private_base=private_base,
        now=dt.date.fromisoformat(now) if now else None,
        keep_days=keep_days,
    )
    if output:
        write_csv(rows, output, RETIRE_FIELDS)
    for r in rows:
        if r["eligible"] or show_all:
            mark = "RETIRE" if r["eligible"] else "keep"
            click.echo(
                f"{mark:6} {r['access']:7} {r['dataset']} {r['version']} "
                f"({r['files']} files, {r['bytes']} bytes): {r['reason']}"
            )
    eligible = [r for r in rows if r["eligible"]]
    click.echo(
        f"{len(eligible)} of {len(rows)} versions eligible "
        f"({sum(r['bytes'] for r in eligible)} bytes)"
    )


@main.command()
@click.option(
    "--base", "-b", default=None, help="Catalogue base: check collection irv:upstream_url."
)
@click.option("--private-base", default=None, help="Override {BASE}-private.")
@click.option(
    "--datasets-csv",
    type=click.Path(exists=True, dir_okay=False),
    default=None,
    help="Also check the upstream column of this CSV (e.g. docs/datasets.csv).",
)
@click.option("--output", "-o", default="linkcheck.csv", show_default=True, type=click.Path())
@click.option("--timeout", default=30.0, show_default=True)
@click.option("--fail-on-error", is_flag=True, help="Exit 1 if any checked URL fails.")
def linkcheck(base, private_base, datasets_csv, output, timeout, fail_on_error):
    """HEAD-check upstream URLs and write a CSV report."""
    from . import linkcheck as lc

    if not base and not datasets_csv:
        raise click.UsageError("give --base and/or --datasets-csv")
    targets = []
    if datasets_csv:
        targets += lc.targets_from_csv(datasets_csv)
    if base:
        targets += lc.targets_from_catalogue(base, private_base=private_base)
    results = lc.linkcheck(targets, timeout=timeout)
    lc.write_report(results, output)
    failed = [r for r in results if r.ok is False]
    skipped = [r for r in results if r.ok is None]
    for r in failed:
        click.echo(f"FAIL {r.dataset} {r.url} {r.status or r.error}", err=True)
    click.echo(
        f"{output}: {len(results) - len(failed) - len(skipped)} ok, "
        f"{len(failed)} failed, {len(skipped)} skipped"
    )
    if failed and fail_on_error:
        sys.exit(1)


if __name__ == "__main__":
    main()
