"""``irv-datapkg`` command line: fetch, lock, validate, audit, extract, release,
publish, retire, linkcheck, plus producer helpers for shell rules in recipes
(provenance, to-cog)."""

import json
import sys
from pathlib import Path

import click
import yaml

from .catalog import (
    Catalog,
    CatalogError,
    CatalogWriter,
    extract as extract_asset,
    check,
    validate_catalog,
)


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
        help="Use base, release and checksums from a lock file (--base overrides its base).",
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
        return Catalog.from_lock(lock_file, base=base, **kwargs)
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
@click.option(
    "--link",
    type=click.Choice(["copy", "hardlink", "symlink"]),
    default="copy",
    show_default=True,
    help="How to place cached files in DEST.",
)
@click.option("--no-cache", is_flag=True, help="Stream straight to DEST, bypassing the cache.")
def fetch(
    dataset,
    item_ids,
    base,
    release,
    lock_file,
    private_base,
    cache_dir,
    version,
    props,
    dest,
    link,
    no_cache,
):
    """Download DATASET items (verified by sha256) into DEST.

    With --lock, files are fetched from the lock file's hrefs and checksums
    alone (no catalogue reads) when the lock records what is needed; --base
    then overrides the recorded base, e.g. for a mirror.
    """
    Path(dest).mkdir(parents=True, exist_ok=True)
    try:
        if lock_file and version is None:
            kwargs = {"cache_dir": cache_dir}
            if private_base:
                kwargs["private_base"] = private_base
            cat = Catalog.from_lock(lock_file, base=base, **kwargs)
            try:
                ids = cat.lockfile.select(dataset, item_ids, **_parse_props(props))
            except CatalogError:
                ids = None  # e.g. an old lock without properties: read the catalogue
            if ids is not None:
                if not ids:
                    raise click.ClickException(f"No matching locked items in {dataset}")
                for item_id in ids:
                    for key in cat.lockfile.datasets[dataset]["items"][item_id]:
                        click.echo(
                            cat.fetch_locked(
                                dataset, item_id, dest, key=key, link=link, cache=not no_cache
                            )
                        )
                return
        else:
            cat = _open(base, release, lock_file, private_base, cache_dir)
        for item in _select(cat, dataset, version, props, item_ids):
            for asset in item.assets.values():
                click.echo(cat.fetch(asset, dest, link=link, cache=not no_cache))
    except CatalogError as e:
        raise click.ClickException(str(e))


def _parse_selection(items, props):
    """{dataset: [item ids..., {filters}]} from --item DATASET/ID and
    --property DATASET:KEY=VALUE options."""
    select = {}
    for spec in items:
        if "/" not in spec:
            raise click.BadParameter(f"expected DATASET/ITEM_ID, got {spec!r}")
        dataset, item_id = spec.split("/", 1)
        select.setdefault(dataset, []).append(item_id)
    filters = {}
    for spec in props:
        if ":" not in spec.split("=", 1)[0]:
            raise click.BadParameter(f"expected DATASET:KEY=VALUE, got {spec!r}")
        dataset, rest = spec.split(":", 1)
        for key, values in _parse_props([rest]).items():
            filters.setdefault(dataset, {}).setdefault(key, []).extend(values)
    for dataset, f in filters.items():
        select.setdefault(dataset, []).append(f)
    return select


@main.command()
@click.argument("datasets", nargs=-1)
@click.option("--base", "-b", envvar="IRV_DATAPKG_BASE", required=True, help="Catalogue base URI.")
@click.option("--release", "-r", required=True, help="Release to lock to, e.g. 2026.1.")
@click.option("--private-base", default=None, help="Override {BASE}-private.")
@click.option("--output", "-o", default="datapkg.lock.yaml", show_default=True)
@click.option(
    "--item", "items", multiple=True, help="Lock only this item, DATASET/ITEM_ID (repeatable)."
)
@click.option(
    "--property",
    "-p",
    "props",
    multiple=True,
    help="Lock only items matching DATASET:KEY=VALUE (repeatable; ANDed per dataset).",
)
@click.option(
    "--record-base", default=None, help="Base to record in the lock (default: --base as given)."
)
@click.option("--check", is_flag=True, help="Write nothing; exit 1 if OUTPUT is missing or stale.")
def lock(datasets, base, release, private_base, output, items, props, record_base, check):
    """Write a lock file pinning DATASETS (default: all, or those selected) of a release."""
    kwargs = {"private_base": private_base} if private_base else {}
    select = _parse_selection(items, props)
    try:
        cat = Catalog.open(base, release=release, **kwargs)
        if check:
            diff = cat.lock_matches(output, datasets=list(datasets) or None, select=select)
            click.echo(f"{output}: {diff}")
            if not diff:
                sys.exit(1)
            return
        click.echo(
            cat.write_lock(output, list(datasets) or None, select=select or None, base=record_base)
        )
    except CatalogError as e:
        raise click.ClickException(str(e))


def remote_options(f):
    """fsspec options for reading a remote catalogue (validate, audit)."""
    f = click.option("--anon", is_flag=True, help="Anonymous (unsigned) S3 access.")(f)
    f = click.option("--profile", default=None, help="AWS credentials profile.")(f)
    f = click.option(
        "--endpoint-url",
        envvar="IRV_DATAPKG_ENDPOINT_URL",
        default=None,
        help="S3 endpoint, e.g. https://data.source.coop.",
    )(f)
    f = click.option(
        "--storage-option", "-S", multiple=True, help="fsspec option KEY=VALUE (repeatable)."
    )(f)
    return f


@main.command()
@click.argument("target")
@click.option("--private-base", default=None, help="Override {BASE}-private.")
@click.option("--check-files", is_flag=True, help="Also verify every asset's sha256.")
@click.option(
    "--deep",
    is_flag=True,
    help="Also validate STAC schemas (core + extensions) and every data file's format "
    "(COG profile, GeoParquet metadata); implies --check-files.",
)
@click.option(
    "--sample",
    type=click.IntRange(min=1),
    default=None,
    help="Check the files of only N assets, chosen at random (metadata is checked in full).",
)
@click.option("--seed", type=int, default=None, help="Random seed for --sample.")
@click.option(
    "--dataset", "-d", "datasets", multiple=True, help="Only check this dataset (repeatable)."
)
@click.option("--strict", is_flag=True, help="Exit 1 on warnings too.")
@remote_options
def validate(
    target,
    private_base,
    check_files,
    deep,
    sample,
    seed,
    datasets,
    strict,
    storage_option,
    endpoint_url,
    profile,
    anon,
):
    """Validate a catalogue at TARGET (a base URI) or a lock file (*.yaml)."""
    warnings = []
    if target.endswith((".yaml", ".yml")):
        with open(target) as f:
            errors = check(yaml.safe_load(f), "lock")
    else:
        errors = validate_catalog(
            target,
            private_base=private_base,
            check_files=check_files,
            storage_options=_storage_options(storage_option, endpoint_url, profile, anon),
            deep=deep,
            sample=sample,
            seed=seed,
            datasets=datasets or None,
            warnings=warnings,
        )
    for w in warnings:
        click.echo(f"warning: {w}", err=True)
    for e in errors:
        click.echo(e, err=True)
    if errors or (strict and warnings):
        click.echo(f"{len(errors)} errors, {len(warnings)} warnings", err=True)
        sys.exit(1)
    click.echo(f"valid ({len(warnings)} warnings)" if warnings else "valid")


@main.command()
@click.argument("target")
@click.option(
    "--base",
    "-b",
    default=None,
    help="With a lock file TARGET: audit this base (e.g. a mirror) instead of the recorded one.",
)
@click.option("--private-base", default=None, help="Override {BASE}-private.")
@click.option(
    "--sample", type=click.IntRange(min=1), default=None, help="Re-hash only N random assets."
)
@click.option("--seed", type=int, default=None, help="Random seed for --sample.")
@click.option(
    "--dataset", "-d", "datasets", multiple=True, help="Only audit this dataset (repeatable)."
)
@click.option("--release", "-r", default=None, help="Only the versions this release pins.")
@click.option("--jobs", "-j", default=4, show_default=True, help="Files hashed in parallel.")
@click.option("--output", "-o", default=None, type=click.Path(), help="Write a CSV report.")
@click.option("--verbose", "-v", is_flag=True, help="Print every file, not just failures.")
@remote_options
def audit(
    target,
    base,
    private_base,
    sample,
    seed,
    datasets,
    release,
    jobs,
    output,
    verbose,
    storage_option,
    endpoint_url,
    profile,
    anon,
):
    """Re-hash data files and report size/sha256 mismatches and missing files.

    TARGET is a catalogue base (local path or URL: every asset of every version,
    public and private) or a lock file (*.yaml: the locked assets). Exits 1 if
    any file is missing, differs or cannot be read. Files of retired versions
    may be missing.
    """
    from .catalog import qa

    opts = _storage_options(storage_option, endpoint_url, profile, anon)
    if target.endswith((".yaml", ".yml")):
        if release:
            raise click.UsageError("--release does not apply to a lock file")
        refs = qa.iter_lock_assets(target, base=base, private_base=private_base)
        if datasets:
            refs = [r for r in refs if r.dataset in set(datasets)]
    else:
        if base:
            raise click.UsageError("--base only applies to a lock file TARGET")
        refs = qa.iter_assets(
            target, private_base, opts, datasets=datasets or None, release=release
        )
    refs = list(refs)

    def show(r):
        if r.failed or verbose:
            click.echo(f"{r.status.upper():9} {r.href} {r.detail}".rstrip(), err=r.failed)

    results = qa.audit(refs, opts, sample_size=sample, seed=seed, jobs=jobs, progress=show)
    if output:
        qa.write_audit(results, output)
    counts = {}
    for r in results:
        counts[r.status] = counts.get(r.status, 0) + 1
    summary = ", ".join(f"{n} {s}" for s, n in sorted(counts.items())) or "nothing to audit"
    click.echo(f"audited {len(results)} of {len(refs)} assets: {summary}")
    if any(r.failed for r in results):
        sys.exit(1)


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
@click.option(
    "--mark",
    multiple=True,
    help="Mark DATASET=VERSION as retired in collection.json instead of reporting (repeatable).",
)
@click.option("--note", default=None, help="With --mark: where the files are, how to restore.")
@click.option("--location", default=None, help="With --mark: tape location.")
def retire(base, private_base, keep_days, now, output, show_all, mark, note, location):
    """Report dataset versions eligible to move to cold (tape) storage, or
    --mark versions as retired once their files are on tape."""
    import datetime as dt

    from .publish import RETIRE_FIELDS, retire_report, write_csv

    if mark:
        if not note:
            raise click.UsageError("--mark needs --note")
        writer = CatalogWriter(base, private_base=private_base)
        for dataset, versions in _parse_props(mark).items():
            for version in versions:
                try:
                    writer.retire(dataset, version, note, date=now, location=location)
                except (ValueError, FileNotFoundError) as e:
                    raise click.ClickException(str(e))
                click.echo(f"retired {dataset} {version}")
        return

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
