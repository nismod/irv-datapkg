#
# Publish the local catalogue to source.coop, make releases, check upstream links.
# See docs/storage.md. Config keys (config.json, or --configfile config/storage.example.yaml):
#
#   workspace_dir            Oxford drive working space (default ".")
#   catalogue_base           local public catalogue (default {workspace_dir}/catalogue/irv-datapkg)
#   catalogue_private_base   local private catalogue (default {catalogue_base}-private)
#   publish_remote           remote public base, e.g. s3://nismod/irv-datapkg
#   publish_private_remote   remote private base (default {publish_remote}-private)
#   publish_private          publish the private root too (default true)
#   publish_storage_options  fsspec options for the remote, e.g. {endpoint_url: https://data.source.coop}
#
# Rule names are prefixed "catalogue_" because rules/zenodo.smk already defines
# a rule called "publish". None of these rules declare outputs, so they run
# every time they are requested by name:
#
#   snakemake -c1 catalogue_publish_dry_run
#   snakemake -c1 catalogue_publish
#   snakemake -c1 catalogue_release --config release=2027.1 release_from=2026.2 \
#       release_pins=aqueduct_flood=v2-r2,storm_fixed_rp=1.0.0-r1
#   snakemake -c1 catalogue_retire_report
#   snakemake -c1 linkcheck
#
import datetime as _dt
import os as _os

# Resolved workspace paths come from rules/common.smk when it is included first;
# fall back to the raw config keys when this file is used on its own.
PUBLISH_WORKSPACE = globals().get("WORKSPACE", config.get("workspace_dir", "."))
PUBLISH_BASE = globals().get(
    "CATALOGUE",
    config.get(
        "catalogue_base", _os.path.join(PUBLISH_WORKSPACE, "catalogue", "irv-datapkg")
    ),
)
PUBLISH_PRIVATE_BASE = config.get(
    "catalogue_private_base", PUBLISH_BASE.rstrip("/") + "-private"
)
PUBLISH_REMOTE = config.get("publish_remote")
PUBLISH_PRIVATE_REMOTE = config.get("publish_private_remote")
PUBLISH_PRIVATE = bool(config.get("publish_private", True))
PUBLISH_OPTIONS = config.get("publish_storage_options") or {}
PUBLISH_REPORTS = _os.path.join(PUBLISH_WORKSPACE, "reports")


def _publish(dry_run, log_path):
    from irv_datapkg.catalog import validate_catalog
    from irv_datapkg.publish import publish, write_summary

    if not PUBLISH_REMOTE:
        raise ValueError("Set publish_remote in config (see docs/storage.md)")
    errors = validate_catalog(PUBLISH_BASE, private_base=PUBLISH_PRIVATE_BASE)
    if errors:
        raise ValueError("Catalogue is invalid, not publishing:\n" + "\n".join(errors))
    summary = publish(
        PUBLISH_BASE,
        PUBLISH_REMOTE,
        local_private=PUBLISH_PRIVATE_BASE,
        remote_private=PUBLISH_PRIVATE_REMOTE,
        include_private=PUBLISH_PRIVATE,
        dry_run=dry_run,
        remote_options=PUBLISH_OPTIONS,
        log=print,
    )
    _os.makedirs(_os.path.dirname(log_path), exist_ok=True)
    write_summary(summary, log_path)
    print(summary.format())
    if summary.conflicts:
        raise ValueError(f"{len(summary.conflicts)} conflicts, see {log_path}")


def _release_name(value):
    """--config parses 2026.2 as a float; turn it back into a release name.

    A float cannot tell 2026.1 from 2026.10, so quote such names:
    --config release='"2026.10"'.
    """
    return None if value is None else str(value)


def _stamp():
    return _dt.datetime.now(_dt.timezone.utc).strftime("%Y%m%dT%H%M%SZ")


rule catalogue_publish_dry_run:
    """Validate the local catalogue and report what a publish would upload."""
    run:
        _publish(
            True, _os.path.join(PUBLISH_REPORTS, f"publish-dry-run-{_stamp()}.json")
        )


rule catalogue_publish:
    """Validate, then upload: data, items, collections, releases, root catalogues."""
    run:
        _publish(False, _os.path.join(PUBLISH_REPORTS, f"publish-{_stamp()}.json"))


rule catalogue_release:
    """Write releases/{release}/catalog.json in the local catalogue (then publish)."""
    run:
        from irv_datapkg.catalog import CatalogWriter

        name = _release_name(config.get("release"))
        if not name:
            raise ValueError(
                "Give --config release=YYYY.N [release_from=...] [release_pins=a=v,...]"
            )
        pins = config.get("release_pins") or {}
        if isinstance(pins, str):
            pins = dict(p.split("=", 1) for p in pins.split(",") if p)
        pins = {k: str(v) for k, v in pins.items()}
        writer = CatalogWriter(PUBLISH_BASE, private_base=PUBLISH_PRIVATE_BASE)
        print(
            writer.release(
                name,
                pins=pins,
                from_release=_release_name(config.get("release_from")),
                description=config.get("release_description"),
            )
        )


rule catalogue_retire_report:
    """List dataset versions eligible to move to tape (no release pins them in a year)."""
    run:
        from irv_datapkg.publish import RETIRE_FIELDS, retire_report, write_csv

        _os.makedirs(PUBLISH_REPORTS, exist_ok=True)
        rows = retire_report(PUBLISH_BASE, private_base=PUBLISH_PRIVATE_BASE)
        out = _os.path.join(
            PUBLISH_REPORTS, f"retire-{_dt.date.today().isoformat()}.csv"
        )
        write_csv(rows, out, RETIRE_FIELDS)
        print(
            f"{sum(r['eligible'] for r in rows)} of {len(rows)} versions eligible: {out}"
        )


rule linkcheck:
    """HEAD-check upstream URLs from docs/datasets.csv and the local catalogue."""
    run:
        from irv_datapkg import linkcheck as lc

        targets = lc.targets_from_csv("docs/datasets.csv")
        if _os.path.exists(_os.path.join(PUBLISH_BASE, "catalog.json")):
            targets += lc.targets_from_catalogue(
                PUBLISH_BASE, private_base=PUBLISH_PRIVATE_BASE
            )
        _os.makedirs(PUBLISH_REPORTS, exist_ok=True)
        out = _os.path.join(
            PUBLISH_REPORTS, f"linkcheck-{_dt.date.today().isoformat()}.csv"
        )
        results = lc.linkcheck(targets)
        lc.write_report(results, out)
        failed = [r for r in results if r.ok is False]
        print(f"{out}: {len(failed)} failed of {len(results)}")
