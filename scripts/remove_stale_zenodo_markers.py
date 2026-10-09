"""Remove Zenodo markers for records with an outdated datapkg version.

The version in each record's citation is compared with ``datapkg_version`` in
``config.json``. For records that do not match, both the deposited and
published marker files are removed when present.
"""

import csv
import json
import argparse
from pathlib import Path


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="list stale marker files without removing them",
    )
    args = parser.parse_args()

    base_path = Path(__file__).resolve().parent.parent

    with (base_path / "config.json").open(encoding="utf-8") as config_file:
        datapkg_version = json.load(config_file)["datapkg_version"]

    stale_iso3 = set()
    with (base_path / "records.csv").open(newline="", encoding="utf-8") as records_file:
        for row in csv.DictReader(records_file):
            if row["version"] != datapkg_version:
                stale_iso3.add(row["iso3"])

    for iso3 in sorted(stale_iso3):
        for suffix in ("deposited", "published"):
            marker = base_path / "zenodo" / f"{iso3}.{suffix}"
            if marker.exists():
                if args.dry_run:
                    print(f"Would remove {marker.relative_to(base_path)}")
                else:
                    marker.unlink()
                    print(f"Removed {marker.relative_to(base_path)}")


if __name__ == "__main__":
    main()
