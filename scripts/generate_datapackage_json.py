"""Generate datapackage.json for a T2 country package

Run as the final data-generation step in the Snakemake workflow. Resources are
the metadata/*.yml files with t2.enabled; files are grouped by their directory
under data/{ISO3}/ (the package resource name).
"""

from collections import defaultdict
import json
from operator import itemgetter
import sys
from pathlib import Path

import geopandas
import irv_datapkg
from irv_datapkg.recipes import load_metadata


def package(
    iso3: str, package_title: str, metadata: list[dict], checksums: dict[list]
) -> dict:
    package = {
        "name": iso3,
        "title": package_title,
        "licenses": [
            {
                "name": "ODbL-1.0",
                "path": "https://opendefinition.org/licenses/odc-odbl",
                "title": "Open Data Commons Open Database License 1.0",
            },
            {
                "name": "CC0",
                "path": "https://creativecommons.org/share-your-work/public-domain/cc0/",
                "title": "CC0",
            },
            {
                "name": "CC-BY-4.0",
                "path": "https://creativecommons.org/licenses/by/4.0/",
                "title": "Creative Commons Attribution 4.0",
            },
        ],
        "resources": resources(iso3, metadata, checksums),
    }
    return package


def resources(iso3: str, metadata: list[dict], checksums: dict[list]) -> list[dict]:
    resources = []
    for meta in metadata:  # read from YAML
        name = meta["name"]
        sorted_filemeta = sorted(checksums[name], key=itemgetter(0))
        if not sorted_filemeta:
            # e.g. no geoBoundaries for this country
            print(f"No files for {name} in {iso3}: resource left out", file=sys.stderr)
            continue
        paths, hashes, bytes = zip(*sorted_filemeta)
        resources.append(
            {
                "name": name,
                "version": meta["version"],
                "path": paths,
                "description": meta["description"],
                "format": meta["data_formats"],
                "bytes": bytes,
                "hashes": hashes,
                "license": meta["data_license"],
                "sources": [
                    {
                        "title": meta["data_author"],
                        "path": meta["data_origin_url"],
                    }
                ],
                # T1 catalogue collections the files were extracted from
                "irv_catalogue": [
                    {"id": c["id"], "version": c["version"]}
                    for c in meta.get("collections", [])
                ],
            }
        )
    return resources


def read_metadata(base_path: Path) -> list[dict]:
    """Metadata of every package resource in the T2 country packages (t2.enabled)."""
    packages = load_metadata(base_path / "metadata")
    return [pkg.raw for pkg in packages.values() if pkg.in_t2]


def read_checksums(package_path: Path) -> dict[str, list]:
    checksums = defaultdict(list)
    with open(package_path / "md5sum.txt") as fh:
        for line in fh:
            checksum, path_str = line.strip().split("  ")
            path = Path(path_str)
            dataset = str(path.parent)
            st_size = (package_path / path).stat().st_size
            checksums[dataset].append((path_str, checksum, st_size))

    return checksums


if __name__ == "__main__":
    try:
        iso3: str = snakemake.wildcards.ISO3
        out_fname: Path = Path(snakemake.output.json)
        base_path: Path = out_fname.parent.parent.parent
    except NameError:
        iso3: str = sys.argv[1]
        base_path: Path = Path(__file__).parent.parent
        out_fname: Path = base_path / "data" / iso3 / "datapackage.json"

    boundaries: geopandas.GeoDataFrame = irv_datapkg.read_boundaries(base_path)

    codes: set[str] = set(boundaries.CODE_A3)
    assert iso3 in codes, f"CODE_A3 {iso3} not found in boundaries"
    boundary_name: str = boundaries.set_index("CODE_A3").loc[iso3, "NAME"]

    metadata: list[dict] = read_metadata(base_path)
    checksums: dict[str, list] = read_checksums(out_fname.parent)
    package_metadata: dict = package(
        iso3,
        f"Infrastructure Climate Resilience Assessment Data Starter Kit for {boundary_name}",
        metadata,
        checksums,
    )

    with open(out_fname, "w") as fh:
        json.dump(package_metadata, fh, indent=2)
