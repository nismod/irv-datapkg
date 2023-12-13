import json
import sys
from glob import glob
from pathlib import Path

import geopandas
import yaml

import irv_datapkg


def package(iso3: str, package_title: str, metadata: list[dict]) -> dict:
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
        "resources": resources(iso3, metadata),
    }
    return package


def resources(iso3: str, metadata: list[dict]) -> list[dict]:
    resources = []
    for meta in metadata:  # read from YAML
        resources.append(
            {
                "name": meta["name"],
                "version": meta["version"],
                "path": [
                    # read from disk
                    f"https://irv-autopkg.s3.eu-west-2.amazonaws.com/{iso3}/{meta['name']}/*"
                ],
                "description": meta["description"],
                "format": meta["data_formats"],
                "bytes": 0,  # read from disk
                "hashes": [],  # read from disk
                "license": meta["data_license"],
                "sources": [
                    {
                        "title": meta["data_author"],
                        "path": meta["data_origin_url"],
                    }
                ],
            }
        )
    return resources


def read_metadata(base_path: Path) -> list[dict]:
    fnames: list[str] = glob(str(base_path / "metadata" / "*.yml"))
    metadata: list[dict] = []
    for fname in fnames:
        with open(fname, "r") as fh:
            metadata.append(yaml.safe_load(fh))
    return metadata


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
    package_metadata: dict = package(iso3, f"{boundary_name} Data Package", metadata)

    with open(out_fname, "w") as fh:
        json.dump(package_metadata, fh, indent=2)
