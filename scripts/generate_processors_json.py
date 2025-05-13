"""Generate config JSON for irv-autopkg API

Metadata listing of "processors" - one per dataset
"""

import json
from glob import glob
from pathlib import Path

import yaml


def read_metadata(base_path: Path) -> list[dict]:
    fnames: list[str] = glob(str(base_path / "metadata" / "*.yml"))
    metadata: list[dict] = []
    for fname in fnames:
        with open(fname, "r") as fh:
            metadata.append(yaml.safe_load(fh))
    return metadata


if __name__ == "__main__":
    base_path = Path(__file__).parent.parent
    meta = read_metadata(base_path)
    processors = []
    for resource in meta:
        processor = {"name": resource["name"], "versions": [resource]}
        processors.append(processor)
    with open(base_path / "processors.json", "w") as fh:
        json.dump(processors, fh)
