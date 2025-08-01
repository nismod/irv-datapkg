import json
import os
from datetime import datetime

import requests


def get_deposition(deposition_id, zenodo_url):
    params = {"access_token": os.environ["ZENODO_TOKEN"]}
    r = requests.get(
        f"https://{zenodo_url}/api/deposit/depositions/{deposition_id}", params=params
    )
    r.raise_for_status()
    deposition = r.json()
    return deposition


def log_deposition(iso3, deposition, deposition_id):
    with open(
        f"zenodo/{iso3}.deposition.{deposition_id}.{datetime.now().isoformat()}.json",
        "w",
    ) as fh:
        json.dump(deposition, fh, indent=2)


def write_deposition(fname, deposition):
    with open(fname, "w") as fh:
        json.dump(deposition, fh, indent=2)
