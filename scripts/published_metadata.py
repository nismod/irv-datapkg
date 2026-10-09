"""Generate CSV of Zenodo records published

Run outside of the Snakemake workflow, as occasional job to record the
publication of datasets.
"""

import csv
import os
from datetime import datetime
from glob import glob

import requests
import json
from tqdm import tqdm

from irv_datapkg.retry import retry


@retry(max_attempts=5, exceptions=(requests.RequestException,), delay=1)
def fetch_published_record(dep_id):
    response = requests.get(f"https://zenodo.org/api/records/{dep_id}")
    response.raise_for_status()
    return response.json()


if __name__ == "__main__":
    base_path = os.path.join(os.path.dirname(__file__), "..")
    deposition_fnames = glob(os.path.join(base_path, "zenodo", "*.deposition.json"))
    depositions = []
    for fname in tqdm(sorted(deposition_fnames), desc="Reading"):
        with open(fname, "r") as fh:
            dep = json.load(fh)
            dep["fname"] = fname
            depositions.append(dep)

    with open(os.path.join(base_path, "records.csv"), "w") as fh:
        writer = csv.writer(fh)
        writer.writerow(
            (
                "zenodo_deposition_id",
                "country",
                "title",
                "doi",
                "url",
                "citation",
                "version",
                "iso3",
            )
        )
        for dep in tqdm(depositions, desc="Querying and writing"):
            dep_id = dep["id"]
            pub = fetch_published_record(dep_id)
            title = pub["title"]
            year = datetime.fromisoformat(pub["updated"]).year
            doi = pub["doi"]
            version = pub["metadata"]["version"]
            citation = f"Russell, T., Jaramillo, D., Nicholas, C., Thomas, F., Pant, R., & Hall, J. W. ({year}). {title} ({version}) [Data set]. Zenodo. https://doi.org/{doi}"
            country = title.replace(
                "Infrastructure Climate Resilience Assessment Data Starter Kit for ", ""
            )
            url = f"https://zenodo.org/records/{dep_id}"
            iso3 = os.path.basename(dep["fname"]).split(".")[0]
            writer.writerow((dep_id, country, title, doi, url, citation, version, iso3))
