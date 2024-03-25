"""Generate CSV of Zenodo records published

Run outside of the Snakemake workflow, as occasional job to record the
publication of datasets.
"""

import csv
import os
import time
from glob import glob

import requests
import json

if __name__ == "__main__":
    base_path = os.path.join(os.path.dirname(__file__), "..")
    deposition_fnames = glob(os.path.join(base_path, "zenodo", "*.json"))
    depositions = []
    for fname in sorted(deposition_fnames):
        with open(fname, "r") as fh:
            dep = json.load(fh)
            dep["fname"] = fname
            depositions.append(dep)

    with open(os.path.join(base_path, "records.csv"), "w") as fh:
        writer = csv.writer(fh)
        writer.writerow(
            ("zenodo_deposition_id", "country", "title", "doi", "url", "citation")
        )
        for dep in depositions:
            dep_id = dep["id"]
            r = requests.get(f"https://zenodo.org/api/records/{dep_id}")
            print(r.status_code, dep["fname"])
            if r.status_code != 200:
                continue
            pub = r.json()
            title = pub["title"]
            doi = pub["doi"]
            citation = f"Russell, T., Jaramillo, D., Nicholas, C., Thomas, F., Pant, R., & Hall, J. W. (2023). {title} (0.1.0) [Data set]. Zenodo. https://doi.org/{doi}"
            country = title.replace(
                "Infrastructure Climate Resilience Assessment Data Starter Kit for ", ""
            )
            url = f"https://zenodo.org/records/{dep_id}"
            writer.writerow((dep_id, country, title, doi, url, citation))
            time.sleep(1)
