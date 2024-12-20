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
from tqdm import tqdm

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
            ("zenodo_deposition_id", "country", "title", "doi", "url", "citation")
        )
        for dep in tqdm(depositions, desc="Querying and writing"):
            dep_id = dep["id"]
            retries = 0
            max_retry = 5
            while retries < max_retry:
                try:
                    r = requests.get(f"https://zenodo.org/api/records/{dep_id}")
                    if r.status_code != 200:
                        print(r.status_code, r.text)
                        time.sleep(1)
                    r.raise_for_status()
                except Exception:
                    continue

                pub = r.json()
                break

            title = pub["title"]
            doi = pub["doi"]
            version = pub["metadata"]["version"]
            citation = f"Russell, T., Jaramillo, D., Nicholas, C., Thomas, F., Pant, R., & Hall, J. W. (2023). {title} ({version}) [Data set]. Zenodo. https://doi.org/{doi}"
            country = title.replace(
                "Infrastructure Climate Resilience Assessment Data Starter Kit for ", ""
            )
            url = f"https://zenodo.org/records/{dep_id}"
            writer.writerow((dep_id, country, title, doi, url, citation))
            time.sleep(0.5)  # respect rate-limit of ~120 per minute
