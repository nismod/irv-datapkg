"""Remove zenodo/{ISO3}.published files if deposition is unsubmitted"""

import json
import os
import time
from pathlib import Path

import requests

if __name__ == "__main__":
    base_path = Path(".").parent
    deposition_fnames = (base_path / "zenodo").glob("*.deposition.json")
    depositions = []
    for fname in sorted(deposition_fnames):
        params = {"access_token": os.environ["ZENODO_TOKEN"]}
        print(fname)
        with open(fname, "r") as fh:
            dep = json.load(fh)
            dep["fname"] = fname
        try:
            r = requests.get(f"https://zenodo.org/api/records/{dep['id']}")
            r.raise_for_status()
            dep_latest = r.json()
        except:
            r = requests.get(
                f"https://zenodo.org/api/deposit/depositions/{dep['id']}", params=params
            )
            r.raise_for_status()
            dep_latest = r.json()

        print(dep_latest["state"])
        if dep_latest["state"] == "unsubmitted":
            published_fname = str(fname).replace(".deposition.json", ".published")
            Path(published_fname).unlink(missing_ok=True)
        time.sleep(0.5)
