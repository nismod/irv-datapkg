"""Accept Zenodo community requests

Run as occasional interaction with Zenodo, outside of Snakemake workflow.
Requires ZENODO_TOKEN to correspond to a user with "Curator" role in the
requested community.
"""

import os
import time

import requests


ZENODO_TOKEN = os.environ["ZENODO_TOKEN"]

if __name__ == "__main__":
    # Get all open requests
    r = requests.get(
        "https://zenodo.org/api/user/requests",
        params={"is_open": True, "access_token": ZENODO_TOKEN, "size": 500},
    )
    r.raise_for_status()

    # Filter based on title
    community_requests = [
        hit
        for hit in r.json()["hits"]["hits"]
        if "Infrastructure Climate Resilience Assessment Data Starter Kit"
        in hit["title"]
    ]

    # POST to accept
    for community_request in community_requests:
        cr_id = community_request["id"]
        r = requests.post(
            f"https://zenodo.org/api/requests/{cr_id}/actions/accept",
            params={"access_token": ZENODO_TOKEN},
        )
        r.raise_for_status()
        time.sleep(0.5)
