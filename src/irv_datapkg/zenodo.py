import json
import os
from datetime import datetime

import requests

from irv_datapkg.retry import retry


def _is_retryable_zenodo_error(error):
    if isinstance(error, (requests.ConnectionError, requests.Timeout)):
        return True
    return isinstance(error, requests.HTTPError) and error.response is not None and (
        error.response.status_code in {408, 429, 500, 502, 503, 504}
    )


@retry(
    max_attempts=6,
    exceptions=(requests.RequestException,),
    delay=5,
    backoff=2,
    retry_if=_is_retryable_zenodo_error,
)
def get_deposition(deposition_id, zenodo_url):
    params = {"access_token": os.environ["ZENODO_TOKEN"]}
    r = requests.get(
        f"https://{zenodo_url}/api/deposit/depositions/{deposition_id}",
        params=params,
        timeout=120,
    )
    r.raise_for_status()
    deposition = r.json()
    return deposition


def get_draft_deposition(conceptrecid, zenodo_url):
    headers = {"Authorization": f"Bearer {os.environ['ZENODO_TOKEN']}"}
    page = 1
    while True:
        r = requests.get(
            f"https://{zenodo_url}/api/deposit/depositions",
            headers=headers,
            params={"status": "draft", "page": page, "size": 100},
        )
        r.raise_for_status()
        drafts = r.json()
        draft = next(
            (
                item
                for item in drafts
                if not item["submitted"]
                and str(item.get("conceptrecid")) == str(conceptrecid)
            ),
            None,
        )
        if draft is not None:
            return draft
        if len(drafts) < 100:
            return None
        page += 1


def log_deposition(iso3, deposition, deposition_id):
    with open(
        f"zenodo/{iso3}.deposition.{deposition_id}.{datetime.now().isoformat()}.json",
        "w",
    ) as fh:
        json.dump(deposition, fh, indent=2)


def write_deposition(fname, deposition):
    with open(fname, "w") as fh:
        json.dump(deposition, fh, indent=2)
