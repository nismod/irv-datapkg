import io
import requests
import pandas as pd
from concurrent.futures import ThreadPoolExecutor
from threading import local
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

thread_sessions = local()


def get_session():
    if not hasattr(thread_sessions, "session"):
        thread_sessions.session = create_session()
    return thread_sessions.session


def create_session():
    retry = Retry(
        total=5,
        connect=5,
        read=5,
        status=5,
        backoff_factor=1,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods={"GET"},
        respect_retry_after_header=True,
    )
    adapter = HTTPAdapter(max_retries=retry, pool_connections=8, pool_maxsize=8)
    session = requests.Session()
    session.mount("https://", adapter)
    return session


csv_url = (
    "https://raw.githubusercontent.com/"
    "nismod/irv-datapkg/main/records.csv"
)
df = pd.read_csv(io.StringIO(get_session().get(csv_url).text))

def downloads(record_id):
    response = get_session().get(
        f"https://zenodo.org/api/records/{record_id}",
        timeout=30,
    )
    response.raise_for_status()
    d = response.json()["stats"]["downloads"]
    v = response.json()["stats"]["views"]
    return (record_id, d, v)

with ThreadPoolExecutor(max_workers=8) as pool:
    counts = list(pool.map(downloads, df.zenodo_deposition_id))


stats = pd.DataFrame(columns=["record_id", "downloads", "views"], data=counts)
stats.to_csv("stats.csv", index=False)
print("Total downloads (all versions):", stats.downloads.sum())

