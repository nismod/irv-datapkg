"""Check that upstream data sources are still reachable (monthly, ADR 0002).

Targets come from collection ``irv:upstream_url`` in a catalogue and/or the
``upstream`` column of ``docs/datasets.csv``. Each target is turned into zero
or more HTTP URLs:

- ``http(s)://...`` is checked as given;
- DOIs (``10.5281/zenodo.123``, ``doi:10.5061/...``) become ``https://doi.org/{doi}``;
- ``s3://bucket/prefix`` becomes a one-key ListObjectsV2 request on the
  public ``https://{bucket}.s3.amazonaws.com`` endpoint;
- anything else (free text such as ``Oxford (not public)``) is reported as skipped.

Each URL gets a HEAD request (following redirects), falling back to a
streamed GET when the server does not support HEAD. The HTTP session is a
parameter so tests can pass a fake one; nothing here needs credentials.
"""

import csv
import datetime as dt
import re
from dataclasses import asdict, dataclass
from typing import Iterable, List, Optional
from urllib.parse import quote

from .catalog import _fs

USER_AGENT = "irv-datapkg-linkcheck (+https://github.com/nismod/irv-datapkg)"
DOI_RE = re.compile(r"\b(10\.\d{4,9}/[^\s,;]+)")
FALLBACK_TO_GET = {403, 405, 501}
FIELDS = [
    "dataset",
    "source",
    "upstream",
    "url",
    "status",
    "ok",
    "final_url",
    "method",
    "error",
    "checked_at",
]


@dataclass
class Target:
    dataset: str
    source: str  # "catalogue" or "datasets.csv"
    upstream: str


@dataclass
class Result:
    dataset: str
    source: str
    upstream: str
    url: str
    status: Optional[int]
    ok: Optional[bool]  # None when skipped
    final_url: str = ""
    method: str = ""
    error: str = ""
    checked_at: str = ""


def targets_from_csv(path: str) -> List[Target]:
    with open(path, newline="") as f:
        return [
            Target(row["id"], "datasets.csv", (row.get("upstream") or "").strip())
            for row in csv.DictReader(f)
        ]


def targets_from_catalogue(
    base: str, private_base: Optional[str] = None, storage_options: Optional[dict] = None
) -> List[Target]:
    so = storage_options or {}
    base = _fs.normalise(base)
    roots = [base, _fs.normalise(private_base) if private_base else _fs.private_base(base)]
    out = []
    for root in roots:
        root_url = _fs.join(root, "catalog.json")
        if not _fs.exists(root_url, so):
            continue
        for link in _fs.read_json(root_url, so).get("links", []):
            if link["rel"] == "child":
                coll = _fs.read_json(_fs.resolve(link["href"], root_url), so)
                out.append(Target(coll["id"], "catalogue", coll.get("irv:upstream_url", "")))
    return out


def urls_for(upstream: str) -> List[str]:
    """HTTP URLs to check for an upstream description (empty: nothing checkable)."""
    upstream = (upstream or "").strip()
    if re.match(r"^https?://", upstream):
        return [upstream.split()[0]]
    if upstream.startswith("s3://"):
        bucket, _, prefix = upstream[len("s3://") :].partition("/")
        return [f"https://{bucket}.s3.amazonaws.com/?list-type=2&max-keys=1&prefix={quote(prefix)}"]
    return [f"https://doi.org/{doi.rstrip('.')}" for doi in DOI_RE.findall(upstream)]


def default_session():
    import requests

    session = requests.Session()
    session.headers["User-Agent"] = USER_AGENT
    return session


def check_url(url: str, session, timeout: float = 30.0):
    """(status, final_url, method, error) for one URL."""
    try:
        resp = session.head(url, allow_redirects=True, timeout=timeout)
        method = "HEAD"
        if resp.status_code in FALLBACK_TO_GET:
            resp = session.get(url, allow_redirects=True, timeout=timeout, stream=True)
            method = "GET"
            close = getattr(resp, "close", None)
            if close:
                close()
        return resp.status_code, getattr(resp, "url", url) or url, method, ""
    except Exception as e:  # network errors are results, not crashes
        return None, "", "HEAD", f"{type(e).__name__}: {e}"


def linkcheck(
    targets: Iterable[Target],
    session=None,
    timeout: float = 30.0,
    now: Optional[dt.datetime] = None,
) -> List[Result]:
    """Check every target, de-duplicating identical URLs."""
    session = session or default_session()
    stamp = (now or dt.datetime.now(dt.timezone.utc)).strftime("%Y-%m-%dT%H:%M:%SZ")
    cache, results = {}, []
    for t in targets:
        urls = urls_for(t.upstream)
        if not urls:
            results.append(
                Result(
                    t.dataset,
                    t.source,
                    t.upstream,
                    "",
                    None,
                    None,
                    error="skipped: not a checkable URL",
                    checked_at=stamp,
                )
            )
            continue
        for url in urls:
            if url not in cache:
                cache[url] = check_url(url, session, timeout)
            status, final, method, error = cache[url]
            ok = status is not None and 200 <= status < 400
            results.append(
                Result(
                    t.dataset, t.source, t.upstream, url, status, ok, final, method, error, stamp
                )
            )
    return results


def write_report(results: List[Result], path: str) -> str:
    with open(path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=FIELDS)
        writer.writeheader()
        for r in results:
            row = asdict(r)
            row["ok"] = "" if r.ok is None else str(r.ok).lower()
            row["status"] = "" if r.status is None else r.status
            writer.writerow(row)
    return path
