"""
generate_rdls_metadata.py — Generate NISMOD ICRA RDLS records from template.

Uses the Aruba ICRA template as a golden reference and generates one
RDLS v0.3 JSON metadata file per country/territory listed in records.csv.

Self-contained version — all utility functions are inlined (no external
module imports required beyond standard library + requests + pycountry + pyyaml).

Field substitutions per country:
  - id, title, description (country name), version (from Zenodo API)
  - spatial: countries, bbox, gazetteer_entries
  - attributions[publisher].entity.url (Zenodo URL)
  - resources[*].access_url, download_url, description (file code from Zenodo API)
  - referenced_by[0]: name, url, doi

Requirements:
    pip install requests pycountry pyyaml

Run (from repo root):
    python scripts/generate_rdls_metadata.py
"""

import copy
import csv
import io
import json
import re
import sys
from collections import Counter
from pathlib import Path
from typing import Any, Dict, List, Optional, Union

import requests
import yaml

# ---------------------------------------------------------------------------
# Paths (relative to repo root — this script lives in scripts/)
# ---------------------------------------------------------------------------
SCRIPT_DIR = Path(__file__).resolve().parent
REPO_ROOT = SCRIPT_DIR.parent
RDLS_META_DIR = REPO_ROOT / "metadata" / "rdls"

# Template (golden reference — manually created for Aruba)
TEMPLATE_PATH = RDLS_META_DIR / "template_rdls_nismod_icra.json"

# Records source (local first, then remote fallback)
RECORDS_CSV_URL = (
    "https://raw.githubusercontent.com/nismod/irv-datapkg/main/records.csv"
)
RECORDS_CSV_LOCAL = REPO_ROOT / "records.csv"

# Lookup configs
BBOX_YAML = RDLS_META_DIR / "country_bbox.yaml"
GEONAMES_YAML = RDLS_META_DIR / "geonames_country_ids.yaml"
SPATIAL_YAML = RDLS_META_DIR / "spatial_lite.yaml"

# Output
OUTPUT_DIR = REPO_ROOT / "rdls_output"

# Template reference values (what to search & replace)
TEMPLATE_ISO3 = "ABW"
TEMPLATE_COUNTRY = "Aruba"
TEMPLATE_ZENODO_ID = "10796649"


# ---------------------------------------------------------------------------
# Inlined utility functions (from src.utils)
# ---------------------------------------------------------------------------

def load_json(path: Union[str, Path]) -> Any:
    """Load JSON file with UTF-8 encoding."""
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def load_yaml(path: Union[str, Path]) -> Any:
    """Load YAML file."""
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def write_json(path: Union[str, Path], obj: Any, pretty: bool = True) -> None:
    """Write JSON file atomically with UTF-8 encoding."""
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(".tmp")
    try:
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(obj, f, ensure_ascii=False, indent=2 if pretty else None)
            f.write("\n")
        tmp.replace(path)
    except Exception:
        if tmp.exists():
            tmp.unlink()
        raise


# ---------------------------------------------------------------------------
# Inlined spatial functions (from src.spatial)
# ---------------------------------------------------------------------------

def _norm_country_key(s: str) -> str:
    """Normalize country name for lookup."""
    s = (s or "").strip().lower()
    s = re.sub(r"[\(\)\[\]\{\}\.\,\;\:]", " ", s)
    s = s.replace("&", " and ")
    s = re.sub(r"[^a-z0-9\s\-']", " ", s)
    return re.sub(r"\s+", " ", s).strip()


def _try_pycountry(name: str) -> Optional[str]:
    """Try to resolve country name using pycountry."""
    try:
        import pycountry
        c = pycountry.countries.lookup(name)
        return getattr(c, "alpha_3", None)
    except Exception:
        return None


def country_name_to_iso3(
    name: str,
    fixes: Optional[Dict[str, str]] = None,
) -> Optional[str]:
    """Convert country name to ISO 3166-1 alpha-3 code.

    Resolution order:
        1. Already a 3-letter alpha code -> return uppercased
        2. Check country_name_fixes dict
        3. Try pycountry library
    """
    n = (name or "").strip()
    if not n:
        return None

    # Already ISO3?
    if len(n) == 3 and n.isalpha():
        return n.upper()

    key = _norm_country_key(n)

    # Check fixes
    if fixes and key in fixes:
        return fixes[key]

    # Try pycountry
    return _try_pycountry(n)


def load_spatial_config(yaml_path: Union[str, Path]) -> Dict[str, Any]:
    """Load spatial configuration (country name fixes only for ICRA).

    Returns dict with keys:
        country_name_fixes: dict[str, str] (normalized keys)
    """
    cfg = load_yaml(yaml_path)

    # Normalize country fix keys
    fixes = {_norm_country_key(k): v for k, v in cfg.get("country_name_fixes", {}).items()}

    return {
        "country_name_fixes": fixes,
    }


# ---------------------------------------------------------------------------
# GeoNames URI builder
# ---------------------------------------------------------------------------

def build_geonames_uri(geoname_id: int, country_name: str) -> str:
    """Build GeoNames URI from ID and country name."""
    slug = country_name.lower()
    slug = re.sub(r"[^a-z0-9\s-]", "", slug)  # remove special chars
    slug = re.sub(r"\s+", "-", slug.strip())    # spaces to hyphens
    return f"https://www.geonames.org/{geoname_id}/{slug}.html"


def make_split_slug(country_name: str) -> str:
    """Create a filename-safe slug for split entries (e.g., 'Fiji (East)' -> 'east')."""
    # Extract parenthetical if present
    m = re.search(r"\(([^)]+)\)", country_name)
    if m:
        slug = m.group(1).lower().strip()
    else:
        slug = country_name.lower().strip()
    slug = re.sub(r"[^a-z0-9]+", "-", slug).strip("-")
    return slug


# ---------------------------------------------------------------------------
# Zenodo API lookup
# ---------------------------------------------------------------------------

def fetch_zenodo_metadata(zenodo_url: str) -> dict:
    """Query Zenodo API to get actual ZIP filename and version for a record.

    The ZIP files on Zenodo don't always use ISO3 codes as filenames.
    Split territories (e.g., Antigua/Barbuda within ATG) use unique
    3-letter codes (ACA, ACB). Some main entries also differ (Portugal
    uses PRX, not PRT). Dataset versions also vary (0.1.0, 0.2.0, 0.2.1).

    Args:
        zenodo_url: e.g. "https://zenodo.org/records/10796759"
    Returns:
        Dict with keys:
            file_code: str or None — e.g. "ACA" (from first .zip file key)
            version: str or None — e.g. "0.2.1" (from metadata.version)
    """
    result = {"file_code": None, "version": None}
    try:
        record_id = zenodo_url.rstrip("/").split("/")[-1]
        api_url = f"https://zenodo.org/api/records/{record_id}"
        resp = requests.get(api_url, timeout=30)
        resp.raise_for_status()
        data = resp.json()
        # Extract file code from first .zip
        for f in data.get("files", []):
            key = f.get("key", "")
            if key.lower().endswith(".zip"):
                result["file_code"] = key[:-4]  # strip .zip extension
                break
        # Extract version
        result["version"] = data.get("metadata", {}).get("version") or data.get("version")
    except Exception as e:
        print(f"    WARNING: Zenodo API failed for {zenodo_url}: {e}")
    return result


# ---------------------------------------------------------------------------
# Template substitution
# ---------------------------------------------------------------------------

def generate_icra_record(
    template_ds: dict,
    iso3: str,
    country_name: str,
    zenodo_id: str,
    title: str,
    doi: str,
    url: str,
    bbox: list,
    geonames_entry: dict | None,
    record_id: str,
    file_code: str | None = None,
    version: str | None = None,
) -> dict:
    """Generate an ICRA RDLS record by substituting template fields.

    Args:
        template_ds: The template dataset dict (single dataset, not the wrapper).
        iso3: ISO 3166-1 alpha-3 code (uppercase).
        country_name: Country name from records.csv.
        zenodo_id: Zenodo deposition ID from records.csv.
        title: Dataset title from records.csv.
        doi: DOI string from records.csv.
        url: Zenodo URL from records.csv.
        bbox: Bounding box [minx, miny, maxx, maxy] or empty list.
        geonames_entry: {geoname_id, name} from geonames lookup, or None.
        record_id: The RDLS record ID (includes split suffix if needed).
        file_code: Actual Zenodo ZIP file code (e.g. "ACA"). Falls back to ISO3.
        version: Dataset version from Zenodo API (e.g. "0.2.1"). Falls back to template.

    Returns:
        Modified dataset dict.
    """
    ds = copy.deepcopy(template_ds)
    iso3_upper = iso3.upper()
    # Use Zenodo file code for download URLs; fall back to ISO3
    fc = file_code or iso3_upper

    # 1. ID
    ds["id"] = record_id

    # 2. Title
    ds["title"] = title

    # 3. Description — replace country name
    ds["description"] = ds["description"].replace(TEMPLATE_COUNTRY, country_name)

    # 4. Version — update if provided
    if version:
        ds["version"] = version

    # 5. Spatial
    ds["spatial"]["countries"] = [iso3_upper]

    if bbox:
        ds["spatial"]["bbox"] = bbox
    else:
        ds["spatial"].pop("bbox", None)

    if geonames_entry:
        uri = build_geonames_uri(geonames_entry["geoname_id"], geonames_entry["name"])
        ds["spatial"]["gazetteer_entries"] = [
            {
                "scheme": "GEONAMES",
                "description": "Geonames",
                "uri": uri,
                "id": "gazetteer_1",
            }
        ]
    else:
        ds["spatial"].pop("gazetteer_entries", None)

    # 6. Attributions — update publisher URL
    for attr in ds.get("attributions", []):
        if attr.get("role") == "publisher":
            attr["entity"]["url"] = url

    # 7. Resources — update access_url, download_url, descriptions, id, title
    for res in ds.get("resources", []):
        # access_url -> Zenodo record URL
        res["access_url"] = url
        # download_url -> {url}/files/{FILE_CODE}.zip
        res["download_url"] = f"{url}/files/{fc}.zip"
        # Description — replace country name
        desc = res.get("description", "")
        desc = desc.replace(TEMPLATE_COUNTRY, country_name)
        res["description"] = desc
        # Title — replace country name
        res_title = res.get("title", "")
        res_title = res_title.replace(TEMPLATE_COUNTRY, country_name)
        res["title"] = res_title
        # ID — replace template ISO3
        res_id = res.get("id", "")
        res_id = res_id.replace(TEMPLATE_ISO3.lower(), iso3_upper.lower())
        res["id"] = res_id

    # 8. Referenced_by — update only if template has entries (respect empty arrays)
    for ref in ds.get("referenced_by", []):
        ref["name"] = title
        ref["url"] = url
        ref["doi"] = f"https://doi.org/{doi}"

    return ds


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    print("=" * 70)
    print("NISMOD ICRA RDLS Record Generator")
    print("=" * 70)

    # --- Load template ---
    print(f"\n[1] Loading template: {TEMPLATE_PATH.name}")
    template_wrapper = load_json(TEMPLATE_PATH)
    template_ds = template_wrapper["datasets"][0]
    print(f"    Template ID: {template_ds['id']}")

    # --- Load records.csv (local fallback) ---
    print(f"\n[2] Loading records.csv...")
    if RECORDS_CSV_LOCAL.exists():
        with open(RECORDS_CSV_LOCAL, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            rows = list(reader)
        print(f"    {len(rows)} rows loaded from {RECORDS_CSV_LOCAL}")
    else:
        try:
            resp = requests.get(RECORDS_CSV_URL, timeout=15)
            resp.encoding = "utf-8"
            resp.raise_for_status()
            reader = csv.DictReader(io.StringIO(resp.text))
            rows = list(reader)
            print(f"    {len(rows)} rows loaded from GitHub (remote fallback)")
        except Exception as e:
            print(f"    ERROR: Could not load records.csv locally or from GitHub: {e}")
            sys.exit(1)

    # --- Load lookups ---
    print(f"\n[3] Loading lookup configs...")
    bbox_data = load_yaml(BBOX_YAML)
    bbox_lookup = bbox_data.get("country_bbox", {})
    print(f"    Bbox entries: {len(bbox_lookup)}")

    geonames_data = load_yaml(GEONAMES_YAML)
    geonames_lookup = geonames_data.get("geonames_country_ids", {})
    print(f"    GeoNames entries: {len(geonames_lookup)}")

    spatial_cfg = load_spatial_config(SPATIAL_YAML)
    fixes = spatial_cfg["country_name_fixes"]
    print(f"    Country name fixes: {len(fixes)}")

    # --- Resolve ISO3 for each row ---
    print(f"\n[4] Resolving ISO3 codes...")
    resolved = []
    failed = []
    iso3_counter = Counter()

    for row in rows:
        country = row["country"]
        iso3 = country_name_to_iso3(country, fixes=fixes)
        if iso3:
            resolved.append({
                "country": country,
                "iso3": iso3,
                "zenodo_id": row["zenodo_deposition_id"],
                "title": row["title"],
                "doi": row["doi"],
                "url": row["url"],
            })
            iso3_counter[iso3] += 1
        else:
            failed.append(country)

    print(f"    Resolved: {len(resolved)}, Failed: {len(failed)}")
    if failed:
        print(f"    FAILED: {failed}")

    # Detect split entries (multiple rows -> same ISO3)
    duplicates = {k for k, v in iso3_counter.items() if v > 1}
    if duplicates:
        print(f"    Split ISO3 codes: {sorted(duplicates)}")

    # --- Fetch Zenodo metadata (file codes + versions) ---
    print(f"\n[5] Fetching file codes & versions from Zenodo API...")
    zenodo_meta = {}  # zenodo_url -> {file_code, version}
    offline = False
    for i, rec in enumerate(resolved, 1):
        url = rec["url"]
        if url not in zenodo_meta:
            if offline:
                zenodo_meta[url] = {"file_code": None, "version": None}
            else:
                meta = fetch_zenodo_metadata(url)
                if meta["file_code"] is None and meta["version"] is None and i <= 2:
                    # First attempts failed — likely offline
                    offline = True
                    print(f"    Zenodo API unreachable — running in offline mode (using ISO3 as file codes)")
                zenodo_meta[url] = meta
        if i % 50 == 0 or i == len(resolved):
            print(f"    {i}/{len(resolved)} queried...")

    # Count mismatches for summary
    fc_mismatches = 0
    for rec in resolved:
        meta = zenodo_meta.get(rec["url"], {})
        fc = meta.get("file_code")
        if fc and fc != rec["iso3"]:
            fc_mismatches += 1

    versions_seen = sorted({m.get("version", "?") for m in zenodo_meta.values() if m.get("version")})
    print(f"    File code != ISO3: {fc_mismatches}")
    print(f"    Versions seen:     {versions_seen}")

    # --- Generate records ---
    print(f"\n[6] Generating records to {OUTPUT_DIR}...")
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    generated = []
    stats = {"missing_bbox": 0, "missing_geonames": 0, "missing_file_code": 0}

    for rec in resolved:
        iso3 = rec["iso3"]
        country = rec["country"]
        bbox = bbox_lookup.get(iso3, [])
        geonames = geonames_lookup.get(iso3)
        meta = zenodo_meta.get(rec["url"], {})
        file_code = meta.get("file_code")
        version = meta.get("version")

        if not bbox:
            stats["missing_bbox"] += 1
        if not geonames:
            stats["missing_geonames"] += 1
        if not file_code:
            stats["missing_file_code"] += 1

        # Build record ID and filename
        base_id = f"rdls_he-{iso3.lower()}_nismod_sdkicra"
        if iso3 in duplicates:
            slug = make_split_slug(country)
            record_id = f"{base_id}__{slug}"
            filename = f"{record_id}.json"
        else:
            record_id = base_id
            filename = f"{base_id}.json"

        record = generate_icra_record(
            template_ds=template_ds,
            iso3=iso3,
            country_name=country,
            zenodo_id=rec["zenodo_id"],
            title=rec["title"],
            doi=rec["doi"],
            url=rec["url"],
            bbox=bbox,
            geonames_entry=geonames,
            record_id=record_id,
            file_code=file_code,
            version=version,
        )

        output_path = OUTPUT_DIR / filename
        write_json(output_path, {"datasets": [record]})
        generated.append(filename)

    # --- Summary ---
    print(f"\n{'=' * 70}")
    print(f"SUMMARY")
    print(f"{'=' * 70}")
    print(f"  Records.csv rows:     {len(rows)}")
    print(f"  ISO3 resolved:        {len(resolved)}")
    print(f"  ISO3 failed:          {len(failed)}")
    print(f"  Files generated:      {len(generated)}")
    print(f"  Missing bbox:         {stats['missing_bbox']}")
    print(f"  Missing GeoNames:     {stats['missing_geonames']}")
    print(f"  Missing file code:    {stats['missing_file_code']}")
    print(f"  File code != ISO3:    {fc_mismatches}")
    print(f"  Versions:             {versions_seen}")
    print(f"  Split entries:        {sum(iso3_counter[k] for k in duplicates)} rows -> {len(duplicates)} ISO3 codes")
    print(f"  Output dir:           {OUTPUT_DIR}")

    if failed:
        print(f"\n  UNRESOLVED COUNTRIES:")
        for f in failed:
            print(f"    - {f}")


if __name__ == "__main__":
    main()
