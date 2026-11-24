#
# Shared workflow setup: workspace paths, dataset metadata, T1 item registry
#
# Workspace layout (config.json keys, see docs/recipes.md):
#
#   {raw_dir}/{dataset}/{upstream}/...                T0 raw mirror + provenance sidecars
#   {build_dir}/{dataset}/{version}/...               intermediate files (not published)
#   {catalogue_base}/{dataset}/{version}/{item}.*     T1 files + STAC items (local catalogue)
#   {catalogue_base}-private/...                      T1 for private datasets
#   data/{ISO3}/...                                   T2 country packages (Zenodo)
#
import tempfile
from functools import lru_cache

from irv_datapkg import recipes
from irv_datapkg.recipes import T1Item


def _config_path(key, default):
    value = str(config.get(key) or default)
    return value.format(workspace_dir=WORKSPACE).rstrip("/")


WORKSPACE = str(config.get("workspace_dir", "workspace")).rstrip("/")
RAW = _config_path("raw_dir", "{workspace_dir}/raw")
BUILD = _config_path("build_dir", "{workspace_dir}/build")
CATALOGUE = _config_path("catalogue_base", "{workspace_dir}/catalogue")

PACKAGES = recipes.load_metadata("metadata")
COLLECTIONS = recipes.collections_by_id(PACKAGES)

# dataset id -> function() returning {item_id: T1Item}; filled by each recipe
T1_ITEMS = {}
# T2 package name -> function(iso3) returning extra T2 files (docs, boundary)
T2_EXTRA = {}


def register_t1(dataset, items_fn):
    """Declare the T1 items of a dataset (a function returning {item_id: T1Item})."""
    if dataset not in COLLECTIONS:
        raise ValueError(f"{dataset} has no collection in metadata/*.yml")
    T1_ITEMS[dataset] = items_fn


def raw_dir(dataset):
    """T0 raw mirror directory: {raw_dir}/{dataset}/{upstream version}"""
    return f"{RAW}/{dataset}/{COLLECTIONS[dataset].upstream_dir}"


def build_dir(dataset):
    """Scratch directory for intermediate files: {build_dir}/{dataset}/{version}"""
    return f"{BUILD}/{dataset}/{COLLECTIONS[dataset].version}"


def build_tmpdir(dataset):
    """A temporary directory under build_dir (same filesystem as the outputs)."""
    path = build_dir(dataset)
    os.makedirs(path, exist_ok=True)
    return tempfile.TemporaryDirectory(dir=path, prefix=".tmp-")


def t1_dir(dataset):
    """T1 directory in the local catalogue: {catalogue_base}/{dataset}/{version}"""
    return recipes.item_dir(CATALOGUE, COLLECTIONS[dataset])


def t1_items(dataset):
    return T1_ITEMS[dataset]()


def t1_item(dataset, item_id):
    try:
        return t1_items(dataset)[item_id]
    except KeyError:
        raise ValueError(f"{dataset} declares no T1 item {item_id}") from None


def t1_data(dataset, item_id):
    spec = t1_item(dataset, item_id)
    return recipes.item_data_path(CATALOGUE, COLLECTIONS[dataset], item_id, spec.ext)


def t1_json(dataset, item_id):
    return recipes.item_json_path(CATALOGUE, COLLECTIONS[dataset], item_id)


def t1_targets(dataset):
    """Every T1 item JSON of a dataset (triggers checkpoints where needed)."""
    return [t1_json(dataset, item_id) for item_id in sorted(t1_items(dataset))]


def provenance_cmd(
    output, files, tool, url_prefix=None, source_url=None, urls_from=None
):
    """Shell snippet writing a T0 provenance sidecar (see irv-datapkg provenance)."""
    args = [f"irv-datapkg provenance -o {output} --tool '{tool}'"]
    if url_prefix:
        args.append(f"--url-prefix '{url_prefix}'")
    if source_url:
        args.append(f"--source-url '{source_url}'")
    if urls_from:
        args.append(f"--urls-from {urls_from}")
    return " ".join(args + [files])
