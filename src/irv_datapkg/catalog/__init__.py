"""irv-datapkg catalogue: contract in docs/catalogue-spec.md."""

from .client import Asset, Catalog, Item
from .lock import ABSENT, CatalogError, ChecksumError, LockDiff, LockFile, RetiredError
from .extract import extract
from .producer import (
    CatalogWriter,
    ImmutableError,
    build_collection,
    build_item,
    describe_raster,
    describe_vector,
    git_commit,
)
from .validate import check, validate_catalog

__all__ = [
    "ABSENT",
    "Asset",
    "Catalog",
    "CatalogError",
    "CatalogWriter",
    "ChecksumError",
    "ImmutableError",
    "Item",
    "LockDiff",
    "LockFile",
    "RetiredError",
    "build_collection",
    "build_item",
    "check",
    "describe_raster",
    "describe_vector",
    "extract",
    "git_commit",
    "validate_catalog",
]
