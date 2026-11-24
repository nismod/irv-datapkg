"""Adaptive, population-balanced spatial partition keys for Hive-partitioned GeoParquet."""

__version__ = "0.1.0"

from .core import (  # noqa: E402
    SCHEMES,
    assign,
    build_cells,
    cells_from_keys,
    keys_for_bbox,
    read_cells,
    write_cells,
)
from .population import PopulationGrid, aggregate_population  # noqa: E402

__all__ = [
    "SCHEMES",
    "PopulationGrid",
    "aggregate_population",
    "assign",
    "build_cells",
    "cells_from_keys",
    "keys_for_bbox",
    "read_cells",
    "write_cells",
]
