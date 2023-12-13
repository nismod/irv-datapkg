from pathlib import Path

import geopandas


def read_boundaries(base_path: Path) -> geopandas.GeoDataFrame:
    return geopandas.read_file(
        base_path / "bundled_data" / "ne_10m_admin_0_map_units_custom.gpkg"
    )
