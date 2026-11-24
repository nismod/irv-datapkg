"""Command line interface: ``adaptive-partitions build|keys|show``."""

from __future__ import annotations

import argparse
import json
import sys

from .core import SCHEMES, build_cells, keys_for_bbox, read_cells, write_cells


def _build(args) -> int:
    cells = build_cells(
        args.raster,
        scheme=args.scheme,
        target=args.target,
        start_level=args.start_level,
        max_level=args.max_level,
        max_population=args.max_population,
        allow_overshoot=args.allow_overshoot,
        grid_level=args.grid_level,
        block_rows=args.block_rows,
    )
    write_cells(cells, args.output)
    pop = cells["population"]
    print(
        f"wrote {len(cells)} {args.scheme} cells to {args.output} "
        f"(levels {cells['level'].min()}-{cells['level'].max()}, "
        f"population total {pop.sum():,.0f}, max {pop.max():,.0f}, "
        f"median {pop.median():,.0f})",
        file=sys.stderr,
    )
    return 0


def _keys(args) -> int:
    cells = read_cells(args.cells)
    for k in keys_for_bbox(cells, args.bbox, buffer=args.buffer):
        print(k)
    return 0


def _show(args) -> int:
    cells = read_cells(args.cells)
    print(json.dumps(cells.attrs.get("adaptive_partitions", {}), indent=2))
    cols = ["key", "level", "population", "xmin", "ymin", "xmax", "ymax"]
    print(cells[cols].sort_values("population", ascending=False).to_string(index=False))
    return 0


def main(argv=None) -> int:
    p = argparse.ArgumentParser(
        prog="adaptive-partitions",
        description="Population-balanced adaptive spatial partition keys.",
    )
    sub = p.add_subparsers(dest="command", required=True)

    b = sub.add_parser("build", help="build cells from a population raster")
    b.add_argument("raster", help="population raster (EPSG:4326 or projected, e.g. Mollweide)")
    b.add_argument("-o", "--output", required=True, help="output GeoParquet path")
    b.add_argument("--scheme", choices=SCHEMES, default="quadkey")
    b.add_argument("--target", type=int, default=200, help="maximum number of cells (default 200)")
    b.add_argument("--start-level", type=int, default=None,
                   help="starting level/resolution (quadkey default 1, h3 default 0)")
    b.add_argument("--max-level", type=int, default=None,
                   help="finest level/resolution (quadkey default = grid level, h3 default 4)")
    b.add_argument("--max-population", type=float, default=None,
                   help="also stop once no cell exceeds this population")
    b.add_argument("--allow-overshoot", action="store_true",
                   help="let the final split exceed --target")
    b.add_argument("--grid-level", type=int, default=None,
                   help="aggregation grid level (default 10 quadkey, 11 h3)")
    b.add_argument("--block-rows", type=int, default=512,
                   help="raster rows read per block (memory control)")
    b.set_defaults(func=_build)

    k = sub.add_parser("keys", help="list keys intersecting a bbox")
    k.add_argument("cells", help="cells GeoParquet")
    k.add_argument("--bbox", type=float, nargs=4, required=True,
                   metavar=("XMIN", "YMIN", "XMAX", "YMAX"))
    k.add_argument("--buffer", type=float, default=0.0)
    k.set_defaults(func=_keys)

    s = sub.add_parser("show", help="print build metadata and cells")
    s.add_argument("cells")
    s.set_defaults(func=_show)

    args = p.parse_args(argv)
    return args.func(args)


if __name__ == "__main__":
    raise SystemExit(main())
