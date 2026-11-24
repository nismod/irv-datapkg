# Architecture decision records

Decisions for consolidating global open-data preparation in irv-datapkg, so
that open-gira and the infra-risk-vis ETL start from one published catalogue.
They record the outcome of the design discussion (November 2026).

| ADR | Decision |
| --- | --- |
| [0001](0001-tiered-data-products.md) | Three tiers of data product; T1 keeps the source grid |
| [0002](0002-storage-and-access.md) | Oxford drive workspace, public source.coop store, private prefix |
| [0003](0003-catalogue-and-releases.md) | Static STAC catalogue, immutable versions, six-monthly release manifests |
| [0004](0004-file-formats.md) | COG, GeoParquet, Zarr; adaptive spatial partitioning |
| [0005](0005-spatial-partition-key.md) | Partition key as its own small library: quadkey default, H3 alongside |
| [0006](0006-boundaries-and-extracts.md) | Boundary sets side by side; client-built T2 extracts; Zenodo continues |
| [0007](0007-producer-consumer-scope.md) | What moves to irv-datapkg, what stays in consumers, what is dropped |
| [0008](0008-naming-and-retirements.md) | Keep the irv-datapkg name; retire irv-autopkg |

Dataset ids are listed in [`../datasets.csv`](../datasets.csv).

Each ADR uses the format: Status, Context, Decision, Consequences.
