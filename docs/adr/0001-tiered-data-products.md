# 0001 Tiered data products

Status: accepted

## Context

open-gira, infra-risk-vis and irv-datapkg each download and pre-process global
open datasets. At least ten datasets are fetched by two or three of them, with
drifting versions and little provenance capture.

## Decision

irv-datapkg produces three tiers:

- **T0 raw mirror**: verbatim upstream files with sha256 checksums, retrieval
  date and source URL. Kept on the Oxford drive workspace, never published.
- **T1 analysis-ready**: one standardised global layer per dataset variant.
  Rasters keep the source CRS and grid: no reprojection or resampling at T1.
  Clients reproject on demand, which is cheap for small extracts.
- **T2 extracts**: per-region clips of T1 (today's country starter kits).

A future **T3** may reproject everything to a discrete global grid system
(DGGS). It is out of scope for now.

## Consequences

- Only irv-datapkg talks to upstream hosts. Consumers never hard-code upstream URLs.
- Consumers own reprojection, Web Mercator clipping and anything analysis- or
  visualisation-specific.
