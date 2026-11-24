# 0006 Boundaries and extracts

Status: accepted

## Decision

- T1 publishes several boundary sets **side by side**, as published by their
  sources: Natural Earth, GADM, geoBoundaries, and an EEZ marine boundaries set.
- The custom Natural Earth map units used today solve how to extract compact
  subsets for nations. They are replaced by an explicit **nation/region
  relation**, preferably built from EEZ marine boundaries, with outlying
  islands as separate regions. This is a T2 concern.
- Zenodo T2 country packages continue. The catalogue client gains an
  `extract` capability; the packaging workflow uses the client, then uploads
  to Zenodo.
