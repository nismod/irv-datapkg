# 0002 Storage and access

Status: accepted

## Decision

- The **Oxford drive** is the primary working space: T0, builds and private data.
- **T1 onwards** is published to a public bucket on **source.coop**
  (to confirm options and quotas with source.coop).
- Non-public datasets (e.g. IRIS, CHAZ, Emanuel tracks, unreleased NbS layers)
  are siloed under a separate top-level `-private` prefix with the same layout.
- The Oxford drive and the source.coop organisation are authenticated.
  Contact Tom Russell for publishing rights.
- Retired dataset versions move to cold (tape) storage after one year.
- A monthly link check runs against upstream URLs.

## Consequences

- All paths are relative to one base URI (`{BASE}`), so local folders, the
  Oxford drive and source.coop differ only in that base.
- Licences must be checked per dataset before it goes in the public prefix.
