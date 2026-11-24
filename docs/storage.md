# Storage, publishing and releases

Status: draft for WP4. Implements [ADR 0002](adr/0002-storage-and-access.md) and
[ADR 0003](adr/0003-catalogue-and-releases.md). The catalogue layout is defined in
[catalogue-spec.md](catalogue-spec.md).

Things marked **ASSUMPTION** have not been confirmed with source.coop or Oxford IT.
They are listed together in [section 9](#9-assumptions-to-confirm).

## 1. Where things live

| Tier | Where | Who can read |
| --- | --- | --- |
| T0 raw downloads | Oxford drive, `{workspace_dir}/raw/` | project members |
| Builds (intermediate, scratch) | Oxford drive, `{workspace_dir}/build/` | project members |
| T1+ local catalogue (source of truth for publishing) | Oxford drive, `{workspace_dir}/catalogue/irv-datapkg/` | project members |
| Private catalogue | Oxford drive, `{workspace_dir}/catalogue/irv-datapkg-private/` | project members |
| Public catalogue | source.coop, `{BASE}` | anyone, no credentials |
| Private catalogue (published) | source.coop, `{BASE}-private` | named collaborators, with credentials |
| Retired versions | Oxford tape (cold) storage | on request |

The Oxford drive is the working space. Everything is built there, and the
local catalogue is written there with `CatalogWriter`. Nothing is built on
source.coop: `irv-datapkg publish` copies the finished local catalogue up.

### Oxford drive layout

```
{workspace_dir}/                       # config key workspace_dir
  raw/{dataset}/{upstream_version}/     # T0: files as downloaded, never edited
  build/{dataset}/...                   # scratch, safe to delete
  catalogue/
    irv-datapkg/                        # config key catalogue_base (= local {BASE})
      catalog.json
      releases/YYYY.N/catalog.json
      {dataset}/collection.json
      {dataset}/{version}/{item}.json|.tif|.parquet
    irv-datapkg-private/                # config key catalogue_private_base
      catalog.json
      {dataset}/...
  reports/                              # publish summaries, retire and link check reports
```

T0 raw data is kept even for public datasets: it is what a new processing
revision (`-r2`) is rebuilt from, and some upstream sources disappear. Raw data
is not published.

### Remote layout

The remote is the same tree with a different `{BASE}`:

```
{BASE}         = s3://nismod/irv-datapkg            (ASSUMPTION: account nismod, repository irv-datapkg)
                 https://data.source.coop/nismod/irv-datapkg      (HTTP, read-only)
{BASE}-private = s3://nismod/irv-datapkg-private    (ASSUMPTION: second, private repository)
```

S3 clients use the source.coop data proxy as their endpoint
(`endpoint_url = https://data.source.coop`), with the source.coop account as
bucket and the repository as the first part of the key. The public catalogue
is also readable over plain HTTPS, which the catalogue client supports:

```python
from irv_datapkg.catalog import Catalog
cat = Catalog.open("https://data.source.coop/nismod/irv-datapkg", release="2026.1")
```

source.coop also exposes the underlying AWS bucket directly
(`s3://us-west-2.opendata.source.coop/{account}/{repository}/`). Use the data
proxy unless source.coop advises otherwise.

Because `{BASE}-private` is the string `{BASE}` plus `-private`, a private
repository named `irv-datapkg-private` in the same account keeps the
client's default rule working without configuration.

## 2. Configuration

These keys go in `config.json`, or in a YAML file passed with `--configfile`.
See [`config/storage.example.yaml`](../config/storage.example.yaml).

| Key | Default | Meaning |
| --- | --- | --- |
| `workspace_dir` | `.` | Oxford drive working space |
| `catalogue_base` | `{workspace_dir}/catalogue/irv-datapkg` | local public catalogue |
| `catalogue_private_base` | `{catalogue_base}-private` | local private catalogue |
| `publish_remote` | none (required to publish) | remote `{BASE}`, e.g. `s3://nismod/irv-datapkg` |
| `publish_private_remote` | `{publish_remote}-private` | remote `{BASE}-private` |
| `publish_private` | `true` | also publish the private root |
| `publish_storage_options` | `{}` | fsspec/s3fs options, e.g. `{endpoint_url: https://data.source.coop}` |

Credentials are never config. s3fs reads them from `AWS_ACCESS_KEY_ID`,
`AWS_SECRET_ACCESS_KEY` and `AWS_SESSION_TOKEN`, or from an AWS profile
(`publish_storage_options: {profile: source-coop}`).

The workflow rules in [`rules/publish.smk`](../rules/publish.smk) are:

| Rule | Does |
| --- | --- |
| `catalogue_publish_dry_run` | validate the local catalogue, report what would be uploaded |
| `catalogue_publish` | validate, then publish (section 3) |
| `catalogue_release` | write a release manifest locally (section 5) |
| `catalogue_retire_report` | write `reports/retire-DATE.csv` (section 6) |
| `linkcheck` | write `reports/linkcheck-DATE.csv` (section 7) |

They are prefixed `catalogue_` because `rules/zenodo.smk` already has a rule
called `publish`. None of them declares outputs, so they run every time you
ask for them by name, e.g. `snakemake -c1 catalogue_publish_dry_run`.

## 3. Publishing

```bash
irv-datapkg validate /path/to/catalogue/irv-datapkg --check-files
irv-datapkg publish /path/to/catalogue/irv-datapkg s3://nismod/irv-datapkg \
    --endpoint-url https://data.source.coop --dry-run
irv-datapkg publish /path/to/catalogue/irv-datapkg s3://nismod/irv-datapkg \
    --endpoint-url https://data.source.coop --report reports/publish.json
```

The same from Python: `irv_datapkg.publish.publish(local, remote, ...)`.

What `publish` does:

1. Lists the local public root and the local private root (`--private-base`,
   default `{local}-private`). The private root goes to `--private-remote`
   (default `{remote}-private`). `--no-private` skips it.
2. Refuses to run if a collection's `irv:access` does not match the root it is
   under, or if the public and private remotes overlap.
3. Compares each local file with the remote:
   - not on the remote: **upload**;
   - same size and sha256: **skip**;
   - `collection.json` or root `catalog.json` that differs: **update** (the
     only mutable documents);
   - any other file that differs: **conflict**. Published versions and
     releases are immutable, so a conflict means a new revision is needed.
4. If there is any conflict, it writes nothing and exits with an error. With
   `--dry-run` it reports conflicts and exits 1.
5. Uploads in tiers: data files, then item JSON, then collection JSON, then
   release manifests, then root `catalog.json`. Each tier is done for the
   private root and then the public root. A reader of the remote never
   follows a link to something that has not arrived, and a public release
   never names a private dataset that has not arrived.
6. Never deletes anything from the remote.

Checking sha256 of a remote data file means reading it. To avoid
re-downloading terabytes on every publish, a data file of the same size
counts as identical when the item JSON that records its checksum is already
on the remote and identical to the local one. Items are uploaded only after
their data, so an item on the remote marks its data file as complete.
`--verify-remote` hashes every same-size remote data file instead. Run it
after a failed or interrupted upload, or once a year.

An interrupted publish is safe to re-run. Files that were uploaded are
skipped, and the catalogue documents that link to them go up last.

## 4. Access and authentication

**Public prefix.** Anonymous, read-only: plain HTTPS, or S3 with no
credentials (`--no-sign-request`, `anon=True`). Only datasets whose licence
allows redistribution go here. Check the licence before setting
`irv:access: public` (ADR 0002).

**Private prefix.** Authenticated read, for datasets we may use but not
redistribute (IRIS, CHAZ, Emanuel tracks, unreleased NbS layers).

- Collaborators get read access to the private repository from the
  source.coop organisation. ASSUMPTION: source.coop supports private
  repositories with per-member read access for our account. If it does not,
  the private root stays on the Oxford drive only and `{BASE}-private` points
  there (the client's `private_base=` override). Collaborators outside Oxford
  then get files through Oxford's own sharing.
- Credentials are short-lived and scoped to a repository (source.coop
  "View credentials" on the repository page). Export them as environment
  variables or put them in an AWS profile. Never commit them, and never put
  them in config or CI secrets for this repository.

**Publishing rights.** Write access to both source.coop repositories and to
the Oxford drive project area is granted by **Tom Russell**, as owner of
the source.coop organisation and the project area. Ask him for publishing
rights. Keep the number of people with write access small: only people
running releases need it. The monthly link check in CI needs no credentials.

## 5. Six-monthly release procedure

Releases are `YYYY.1` (end of June) and `YYYY.2` (end of December), plus extra
releases when needed. A release only writes `releases/YYYY.N/catalog.json`,
which pins one version of each dataset. Nothing is copied.

1. **Announce a freeze** two weeks ahead (issue in this repository). List the
   datasets to add or bump, and their target versions.
2. **Build and add** each new or bumped version on the Oxford drive with
   `CatalogWriter.add_item` (WP5 rules). Bumps are new revisions (`-rN+1`).
   Published versions are never edited.
3. **Validate** the local catalogue, checking every file:
   `irv-datapkg validate {catalogue_base} --check-files`.
4. **Publish the new versions** before the release:
   `snakemake -c1 catalogue_publish_dry_run`, review the report, then
   `snakemake -c1 catalogue_publish`. New versions become visible as
   "latest" but are not yet pinned by any release.
5. **Smoke-test consumers** (open-gira, infra-risk-vis) against the published
   catalogue with latest versions. Fix any problems with a new revision and
   repeat steps 2 to 4. Do this before the release, because a release
   cannot be changed afterwards.
6. **Write the release**, starting from the previous one:
   ```bash
   snakemake -c1 catalogue_release --config release=2027.1 release_from=2026.2 \
       release_pins=aqueduct_flood=v2-r2,storm_fixed_rp=1.0.0-r1
   # or: irv-datapkg release {catalogue_base} 2027.1 --from 2026.2 --pin aqueduct_flood=v2-r2
   ```
   Without `release_from`, every dataset is pinned to its latest version.
   Quote names whose `N` is 10 or more (`release='"2027.10"'`): snakemake
   parses `--config` values as YAML, so `2027.10` would become `2027.1`.
7. **Validate and publish again**. Only the release manifest and the root
   `catalog.json` change, and the manifest goes up before the root.
8. **Check the remote**: `irv-datapkg validate https://data.source.coop/nismod/irv-datapkg`,
   then `irv-datapkg lock --base ... --release 2027.1` in a scratch directory.
9. **Tag and announce**. Make a git tag `release-2027.1` on the commit that
   produced the manifest. Write release notes from the diff of
   `irv:datasets` against the previous release, and open PRs in consumer
   repositories to update their `datapkg.lock.yaml`.
10. **Run the retirement report** (section 6) and the link check (section 7).

## 6. Retiring versions to tape

A version moves to cold (tape) storage when **no release made in the last year
pins it**. `irv-datapkg retire` (or `snakemake -c1 catalogue_retire_report`)
lists every version and whether it is eligible:

```bash
irv-datapkg retire {catalogue_base} -o reports/retire-2027-07-01.csv
```

A version is eligible when all of these are true:

- it is not the latest version of its dataset;
- it is not pinned by the most recent release, however old that release is;
- no release created in the last 365 days (`--keep-days`) pins it;
- its newest asset `irv:retrieved` date is over 365 days old. This is a grace
  period for versions superseded before any release pinned them.

The report gives file count and bytes per version, so you can see how much tape
is needed. Versions already retired are listed with their retirement date
(`retired` column) and are never eligible. To retire, after each release:

1. Run the report and review the eligible versions. Search consumer
   repositories' lock files for each version string. A version still locked
   by a consumer that is being maintained stays online, and the consumer is
   asked to move to a newer release.
2. Copy `{dataset}/{version}/` (data and item JSON) from the Oxford drive to
   tape. Write a `SHA256SUMS` file next to it, and check that a test restore
   of one file matches.
3. Mark the version as retired in the local catalogue:

   ```bash
   irv-datapkg retire {catalogue_base} --mark aqueduct_flood=v2-r1 \
       --note "On Oxford tape; ask the irv-datapkg maintainers to restore" \
       --location "tape LTO-0042"
   ```

   or `CatalogWriter(base).retire(dataset, version, note, location=...)`. This
   adds `{version: {"date", "note", "location"}}` to the collection's
   `irv:retired` (catalogue spec section 7). It refuses the latest version and
   a version pinned by the most recent release.
4. Publish (section 3). Only `collection.json` changes. From then on, clients
   refuse to fetch the version with an error that gives the date and note;
   `irv-datapkg release` refuses to pin it; consumers'
   `irv-datapkg lock --check` reports their lock as stale.
5. Remove the **data files** of that version from source.coop
   (`aws s3 rm s3://nismod/irv-datapkg/{dataset}/{version}/ --recursive
   --exclude "*.json" --endpoint-url https://data.source.coop`) and from the
   Oxford drive catalogue. Keep the item JSON and the collection's
   `irv:versions` entry, so citations and old release manifests still resolve
   to metadata. `validate` does not report missing files of retired versions.
6. Add a line to the retirement log (`reports/`), giving dataset, version,
   date and tape location.
7. To restore on request, copy the files back to the same paths, run
   `CatalogWriter(base).unretire(dataset, version)` and publish. The content
   is identical, so `publish` accepts it.

`publish` never deletes, and it only uploads files that exist locally. It also
does not re-upload data files of a retired version that are missing on the
remote (reported as `retired` in the summary), so step 5 can be done in
either order on the two copies.

## 7. Monthly link check

[`ci/linkcheck.yml`](../ci/linkcheck.yml) is a GitHub Actions workflow. Copy it to
`.github/workflows/linkcheck.yml` to enable it (automated pushes here cannot
add workflow files). It runs on the 1st of each month, and on demand. It runs
`irv-datapkg linkcheck --datasets-csv docs/datasets.csv` and uploads
`linkcheck-report.csv` as an artifact, kept for 90 days. A table of failures
goes in the job summary. The job fails when any URL fails, so that GitHub
sends a notification. It needs no secrets.

Locally, also check the catalogue's `irv:upstream_url` values:

```bash
irv-datapkg linkcheck --datasets-csv docs/datasets.csv --base {catalogue_base} -o reports/linkcheck.csv
# or: snakemake -c1 linkcheck
```

Each upstream entry is turned into URLs as follows:

- `http(s)` URLs are checked as given;
- DOIs (`Zenodo 10.5281/...`, `doi:10.5061/...`) are checked at `https://doi.org/`;
- `s3://bucket/prefix` becomes a one-key list request on the bucket's public
  endpoint;
- free text (`Oxford (not public)`) is reported as skipped.

Each URL gets a HEAD request, falling back to GET when the server rejects
HEAD with 403, 405 or 501. Any status below 400 after redirects counts as
OK. A failure is a prompt to check by hand, not proof that the data has
gone, because some portals block automated requests. Replace descriptive
upstream entries in `docs/datasets.csv` with URLs where possible, so that they
can be checked.

## 8. Mirroring

Anyone can mirror the public catalogue read-only with no credentials. All
hrefs are relative, so a mirror works as a `{BASE}` as soon as it is
complete.

**rclone**, in publish order so readers of the mirror never see dangling links:

```bash
scripts/mirror_catalogue.sh /data/irv-datapkg               # from source.coop
scripts/mirror_catalogue.sh othersite:bucket/irv-datapkg    # to another S3 site
irv-datapkg validate /data/irv-datapkg --check-files
```

The script sets up an anonymous rclone S3 remote for
`https://data.source.coop`:

```ini
[sourcecoop]
type = s3
provider = Other
endpoint = https://data.source.coop
env_auth = false
```

**AWS CLI**, for a one-off copy where ordering does not matter:

```bash
aws s3 sync s3://nismod/irv-datapkg /data/irv-datapkg \
    --endpoint-url https://data.source.coop --no-sign-request
```

**One release only**: open the release with the client and fetch what it pins,
e.g. `irv-datapkg fetch DATASET --base https://data.source.coop/nismod/irv-datapkg --release 2026.1 -o ...`.

Point consumers at a mirror with `--base /data/irv-datapkg`, or by
`base:` in their lock file. The checksums in the lock file still apply. A
site with its own S3 store can also use `irv-datapkg publish` to push from an
Oxford drive copy to its own bucket. It gets the same ordering and
immutability checks.

Private mirrors need source.coop credentials for the private repository,
and the mirror must be under the same access controls. Use an authenticated
rclone remote and `scripts/mirror_catalogue.sh sourcecoop-auth:nismod/irv-datapkg-private DEST`.

## 9. Assumptions to confirm

With **source.coop**:

1. Account and repository names: `nismod/irv-datapkg` and
   `nismod/irv-datapkg-private`.
2. S3 addressing through the data proxy: endpoint `https://data.source.coop`,
   bucket = account, key = `{repository}/...`. Anonymous `ListObjects` must
   work there for `aws s3 sync --no-sign-request` and rclone.
3. Private repositories: whether they are offered, who can be given read
   access, and whether credentials can be scoped to read-only.
4. Credentials: repository-scoped, short-lived keys from the web UI. Is
   there a way to get keys for automated publishing from the Oxford machine
   (for example a long-lived key or an API token)?
5. Quotas: total storage (our estimate is 2 to 5 TB public T1 in the first
   year, growing by about 1 TB per release cycle, all rough), maximum
   object size (largest single COG is probably 10 to 50 GB; s3fs uses
   multipart uploads), request-rate limits, and the egress policy for heavy
   users such as other sites' mirrors.
6. Cost: we assume public open data is hosted free of charge
   (source.coop's open data model). If not, budget storage at S3 standard
   rates (about $23 per TB per month in us-west-2) plus egress.
7. Deleting objects (needed for retirement, section 6) is allowed and does
   not break any source.coop indexing.
8. Whether source.coop needs per-repository metadata such as a README or
   licence. The repository holds many datasets with different licences,
   each recorded in its `collection.json`.

With **Oxford IT / the project area owner**:

9. The mount path of the project share (`workspace_dir`), its quota (T0
   plus builds plus both catalogues, probably 3 to 4 times the public
   catalogue size) and its backup policy.
10. The name, cost and retrieval time of the tape (cold) service, and
    whether it can be written from the same machine.
