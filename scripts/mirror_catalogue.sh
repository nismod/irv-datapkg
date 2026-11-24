#!/usr/bin/env bash
# Mirror the public irv-datapkg catalogue (read-only, no credentials) with rclone,
# in the same order as `irv-datapkg publish`: data, items, collections, releases,
# root. Anyone reading the mirror while it updates never sees a dangling link.
#
#   scripts/mirror_catalogue.sh [SRC] DEST
#
# SRC defaults to the source.coop data proxy (ASSUMPTION, see docs/storage.md):
#   sourcecoop:nismod/irv-datapkg
# DEST is any rclone destination, e.g. /data/irv-datapkg or othersite:bucket/irv-datapkg.
# For the private root, configure an authenticated rclone remote and pass
# SRC=sourcecoop-auth:nismod/irv-datapkg-private explicitly.
set -euo pipefail

if [ $# -eq 1 ]; then
  SRC="sourcecoop:nismod/irv-datapkg"
  DEST="$1"
elif [ $# -eq 2 ]; then
  SRC="$1"
  DEST="$2"
else
  sed -n '2,13p' "$0"
  exit 2
fi

# Anonymous S3 remote for the source.coop data proxy, unless already configured
export RCLONE_CONFIG_SOURCECOOP_TYPE="${RCLONE_CONFIG_SOURCECOOP_TYPE:-s3}"
export RCLONE_CONFIG_SOURCECOOP_PROVIDER="${RCLONE_CONFIG_SOURCECOOP_PROVIDER:-Other}"
export RCLONE_CONFIG_SOURCECOOP_ENDPOINT="${RCLONE_CONFIG_SOURCECOOP_ENDPOINT:-https://data.source.coop}"
export RCLONE_CONFIG_SOURCECOOP_ENV_AUTH="${RCLONE_CONFIG_SOURCECOOP_ENV_AUTH:-false}"

OPTS=(--transfers 8 --checkers 16 --fast-list ${RCLONE_EXTRA_OPTS:-})

echo "1/5 data files" >&2
# Published data are immutable: size is enough to skip what is already there.
rclone copy "$SRC" "$DEST" "${OPTS[@]}" --size-only \
  --filter "- /catalog.json" \
  --filter "- /*/collection.json" \
  --filter "- /*/*/*.json" \
  --filter "+ **"

echo "2/5 item JSON" >&2
rclone copy "$SRC" "$DEST" "${OPTS[@]}" --size-only \
  --filter "- /releases/**" \
  --filter "+ /*/*/*.json" \
  --filter "- **"

# Collections and root catalogues are mutable and small: always re-copy them.
echo "3/5 collections" >&2
rclone copy "$SRC" "$DEST" "${OPTS[@]}" --ignore-times \
  --filter "+ /*/collection.json" \
  --filter "- **"

echo "4/5 release manifests" >&2
rclone copy "$SRC" "$DEST" "${OPTS[@]}" --size-only \
  --filter "+ /releases/*/catalog.json" \
  --filter "- **"

echo "5/5 root catalogue" >&2
rclone copy "$SRC" "$DEST" "${OPTS[@]}" --ignore-times \
  --filter "+ /catalog.json" \
  --filter "- **"

echo "done; verify with: irv-datapkg validate $DEST --check-files" >&2
