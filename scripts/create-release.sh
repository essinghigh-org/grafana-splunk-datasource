#!/usr/bin/env bash

set -euo pipefail

commit_hash=$(git rev-parse --short HEAD)
full_commit_hash=$(git rev-parse HEAD)

if tag_commit=$(git rev-parse --verify --quiet "refs/tags/$commit_hash^{commit}"); then
  if [ "$tag_commit" != "$full_commit_hash" ]; then
    printf 'Tag %s points to a different commit; refusing to publish.\n' "$commit_hash" >&2
    exit 1
  fi
  if gh release view "$commit_hash" >/dev/null 2>&1; then
    printf 'Release %s already exists, skipping release creation.\n' "$commit_hash"
    exit 0
  fi
fi

./scripts/update-changelog.sh
./scripts/package-plugin.sh "$commit_hash"
plugin_id=$(node -p 'require("./dist/plugin.json").id')

release_notes=$(mktemp "${TMPDIR:-/tmp}/grafana-plugin-release.XXXXXX")
trap 'rm -f "$release_notes"' EXIT
cat > "$release_notes" <<EOF
Release $commit_hash

Commit: $full_commit_hash

### Changes
$(git log -1 --pretty=%B)
EOF

gh release create "$commit_hash" \
  "packages/$plugin_id-$commit_hash.zip" \
  "packages/$plugin_id-$commit_hash.tar.gz" \
  --target "$full_commit_hash" \
  --title "Release $commit_hash" \
  --notes-file "$release_notes" \
  --generate-notes

printf 'Release %s created successfully.\n' "$commit_hash"
