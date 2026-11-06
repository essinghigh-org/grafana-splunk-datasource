#!/usr/bin/env bash

set -euo pipefail

commit_hash=$(git rev-parse --short HEAD)
commit_message=$(git log -1 --pretty=%B)
release_date=$(date -u '+%Y-%m-%d')

if [ -f CHANGELOG.md ] && grep -q "^## $commit_hash (" CHANGELOG.md; then
  exit 0
fi

changelog_tmp=$(mktemp ./CHANGELOG.md.XXXXXX)
trap 'rm -f "$changelog_tmp"' EXIT
{
  printf '## %s (%s)\n\n%s\n\n' "$commit_hash" "$release_date" "$commit_message"
  if [ -f CHANGELOG.md ]; then
    cat CHANGELOG.md
  else
    printf '# Changelog\n'
  fi
} > "$changelog_tmp"
chmod 0644 "$changelog_tmp"
mv "$changelog_tmp" CHANGELOG.md
printf 'Updated changelog with entry for %s.\n' "$commit_hash"
