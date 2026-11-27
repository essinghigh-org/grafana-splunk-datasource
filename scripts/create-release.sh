#!/usr/bin/env bash

set -euo pipefail

tag=${GITHUB_REF_NAME:?A release tag is required}
if [[ ! "$tag" =~ ^v(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)$ ]]; then
  printf 'Invalid release tag: %s\n' "$tag" >&2
  exit 1
fi

version=${tag#v}
package_version=$(node -p 'require("./package.json").version')
plugin_version=$(node -p 'require("./dist/plugin.json").info.version')
if [ "$version" != "$package_version" ] || [ "$version" != "$plugin_version" ]; then
  printf 'Tag, package version and built plugin version must match.\n' >&2
  exit 1
fi
if [ "$(git rev-parse "refs/tags/${tag}^{commit}")" != "$(git rev-parse HEAD)" ]; then
  printf 'Tag %s does not point to the checked-out commit.\n' "$tag" >&2
  exit 1
fi

./scripts/package-plugin.sh "$tag"
plugin_id=$(node -p 'require("./dist/plugin.json").id')

gh release create "$tag" --verify-tag \
  "packages/$plugin_id-$tag-linux.tar.gz" \
  "packages/$plugin_id-$tag-windows.zip" \
  "packages/$plugin_id-$tag-darwin.tar.gz" \
  --title "Release $tag" \
  --generate-notes
