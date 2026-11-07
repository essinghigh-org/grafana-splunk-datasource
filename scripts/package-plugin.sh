#!/usr/bin/env bash

set -euo pipefail

if [ "$#" -gt 1 ]; then
  printf 'Usage: %s [version-or-commit]\n' "$0" >&2
  exit 2
fi

for file in plugin.json module.js LICENSE; do
  if [ ! -f "dist/$file" ]; then
    printf 'Missing dist/%s. Build the plugin before packaging it.\n' "$file" >&2
    exit 1
  fi
done

plugin_id=$(node -e '
  const plugin = require("./dist/plugin.json");
  if (!/^[a-z0-9]+(?:-[a-z0-9]+)*-(?:app|panel|datasource)$/.test(plugin.id)) {
    throw new Error("Invalid plugin ID");
  }
  process.stdout.write(plugin.id);
')
backend_executable=$(node -e '
  const plugin = require("./dist/plugin.json");
  if (plugin.backend) {
    if (typeof plugin.executable !== "string" || !/^[a-zA-Z0-9_-]+$/.test(plugin.executable)) {
      throw new Error("Invalid backend executable name");
    }
    process.stdout.write(plugin.executable);
  }
')
version=${1:-$(node -p 'require("./package.json").version')}
if [[ ! "$version" =~ ^[a-zA-Z0-9][a-zA-Z0-9._-]*$ ]]; then
  printf 'Invalid package version or commit: %s\n' "$version" >&2
  exit 2
fi

staging_dir=$(mktemp -d "${TMPDIR:-/tmp}/${plugin_id}.package.XXXXXX")
trap 'rm -rf "$staging_dir"' EXIT
plugin_dir="$staging_dir/$plugin_id"
mkdir -p "$plugin_dir" packages
cp -R dist/. "$plugin_dir/"
if [ -f CHANGELOG.md ]; then
  cp CHANGELOG.md "$plugin_dir/CHANGELOG.md"
fi
find "$plugin_dir" -type d -exec chmod 0755 {} +
find "$plugin_dir" -type f -exec chmod 0644 {} +

if [ -n "$backend_executable" ]; then
  if [ ! -f "$plugin_dir/go_plugin_build_manifest" ]; then
    printf 'Missing Go build manifest. Build the backend before packaging it.\n' >&2
    exit 1
  fi
  found_binary=false
  for binary in "$plugin_dir/${backend_executable}_"*; do
    if [ -f "$binary" ]; then
      # GitHub artifact downloads reset files to 0644, including executables.
      chmod 0755 "$binary"
      found_binary=true
    fi
  done
  if [ "$found_binary" = false ]; then
    printf 'Missing backend executable. Build the backend before packaging it.\n' >&2
    exit 1
  fi
fi

output_dir=$(cd packages && pwd)
archive_name="$plugin_id-$version"
zip_path="$output_dir/$archive_name.zip"
tar_path="$output_dir/$archive_name.tar.gz"
rm -f "$zip_path" "$tar_path"
(cd "$staging_dir" && zip -qr "$zip_path" "$plugin_id")
tar -czf "$tar_path" -C "$staging_dir" "$plugin_id"
printf '%s\n%s\n' "$zip_path" "$tar_path"
