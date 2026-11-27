#!/usr/bin/env bash

set -euo pipefail

if [ "$#" -gt 1 ]; then
  printf 'Usage: %s [version]\n' "$0" >&2
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
  printf 'Invalid package version: %s\n' "$version" >&2
  exit 2
fi

staging_dir=$(mktemp -d "${TMPDIR:-/tmp}/${plugin_id}.package.XXXXXX")
trap 'rm -rf "$staging_dir"' EXIT
if [ -z "$backend_executable" ] || [ ! -f dist/go_plugin_build_manifest ]; then
  printf 'Missing backend executable configuration or Go build manifest. Build the backend before packaging it.\n' >&2
  exit 1
fi

mkdir -p packages
output_dir=$(cd packages && pwd)
found_binary=false
for platform in linux windows darwin; do
  binaries=(dist/"${backend_executable}_${platform}_"*)
  [ -f "${binaries[0]}" ] || continue
  found_binary=true
  plugin_dir="$staging_dir/$plugin_id"
  mkdir -p "$plugin_dir"
  cp -R dist/. "$plugin_dir/"
  # Each archive contains only the binaries for its platform.
  rm -f "$plugin_dir/${backend_executable}_"*
  cp "${binaries[@]}" "$plugin_dir/"
  if [ -f CHANGELOG.md ]; then
    cp CHANGELOG.md "$plugin_dir/CHANGELOG.md"
  fi
  find "$plugin_dir" -type d -exec chmod 0755 {} +
  find "$plugin_dir" -type f -exec chmod 0644 {} +
  chmod 0755 "$plugin_dir/${backend_executable}_${platform}_"*

  if [ "$platform" = windows ]; then
    archive_path="$output_dir/$plugin_id-$version-$platform.zip"
    rm -f "$archive_path"
    (cd "$staging_dir" && zip -qr "$archive_path" "$plugin_id")
  else
    archive_path="$output_dir/$plugin_id-$version-$platform.tar.gz"
    tar -czf "$archive_path" -C "$staging_dir" "$plugin_id"
  fi
  printf '%s\n' "$archive_path"
  rm -rf "$plugin_dir"
done

if [ "$found_binary" = false ]; then
  printf 'Missing backend executable. Build the backend before packaging it.\n' >&2
  exit 1
fi
