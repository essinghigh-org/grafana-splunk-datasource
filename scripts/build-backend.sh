#!/bin/sh

set -eu

goos="${GOOS:-$(go env GOOS)}"
goarch="${GOARCH:-$(go env GOARCH)}"

# Mage must run on the host even when a different backend target is requested.
unset GOOS GOARCH

case "${1:-}" in
  '')
    if [ "$#" -ne 0 ]; then
      printf 'Usage: %s [--all]\n' "$0" >&2
      exit 2
    fi
    go run github.com/magefile/mage build:custom "$goos" "$goarch"
    ;;
  --all)
    if [ "$#" -ne 1 ]; then
      printf 'Usage: %s [--all]\n' "$0" >&2
      exit 2
    fi
    go run github.com/magefile/mage buildAll
    ;;
  *)
    printf 'Usage: %s [--all]\n' "$0" >&2
    exit 2
    ;;
esac
