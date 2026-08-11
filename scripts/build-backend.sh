#!/bin/sh

set -eu

plugin_name="essinghigh-splunk-datasource"
goos="${GOOS:-$(go env GOOS)}"
goarch="${GOARCH:-$(go env GOARCH)}"
output="dist/${plugin_name}_${goos}_${goarch}"

mkdir -p dist
CGO_ENABLED=0 GOOS="${goos}" GOARCH="${goarch}" go build -trimpath -o "${output}" ./pkg
