#!/bin/sh
set -eu

output=--load
if [ "${1:-}" = "--push" ]; then
  output=--push
  shift
fi

if [ "$#" -ne 1 ]; then
  echo "Usage: sh ./docker-build.sh [--push] <registry/project/image>" >&2
  exit 1
fi

cd "$(dirname "$0")"
version=$(sed -n 's/^  version: //p' config/settings.yaml | head -n 1 | tr -d '\r')
docker buildx build \
  --pull \
  --platform "${PLATFORM:-linux/amd64}" \
  --tag "$1:$version" \
  --tag "$1:latest" \
  "$output" \
  .
