#!/usr/bin/env bash
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
docker run --rm -v "$SCRIPT_DIR/website:/app" -w /app node:22 npm run docs:build
