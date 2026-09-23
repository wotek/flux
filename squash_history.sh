#!/bin/bash
set -e

echo "1. Squashing git history into a single commit..."
git checkout --orphan temp_main
git add -A
git commit -m "feat: initial release"

echo "2. Deleting old main and replacing with squashed main..."
git branch -D main
git branch -m main

echo "3. Force pushing squashed main to origin..."
git push -f origin main

echo "4. Deleting old v1.0.0 release and tag..."
gh release delete v1.0.0 -y || true
git push origin :refs/tags/v1.0.0 || true
git tag -d v1.0.0 || true

echo "5. Re-tagging and re-releasing v1.0.0..."
git tag v1.0.0
git push origin v1.0.0
gh release create v1.0.0 --title "v1.0.0" --notes "Initial release"

echo "6. Warming up pkg.go.dev..."
GOPROXY=proxy.golang.org go list -m github.com/wotek/flux@v1.0.0 || true
curl -s "https://pkg.go.dev/github.com/wotek/flux@v1.0.0" > /dev/null || true

echo "Done!"
