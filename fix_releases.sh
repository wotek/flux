#!/bin/bash
set -e

# 1. Delete all releases on GitHub
gh release delete v2.2.0 -y || true
gh release delete v2.1.0 -y || true
gh release delete v2.0.1 -y || true
gh release delete v2.0.0 -y || true
gh release delete v1.0.0 -y || true

# 2. Delete all tags on remote
git push origin :refs/tags/v2.2.0 :refs/tags/v2.1.0 :refs/tags/v2.0.1 :refs/tags/v2.0.0 :refs/tags/v1.0.0 || true

# 3. Delete all tags locally
git tag -d v2.2.0 v2.1.0 v2.0.1 v2.0.0 v1.0.0 || true

# 4. Create new v1.0.0 tag at current HEAD
git tag v1.0.0
git push origin v1.0.0

# 5. Create new GitHub Release
gh release create v1.0.0 --title "v1.0.0" --notes "Reset major version to v1.0.0 to fix Go Module proxy invalidation."

# 6. Warm up proxy and pkg.go.dev
GOPROXY=proxy.golang.org go list -m github.com/wotek/flux@v1.0.0 || true
curl -s "https://pkg.go.dev/github.com/wotek/flux@v1.0.0" > /dev/null || true
echo "Done!"
