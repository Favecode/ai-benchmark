#!/usr/bin/env bash
# Canonical reference solution installer.
# Copies the reference RateLimiter implementation into place, overwriting
# the stub — this is what an agent's own edit-in-place solution would do.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"

TARGET=""
if [ -d "$ROOT_DIR/src" ]; then
    TARGET="$ROOT_DIR/src/rate_limiter.py"
elif [ -d "$ROOT_DIR/environment/src" ]; then
    TARGET="$ROOT_DIR/environment/src/rate_limiter.py"
else
    TARGET="/workspace/src/rate_limiter.py"
fi

mkdir -p "$(dirname "$TARGET")"
cp "$SCRIPT_DIR/rate_limiter_solution.py" "$TARGET"

echo "Reference solution installed at $TARGET"
