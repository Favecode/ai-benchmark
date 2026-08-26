#!/usr/bin/env bash
# Grading entrypoint. Runs visible tests (informational to the agent),
# then integrity-checks store.py, then runs hidden tests (authoritative
# for scoring). Exits 0 only if all hidden tests pass.
set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
STORE_PATH="$ROOT_DIR/src/store.py"
EXPECTED_HASH_FILE="$ROOT_DIR/.store_hash_expected"

echo "=== Visible tests ==="
python3 "$SCRIPT_DIR/visible/test_visible.py"
VISIBLE_STATUS=$?

echo ""
echo "=== Integrity check: /workspace/src/store.py must be unmodified ==="
if [ ! -f "$EXPECTED_HASH_FILE" ]; then
    echo "FAIL: expected hash file not found at $EXPECTED_HASH_FILE"
    exit 1
fi

EXPECTED_HASH="$(cat "$EXPECTED_HASH_FILE")"
ACTUAL_HASH="$(python3 -c "import hashlib; print(hashlib.sha256(open('$STORE_PATH','rb').read()).hexdigest())")"
if [ "$EXPECTED_HASH" != "$ACTUAL_HASH" ]; then
    echo "FAIL: store.py has been modified. This file is part of the fixed"
    echo "harness and must not be edited. Grading aborted."
    exit 1
fi

echo "OK: store.py hash matches expected."

echo ""
echo "=== Hidden tests (authoritative) ==="
python3 "$SCRIPT_DIR/hidden/test_hidden.py"
HIDDEN_STATUS=$?

echo ""
if [ $HIDDEN_STATUS -eq 0 ]; then
    echo "RESULT: PASS (all hidden tests passed)"
else
    echo "RESULT: FAIL (one or more hidden tests failed)"
fi

if [ $VISIBLE_STATUS -ne 0 ]; then
    echo "NOTE: visible tests also had failures — fix those first."
fi

exit $HIDDEN_STATUS
