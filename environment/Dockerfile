FROM python:3.11-slim

# No external dependencies are required beyond the standard library —
# this keeps the image buildable and runnable fully offline.

WORKDIR /workspace

COPY environment/src/ /workspace/src/
COPY tests/ /workspace/tests/

# Reference hash of store.py is baked in at build time so the grading
# script can detect tampering even if the file's mtime is spoofed.
RUN python3 - <<'PY'
import hashlib
from pathlib import Path
store_path = Path('/workspace/src/store.py')
h = hashlib.sha256(store_path.read_bytes()).hexdigest()
Path('/workspace/.store_hash_expected').write_text(h)
PY

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

CMD ["/bin/bash"]