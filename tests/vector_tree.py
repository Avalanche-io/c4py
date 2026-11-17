"""Build a shared vector's tree from its tree.txt (core's vector READMEs).

Each line is ``<octal mode> <mtime ns> <path>``; a path ending in ``/`` is a
directory; a file's bytes are its base name and a newline. Times are set
once every entry exists, files first, then directories deepest first, so a
child's time never moves its parent's.
"""

from __future__ import annotations

import os
from pathlib import Path


def build_tree(vector: Path, dest: Path) -> None:
    rows = []
    for line in (vector / "tree.txt").read_text(encoding="utf-8").splitlines():
        mode, ns, path = line.split(" ", 2)
        rows.append((int(mode, 8), int(ns), path))
    for _, _, path in rows:
        full = dest / path.rstrip("/")
        if path.endswith("/"):
            full.mkdir(parents=True, exist_ok=True)
        else:
            full.parent.mkdir(parents=True, exist_ok=True)
            full.write_bytes(full.name.encode() + b"\n")
    for mode, ns, path in sorted(rows, key=lambda r: (r[2].endswith("/"), -r[2].count("/"))):
        full = dest / path.rstrip("/")
        os.chmod(full, mode)
        os.utime(full, ns=(ns, ns))
