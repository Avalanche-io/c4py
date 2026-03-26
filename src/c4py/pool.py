"""Pool and ingest — portable content bundles.

A *pool* bundles a c4m file together with the store objects it references
into a self-contained directory that can be transferred via any medium
(USB drive, email, shared folder, S3, etc.).

Layout of a pool directory::

    output_dir/
        project.c4m          # the manifest
        extract.sh            # portable POSIX extraction script
        store/                # FSStore containing referenced objects

``ingest`` is the reverse: it absorbs a pool bundle into the local store
and copies the c4m file(s) into the working directory.
"""

from __future__ import annotations

import os
import shutil
import stat
from dataclasses import dataclass, field
from pathlib import Path

from .decoder import load
from .encoder import dump
from .manifest import Manifest
from .store import FSStore, Store, open_store


@dataclass
class PoolResult:
    """Result of a pool operation."""

    copied: int = 0
    skipped: int = 0
    missing: int = 0


@dataclass
class IngestResult:
    """Result of an ingest operation."""

    copied: int = 0
    skipped: int = 0
    manifests: list[str] = field(default_factory=list)  # c4m filenames copied to cwd


def pool(
    manifest_or_path: Manifest | str | Path,
    output_dir: str | Path,
    *,
    store: Store | None = None,
) -> PoolResult:
    """Bundle a c4m file and its referenced objects into *output_dir*.

    Args:
        manifest_or_path: Loaded ``Manifest``, or path to a c4m file.
        output_dir: Destination directory.  Created if it does not exist.
        store: Source content store.  Discovered via ``open_store()`` when
            ``None``.

    Returns:
        ``PoolResult`` with counts of copied / skipped / missing objects.
    """
    manifest, c4m_path = _resolve_manifest_and_path(manifest_or_path)
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    if store is None:
        store = open_store()

    # Create the pool store.
    pool_store = FSStore(output_dir / "store")

    result = PoolResult()

    # Walk manifest entries and copy referenced objects.
    for _rel_path, entry in manifest.flat_entries():
        if entry.is_dir() or entry.c4id is None or entry.c4id.is_nil():
            continue

        if pool_store.has(entry.c4id):
            result.skipped += 1
            continue

        if not store.has(entry.c4id):
            result.missing += 1
            continue

        reader = store.get(entry.c4id)
        try:
            pool_store.put(reader)
        finally:
            reader.close()
        result.copied += 1

    # Copy the c4m file into the pool directory.
    if c4m_path is not None:
        dest_c4m = output_dir / Path(c4m_path).name
        shutil.copy2(str(c4m_path), str(dest_c4m))
    else:
        # Manifest was passed as an object — write it out.
        dest_c4m = output_dir / "manifest.c4m"
        dump(manifest, str(dest_c4m))

    # Generate portable extract script.
    _write_extract_script(output_dir, manifest)

    return result


def ingest(
    bundle_dir: str | Path,
    *,
    store: Store | None = None,
) -> IngestResult:
    """Absorb a pool bundle into the local store.

    Args:
        bundle_dir: Path to the pool directory (must contain ``store/``).
        store: Destination content store.  Discovered via ``open_store()``
            when ``None``.

    Returns:
        ``IngestResult`` with counts and list of c4m filenames copied to cwd.
    """
    bundle_dir = Path(bundle_dir)

    if store is None:
        store = open_store()

    pool_store_dir = bundle_dir / "store"

    result = IngestResult()

    # Copy objects from the pool store to the local store.
    if pool_store_dir.is_dir():
        pool_store = FSStore(pool_store_dir)
        _copy_store_objects(pool_store, pool_store_dir, store, result)

    # Copy c4m files to the current working directory.
    for item in sorted(bundle_dir.iterdir()):
        if item.is_file() and item.suffix == ".c4m":
            dest = Path.cwd() / item.name
            shutil.copy2(str(item), str(dest))
            result.manifests.append(item.name)

    return result


# ------------------------------------------------------------------
# Internal helpers
# ------------------------------------------------------------------

def _resolve_manifest_and_path(
    manifest_or_path: Manifest | str | Path,
) -> tuple[Manifest, str | None]:
    """Return (manifest, original_file_path_or_None)."""
    if isinstance(manifest_or_path, Manifest):
        return manifest_or_path, None
    path = str(manifest_or_path)
    return load(path), path


def _copy_store_objects(
    src: FSStore,
    src_dir: Path,
    dst: Store,
    result: IngestResult,
) -> None:
    """Walk *src_dir* and copy every C4 object into *dst*."""
    from .id import C4_ID_LENGTH, C4ID

    for dirpath_str, _dirnames, filenames in os.walk(str(src_dir)):
        for fname in filenames:
            # Only process files that look like C4 IDs.
            if len(fname) != C4_ID_LENGTH or not fname.startswith("c4"):
                continue
            try:
                c4id = C4ID(fname)
            except (ValueError, TypeError):
                continue

            if dst.has(c4id):
                result.skipped += 1
                continue

            reader = src.get(c4id)
            try:
                dst.put(reader)
            finally:
                reader.close()
            result.copied += 1


def _write_extract_script(output_dir: Path, manifest: Manifest) -> None:
    """Generate a portable POSIX shell script that extracts the pool."""
    # Find the c4m filename (first .c4m in directory, or fallback).
    c4m_name = "manifest.c4m"
    for item in output_dir.iterdir():
        if item.is_file() and item.suffix == ".c4m":
            c4m_name = item.name
            break

    script = (
        "#!/bin/sh\n"
        "# Auto-generated extraction script for c4m pool bundle.\n"
        "# Requires: c4py (pip install c4py) or c4 CLI\n"
        "#\n"
        "# Usage: sh extract.sh [target_directory]\n"
        "#\n"
        'set -e\n'
        '\n'
        'BUNDLE_DIR="$(cd "$(dirname "$0")" && pwd)"\n'
        'TARGET="${1:-.}"\n'
        '\n'
        'mkdir -p "$TARGET"\n'
        '\n'
        'if command -v c4 >/dev/null 2>&1; then\n'
        f'  C4_STORE="$BUNDLE_DIR/store" c4 patch "$BUNDLE_DIR/{c4m_name}" "$TARGET"\n'
        'elif command -v python3 >/dev/null 2>&1; then\n'
        '  python3 -c "\n'
        'import sys, c4py\n'
        "m = c4py.load(sys.argv[1])\n"
        "s = c4py.open_store(sys.argv[2])\n"
        "c4py.reconcile(m, sys.argv[3], store=s)\n"
        f'" "$BUNDLE_DIR/{c4m_name}" "$BUNDLE_DIR/store" "$TARGET"\n'
        'else\n'
        '  echo "Error: requires c4 CLI or python3 with c4py installed" >&2\n'
        '  exit 1\n'
        'fi\n'
        '\n'
        'echo "Extracted to $TARGET"\n'
    )

    script_path = output_dir / "extract.sh"
    script_path.write_text(script, encoding="utf-8")
    # Make executable.
    script_path.chmod(script_path.stat().st_mode | stat.S_IXUSR | stat.S_IXGRP | stat.S_IXOTH)
