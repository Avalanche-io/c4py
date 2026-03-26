"""Directory scanner — walk a filesystem and produce a Manifest.

Scans a directory tree, computing C4 IDs for each file and building
a c4m manifest that describes the complete filesystem state.

When a Store is provided, content is stored as it's identified — single pass
over the data, zero extra I/O. This matches `c4 id -s` in the Go CLI.

Memory trade-off: For files up to 100 MB, content is read into memory once
and used for both C4 ID computation and store put. For files larger than
100 MB, a temporary file is used to avoid excessive memory consumption.

Reference: github.com/Avalanche-io/c4/scan/generator.go
"""

from __future__ import annotations

import io
import os
import stat
import tempfile
from collections.abc import Callable
from copy import deepcopy
from datetime import datetime, timezone
from pathlib import Path
from typing import TYPE_CHECKING

from .entry import NULL_TIMESTAMP, Entry
from .id import C4ID, identify_bytes
from .manifest import Manifest

if TYPE_CHECKING:
    from .store import Store

# Files larger than this are processed via temp file to limit memory usage.
_LARGE_FILE_THRESHOLD = 100 * 1024 * 1024  # 100 MB


def scan(
    path: str | Path,
    *,
    store: Store | None = None,
    follow_symlinks: bool = False,
    compute_ids: bool = True,
    progress: Callable[[str, int, int], None] | None = None,
) -> Manifest:
    """Scan a directory and produce a c4m Manifest.

    Args:
        path: directory to scan
        store: if provided, store content while identifying (zero extra I/O —
               bytes are already being read for hashing). Equivalent to `c4 id -s`.
        follow_symlinks: if True, follow symlinks when scanning
        compute_ids: if True, compute C4 IDs for all files (slower but complete)
        progress: optional callback called after each file is processed.
               Signature: progress(file_path, files_processed, 0).
               The third argument is 0 (unknown total) — the caller can
               display a counter without a progress bar.

    Returns:
        Manifest describing the directory contents

    The scanner:
    1. Walks the directory tree (os.walk)
    2. For each file: stat for metadata, compute C4 ID (streaming)
    3. If store is provided: read once, identify + store (single pass for
       files <= 100 MB; temp file for larger files)
    4. For symlinks: record target, optionally identify target content
    5. Sort entries into canonical order
    6. Compute directory C4 IDs bottom-up
    """
    root = Path(path).resolve()
    if not root.is_dir():
        raise ValueError(f"not a directory: {root}")

    # Collect entries per directory for bottom-up processing
    all_entries: list[Entry] = []
    # Map from directory path to list of child entries for bottom-up C4 ID
    dir_children: dict[str, list[Entry]] = {}
    files_processed = 0

    for dirpath_str, dirnames, filenames in os.walk(
        str(root), followlinks=follow_symlinks
    ):
        dirpath = Path(dirpath_str)

        # Skip hidden directories (.git, .c4, etc.)
        dirnames[:] = [
            d for d in dirnames
            if not d.startswith(".")
        ]
        # Sort for deterministic order
        dirnames.sort()
        filenames.sort()

        # Compute depth relative to root
        try:
            rel = dirpath.relative_to(root)
        except ValueError:
            continue
        parts = rel.parts
        depth = len(parts)

        children_for_dir: list[Entry] = []
        dir_key = str(dirpath)

        # Process files
        for fname in filenames:
            if fname.startswith("."):
                continue

            fpath = dirpath / fname
            try:
                if follow_symlinks:
                    fstat = fpath.stat()
                else:
                    fstat = fpath.lstat()
            except OSError:
                continue

            file_mode = fstat.st_mode

            # Handle symlinks
            if stat.S_ISLNK(file_mode) and not follow_symlinks:
                entry = _make_symlink_entry(fpath, fstat, fname, depth, compute_ids)
                all_entries.append(entry)
                children_for_dir.append(entry)
                files_processed += 1
                if progress is not None:
                    progress(str(fpath), files_processed, 0)
                continue

            # Regular file
            entry = Entry(
                name=fname,
                mode=file_mode,
                timestamp=datetime.fromtimestamp(fstat.st_mtime, tz=timezone.utc),
                size=fstat.st_size,
                depth=depth,
            )

            if compute_ids and stat.S_ISREG(file_mode):
                entry.c4id = _identify_and_store(fpath, fstat.st_size, store)

            all_entries.append(entry)
            children_for_dir.append(entry)
            files_processed += 1
            if progress is not None:
                progress(str(fpath), files_processed, 0)

        # Process subdirectories (create entries)
        for dname in dirnames:
            dpath = dirpath / dname
            try:
                if follow_symlinks:
                    dstat = dpath.stat()
                else:
                    dstat = dpath.lstat()
            except OSError:
                continue

            dir_mode = dstat.st_mode

            # Handle symlink directories
            if stat.S_ISLNK(dir_mode) and not follow_symlinks:
                entry = _make_symlink_entry(dpath, dstat, dname + "/", depth, compute_ids)
                all_entries.append(entry)
                children_for_dir.append(entry)
                continue

            entry = Entry(
                name=dname + "/",
                mode=dir_mode,
                timestamp=NULL_TIMESTAMP,  # will be computed bottom-up
                size=-1,  # will be computed bottom-up
                depth=depth,
            )
            all_entries.append(entry)
            children_for_dir.append(entry)

        dir_children[dir_key] = children_for_dir

    # Sort entries into canonical order
    manifest = Manifest(entries=all_entries)
    manifest.sort_entries()

    # Compute directory sizes and timestamps bottom-up
    _propagate_metadata(manifest.entries)

    # Compute directory C4 IDs bottom-up
    if compute_ids:
        _compute_dir_ids(manifest)

    return manifest


def _make_symlink_entry(
    fpath: Path,
    fstat: os.stat_result,
    name: str,
    depth: int,
    compute_ids: bool,
) -> Entry:
    """Create an Entry for a symlink."""
    target = ""
    try:
        target = os.readlink(fpath)
        # Normalize to forward slashes for c4m portability
        target = target.replace("\\", "/")
    except OSError:
        pass

    entry = Entry(
        name=name,
        mode=fstat.st_mode,
        timestamp=datetime.fromtimestamp(fstat.st_mtime, tz=timezone.utc),
        size=fstat.st_size,
        depth=depth,
        target=target,
    )

    # Compute C4 ID of symlink target if enabled
    if compute_ids and target:
        target_path = fpath.parent / target if not os.path.isabs(target) else Path(target)
        try:
            target_info = target_path.stat()
            if stat.S_ISREG(target_info.st_mode):
                entry.c4id = _identify_file(target_path)
        except OSError:
            pass  # broken symlink or inaccessible

    return entry


def _identify_and_store(
    fpath: Path, file_size: int, store: Store | None
) -> C4ID | None:
    """Compute C4 ID of a file, optionally storing it.

    For files <= 100 MB: read into memory, canonicalize if c4m, then
    identify + store. For files > 100 MB: use temp file.
    If no store, just identify directly (identify_file handles canonicalization).
    """
    from .canonical import try_canonicalize
    from .id import identify_file as id_file

    if store is None:
        return _identify_file(fpath)

    if file_size <= _LARGE_FILE_THRESHOLD:
        # Small/medium file: read once into memory
        try:
            data = fpath.read_bytes()
        except OSError:
            return None

        # Canonicalize c4m content
        canonical = try_canonicalize(data)
        if canonical is not None:
            data = canonical

        c4id = identify_bytes(data)
        store.put(io.BytesIO(data))
        return c4id
    else:
        # Large file: temp file approach
        tmp_path = None
        try:
            with tempfile.NamedTemporaryFile(delete=False) as tmp:
                tmp_path = tmp.name
                with open(fpath, "rb") as f:
                    while True:
                        chunk = f.read(65536)
                        if not chunk:
                            break
                        tmp.write(chunk)

            # Identify from temp file (identify_file handles canonicalization)
            c4id = id_file(Path(tmp_path))

            # Store from temp file (store.put handles canonicalization)
            with open(tmp_path, "rb") as f:
                store.put(f)

            return c4id
        except OSError:
            return None
        finally:
            if tmp_path:
                try:
                    os.unlink(tmp_path)
                except OSError:
                    pass


def _identify_file(fpath: Path) -> C4ID | None:
    """Compute C4 ID of a file, canonicalizing c4m content."""
    from .id import identify_file as id_file

    try:
        return id_file(fpath)
    except OSError:
        return None


def _propagate_metadata(entries: list[Entry]) -> None:
    """Compute directory sizes and timestamps bottom-up from children.

    Processes in reverse order so child directories resolve before parents.
    """
    for i in range(len(entries) - 1, -1, -1):
        entry = entries[i]
        if not entry.is_dir():
            continue

        children = _get_children(entries, i)
        if not children:
            continue

        # Propagate size if null
        if entry.size < 0:
            total = 0
            null_found = False
            for child in children:
                if child.size < 0:
                    null_found = True
                    break
                total += child.size
            if not null_found:
                # Add the byte length of the directory's canonical c4m content
                for child in children:
                    total += len(child.canonical().encode("utf-8")) + 1  # +1 for '\n'
            entry.size = -1 if null_found else total

        # Propagate timestamp if null
        if entry.has_null_timestamp():
            most_recent = NULL_TIMESTAMP
            null_found = False
            for child in children:
                if child.has_null_timestamp():
                    null_found = True
                    break
                if child.timestamp > most_recent:
                    most_recent = child.timestamp
            entry.timestamp = NULL_TIMESTAMP if null_found else most_recent


def _get_children(entries: list[Entry], dir_idx: int) -> list[Entry]:
    """Get direct children of a directory entry."""
    dir_entry = entries[dir_idx]
    dir_depth = dir_entry.depth
    children: list[Entry] = []

    for j in range(dir_idx + 1, len(entries)):
        e = entries[j]
        if e.depth == dir_depth + 1:
            children.append(e)
        elif e.depth <= dir_depth:
            break

    return children


def _compute_dir_ids(manifest: Manifest) -> None:
    """Compute directory C4 IDs bottom-up.

    Each directory's C4 ID is computed from the canonical form of its
    immediate children (as a sub-manifest).
    """
    entries = manifest.entries

    # Process in reverse so children are resolved before parents
    for i in range(len(entries) - 1, -1, -1):
        entry = entries[i]
        if not entry.is_dir():
            continue

        children = _get_children(entries, i)
        if not children:
            entry.c4id = identify_bytes(b"")
            continue

        # Build a sub-manifest of just these children (at depth 0)
        sub_entries: list[Entry] = []
        _collect_subtree(entries, i, sub_entries)

        # Adjust depths to be relative to this directory
        adjusted: list[Entry] = []
        base_depth = entry.depth + 1
        for e in sub_entries:
            ae = deepcopy(e)
            ae.depth = e.depth - base_depth
            adjusted.append(ae)

        sub_manifest = Manifest(entries=adjusted)
        entry.c4id = sub_manifest.compute_c4id()


def _collect_subtree(
    entries: list[Entry], dir_idx: int, result: list[Entry]
) -> None:
    """Collect all entries that are descendants of the directory at dir_idx."""
    dir_depth = entries[dir_idx].depth
    for j in range(dir_idx + 1, len(entries)):
        e = entries[j]
        if e.depth <= dir_depth:
            break
        result.append(e)
