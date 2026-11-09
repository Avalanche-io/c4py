"""Reconcile — make a directory match a manifest (like ``c4 patch``).

Given a manifest and a target directory, reconcile computes the operations
needed to bring the directory into the state described by the manifest, then
optionally executes them.

Content is pulled from a Store by C4 ID.  If the store is missing any
required objects the caller gets a clear list of what is absent so it can
be fetched before retrying.
"""

from __future__ import annotations

import os
import stat
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path

from .decoder import load
from .entry import NULL_TIMESTAMP, Entry, is_null_mode, os_mode
from .id import C4ID, identify_file
from .manifest import Manifest
from .store import Store, open_store


@dataclass
class ReconcileOp:
    """A single reconcile operation."""

    type: str  # "create", "remove", "update", "mkdir", "rmdir"
    path: str


@dataclass
class ReconcilePlan:
    """Dry-run result: the operations that *would* be performed."""

    operations: list[ReconcileOp] = field(default_factory=list)
    missing: list[C4ID] = field(default_factory=list)  # content needed but not in store


@dataclass
class ReconcileResult:
    """Result of an executed reconcile."""

    created: int = 0
    removed: int = 0
    updated: int = 0
    skipped: int = 0
    errors: list[str] = field(default_factory=list)


# Type alias for the progress callback.
ProgressCallback = Callable[[str, str, int, int], None]


def reconcile(
    manifest_or_path: Manifest | str | Path,
    directory: str | Path,
    *,
    store: Store | None = None,
    dry_run: bool = False,
    progress: ProgressCallback | None = None,
) -> ReconcileResult | ReconcilePlan:
    """Make *directory* match *manifest_or_path*.

    Args:
        manifest_or_path: A loaded ``Manifest``, a c4m file path, or a c4m
            string path.
        directory: The target directory to reconcile.
        store: Content store to pull file data from.  When ``None`` the
            default store is discovered via ``open_store()``.
        dry_run: If ``True`` return a ``ReconcilePlan`` without touching
            the filesystem.
        progress: Optional callback ``(op_type, path, index, total)`` invoked
            for every operation.

    Returns:
        ``ReconcilePlan`` when *dry_run* is True, ``ReconcileResult`` otherwise.
    """
    manifest = _resolve_manifest(manifest_or_path)
    directory = Path(directory)

    if store is None:
        store = open_store()

    # Build the plan.
    plan = _build_plan(manifest, directory, store)

    if dry_run:
        return plan

    # Execute.
    return _execute_plan(plan, directory, store, manifest, progress)


# ------------------------------------------------------------------
# Internal helpers
# ------------------------------------------------------------------


def _resolve_manifest(manifest_or_path: Manifest | str | Path) -> Manifest:
    if isinstance(manifest_or_path, Manifest):
        return manifest_or_path
    return load(str(manifest_or_path))


def _build_plan(manifest: Manifest, directory: Path, store: Store) -> ReconcilePlan:
    """Compare manifest against *directory* and produce a ``ReconcilePlan``."""
    plan = ReconcilePlan()

    # Collect desired state from manifest (full relative paths).
    desired_files: dict[str, C4ID | None] = {}
    desired_dirs: set[str] = set()

    for rel_path, entry in manifest.flat_entries():
        if entry.is_dir():
            desired_dirs.add(rel_path)
        else:
            desired_files[rel_path] = entry.c4id

    # Collect existing state on disk.
    existing_files: set[str] = set()
    existing_dirs: set[str] = set()
    if directory.is_dir():
        for dirpath_str, dirnames, filenames in os.walk(str(directory)):
            dirpath = Path(dirpath_str)
            try:
                rel = dirpath.relative_to(directory)
            except ValueError:
                continue
            if str(rel) != ".":
                existing_dirs.add(str(rel))
            for fname in filenames:
                existing_files.add(str(rel / fname) if str(rel) != "." else fname)

    # 1. Directories to create (parents before children — sorted order gives us that).
    for d in sorted(desired_dirs):
        if d not in existing_dirs:
            plan.operations.append(ReconcileOp(type="mkdir", path=d))

    # 2. Files to create or update.
    for rel_path, c4id in desired_files.items():
        if c4id is not None and not store.has(c4id):
            plan.missing.append(c4id)
            continue

        if rel_path not in existing_files:
            plan.operations.append(ReconcileOp(type="create", path=rel_path))
        else:
            # Check whether existing content already matches.
            full = directory / rel_path
            if c4id is not None and full.is_file():
                existing_id = identify_file(str(full))
                if existing_id == c4id:
                    continue  # already up to date
            plan.operations.append(ReconcileOp(type="update", path=rel_path))

    # 3. Extra files to remove (present on disk but absent from manifest).
    for rel_path in sorted(existing_files):
        if rel_path not in desired_files:
            plan.operations.append(ReconcileOp(type="remove", path=rel_path))

    # 4. Extra directories to remove (deepest first).
    for d in sorted(existing_dirs, key=lambda p: p.count("/"), reverse=True):
        if d not in desired_dirs:
            plan.operations.append(ReconcileOp(type="rmdir", path=d))

    return plan


def _execute_plan(
    plan: ReconcilePlan,
    directory: Path,
    store: Store,
    manifest: Manifest,
    progress: ProgressCallback | None,
) -> ReconcileResult:
    result = ReconcileResult()
    total = len(plan.operations)

    # Pre-build metadata lookup from manifest (path -> entry).
    meta: dict[str, Entry] = {}
    for rel_path, entry in manifest.flat_entries():
        meta[rel_path] = entry

    # Directory modes and timestamps are applied after every child is
    # written (deepest first): writing children would clobber a parent's
    # mtime, and a known zero mode ("d---------") would lock us out of a
    # directory we still have to fill.

    for idx, op in enumerate(plan.operations):
        if progress is not None:
            progress(op.type, op.path, idx, total)

        full = directory / op.path

        try:
            if op.type == "mkdir":
                full.mkdir(parents=True, exist_ok=True)
                result.created += 1

            elif op.type == "create":
                full.parent.mkdir(parents=True, exist_ok=True)
                create_entry = meta.get(op.path)
                if create_entry is not None and create_entry.c4id is not None:
                    _write_from_store(full, create_entry.c4id, store)
                    _apply_file_meta(full, create_entry)
                else:
                    full.touch()
                result.created += 1

            elif op.type == "update":
                update_entry = meta.get(op.path)
                if update_entry is not None and update_entry.c4id is not None:
                    _write_from_store(full, update_entry.c4id, store)
                    _apply_file_meta(full, update_entry)
                result.updated += 1

            elif op.type == "remove":
                if full.is_file() or full.is_symlink():
                    full.unlink()
                result.removed += 1

            elif op.type == "rmdir":
                if full.is_dir():
                    _rmdir_safe(full)
                result.removed += 1

        except Exception as exc:  # noqa: BLE001
            result.errors.append(f"{op.type} {op.path}: {exc}")

    # Directory metadata, deepest first: a child directory's time and mode
    # are set before its parent is closed, so a parent at "d---------"
    # never blocks the utime/chmod of what lies under it. Timestamp goes
    # before mode so the utime is not refused by the mode it is about to
    # get. Nulls never overwrite: a null timestamp leaves the directory's
    # time alone, and a null mode leaves whatever mkdir gave it.
    dirs = [(p, e) for p, e in manifest.flat_entries() if e.is_dir()]
    for rel_path, entry in sorted(dirs, key=lambda t: t[0].count("/"), reverse=True):
        full = directory / rel_path
        if not full.is_dir():
            continue
        _apply_dir_meta(full, entry)

    return result


def _write_from_store(dest: Path, c4id: C4ID, store: Store) -> None:
    """Write content from store to *dest* atomically-ish."""
    reader = store.get(c4id)
    try:
        with open(dest, "wb") as f:
            while True:
                chunk = reader.read(65536)
                if not chunk:
                    break
                f.write(chunk)
    finally:
        reader.close()


def _apply_file_meta(path: Path, entry: Entry) -> None:
    """Set permissions and timestamps on a file.

    A null mode is never applied (the file keeps the platform default it
    was created with, or the mode it already had). The known zero mode
    (ZERO_MODE, "----------") applies as chmod 0000: S_IMODE strips the
    sentinel bit along with the type bits.
    """
    if not is_null_mode(entry.mode):
        _chmod(path, entry.mode)

    if entry.timestamp != NULL_TIMESTAMP:
        epoch = _truncate_to_seconds(entry.timestamp)
        try:
            os.utime(str(path), (epoch, epoch))
        except OSError:
            pass


def _apply_dir_meta(path: Path, entry: Entry) -> None:
    """Set a directory's timestamp, then its permissions.

    Called after the directory's children are written. Order matters: a
    known zero mode ("d---------") set first would refuse the utime.
    """
    if entry.timestamp != NULL_TIMESTAMP:
        epoch = _truncate_to_seconds(entry.timestamp)
        try:
            os.utime(str(path), (epoch, epoch))
        except OSError:
            pass

    if not is_null_mode(entry.mode):
        _chmod(path, entry.mode)


def _chmod(path: Path, mode: int) -> None:
    """chmod *path* to the permission bits of a known c4m mode, if they differ."""
    perms = stat.S_IMODE(os_mode(mode))
    try:
        if stat.S_IMODE(os.lstat(str(path)).st_mode) == perms:
            return
        os.chmod(str(path), perms)
    except OSError:
        pass


def _truncate_to_seconds(ts: datetime) -> float:
    """Truncate a datetime to whole seconds (c4m precision)."""
    epoch = ts.timestamp()
    return float(int(epoch))


def _rmdir_safe(path: Path) -> None:
    """Remove a directory only if it is empty."""
    try:
        path.rmdir()
    except OSError:
        pass
