"""Verification pipeline — compare a c4m manifest against a real directory.

Walk a directory, identify each file, and compare against expected C4 IDs
from a manifest. Reports matches, missing files, corrupt files (wrong C4 ID),
and extra files (on disk but not in the manifest).

This is the engine behind `python -m c4py verify` and the c4py.verify_tree() API.
"""

from __future__ import annotations

import os
from collections.abc import Callable
from dataclasses import dataclass, field
from pathlib import Path

from .id import C4ID, identify_file
from .manifest import Manifest


@dataclass
class CorruptEntry:
    """A file whose C4 ID does not match the manifest."""

    path: str
    expected: C4ID
    actual: C4ID


@dataclass
class VerifyReport:
    """Result of verifying a directory against a manifest."""

    ok: list[str] = field(default_factory=list)
    missing: list[str] = field(default_factory=list)
    corrupt: list[CorruptEntry] = field(default_factory=list)
    extra: list[str] = field(default_factory=list)

    @property
    def is_ok(self) -> bool:
        """True if the directory fully matches the manifest."""
        return not self.missing and not self.corrupt and not self.extra


ProgressCallback = Callable[[str, int, int], None]


def verify_tree(
    manifest_or_path: Manifest | str | Path,
    directory: str | Path,
    *,
    progress: ProgressCallback | None = None,
) -> VerifyReport:
    """Compare a manifest against a real directory.

    Args:
        manifest_or_path: a Manifest object, or a path to a .c4m file to load
        directory: the directory to verify against
        progress: optional callback(path, index, total) called for each file checked

    Returns:
        VerifyReport with ok, missing, corrupt, and extra lists
    """
    # Load manifest if given a path
    if isinstance(manifest_or_path, str | Path):
        from .decoder import load
        manifest = load(str(manifest_or_path))
    else:
        manifest = manifest_or_path

    directory = Path(directory).resolve()

    # Build expected files from manifest: path -> C4ID (files only)
    expected: dict[str, C4ID] = {}
    expected_dirs: set[str] = set()
    for rel_path, entry in manifest.flat_entries():
        if entry.is_dir():
            expected_dirs.add(rel_path)
            continue
        if entry.c4id is not None:
            expected[rel_path] = entry.c4id

    # Walk the actual directory to find all files on disk
    on_disk: set[str] = set()
    for dirpath_str, dirnames, filenames in os.walk(str(directory)):
        dirpath = Path(dirpath_str)
        # Skip hidden directories (match scanner behavior)
        dirnames[:] = [d for d in dirnames if not d.startswith(".")]
        for fname in filenames:
            if fname.startswith("."):
                continue
            fpath = dirpath / fname
            try:
                rel = str(fpath.relative_to(directory))
            except ValueError:
                continue
            # Normalize to forward slashes for cross-platform consistency
            rel = rel.replace(os.sep, "/")
            on_disk.add(rel)

    report = VerifyReport()

    # Determine total for progress reporting
    all_paths = sorted(set(expected.keys()) | on_disk)
    total = len(all_paths)

    for idx, rel_path in enumerate(all_paths):
        if progress is not None:
            progress(rel_path, idx, total)

        in_manifest = rel_path in expected
        in_disk = rel_path in on_disk

        if in_manifest and not in_disk:
            report.missing.append(rel_path)
            continue

        if not in_manifest and in_disk:
            report.extra.append(rel_path)
            continue

        # Both in manifest and on disk — verify C4 ID
        expected_id = expected[rel_path]
        abs_path = directory / rel_path
        actual_id = identify_file(abs_path)

        if actual_id == expected_id:
            report.ok.append(rel_path)
        else:
            report.corrupt.append(CorruptEntry(
                path=rel_path,
                expected=expected_id,
                actual=actual_id,
            ))

    return report
