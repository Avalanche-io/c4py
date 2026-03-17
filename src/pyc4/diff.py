"""C4M diff — compare two manifests and categorize changes.

Operations:
- diff(a, b) -> DiffResult with added/removed/modified/same
- patch_diff(old, new) -> patch entries for incremental updates
- apply_patch(base, patch) -> result manifest

Reference: /Users/joshua/ws/active/c4/oss/c4/c4m/operations.go
"""

from __future__ import annotations

from dataclasses import dataclass, field

from .entry import Entry
from .manifest import Manifest


@dataclass
class DiffResult:
    """Result of comparing two manifests."""

    added: list[Entry] = field(default_factory=list)
    removed: list[Entry] = field(default_factory=list)
    modified: list[Entry] = field(default_factory=list)
    same: list[Entry] = field(default_factory=list)


def diff(a: Manifest, b: Manifest) -> DiffResult:
    """Compare two manifests and categorize entries.

    Args:
        a: the "old" manifest
        b: the "new" manifest

    Returns:
        DiffResult with entries categorized as added, removed, modified, or same.
        Entries are matched by path (name + depth position in tree).
    """
    # TODO: implement manifest diff
    # Key: match entries by full path, compare C4 IDs and metadata
    raise NotImplementedError
