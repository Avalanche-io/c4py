"""C4M diff, patch, and merge — compare, version, and combine manifests.

The c4m format supports patch chains: a base manifest followed by zero or more
patches, each starting with the C4 ID of the preceding state. This makes c4m
files self-describing version histories.

Operations:
- diff(a, b) -> DiffResult with added/removed/modified/same (high-level view)
- patch_diff(old, new) -> str (c4m patch text, suitable for appending)
- apply_patch(base, patch) -> Manifest (apply one patch to a base)
- resolve_chain(manifest) -> Manifest (resolve all patches to final state)
- log_chain(manifest) -> list[PatchInfo] (enumerate patches with stats)
- merge(base, local, remote) -> (Manifest, list[Conflict]) (three-way merge)

The CLI mapping:
- c4py.diff()          <-> c4 diff (high-level comparison)
- c4py.patch_diff()    <-> c4 diff old new (produces c4m patch)
- c4py.apply_patch()   <-> internal step of c4 patch
- c4py.resolve_chain() <-> c4 patch project.c4m
- c4py.log_chain()     <-> c4 log project.c4m
- c4py.merge()         <-> c4 merge path1 path2 [path3...]

Reference: github.com/Avalanche-io/c4/c4m/operations.go
           github.com/Avalanche-io/c4/c4m/merge.go
           github.com/Avalanche-io/c4/c4m/chain.go
"""

from __future__ import annotations

from copy import deepcopy
from dataclasses import dataclass, field
from typing import Optional

from .entry import Entry
from .id import C4ID
from .manifest import Manifest


@dataclass
class DiffResult:
    """Result of comparing two manifests."""

    added: list[Entry] = field(default_factory=list)
    removed: list[Entry] = field(default_factory=list)
    modified: list[Entry] = field(default_factory=list)
    same: list[Entry] = field(default_factory=list)

    @property
    def is_empty(self) -> bool:
        """True if there are no differences."""
        return not self.added and not self.removed and not self.modified


@dataclass
class PatchInfo:
    """Summary of one patch in a chain."""

    index: int  # 1-based patch number
    c4id: C4ID | None  # C4 ID of the state after this patch
    added: int = 0
    removed: int = 0
    modified: int = 0
    is_base: bool = False  # True for the initial manifest (not a patch)
    entry_count: int = 0  # Total entries (only meaningful for base)


def _entry_paths(entries: list[Entry]) -> dict[str, Entry]:
    """Build a map from full path to entry by walking the entry list.

    Paths use forward slashes; directories end with '/'.
    Mirrors Go's EntryPaths in merge.go.
    """
    result: dict[str, Entry] = {}
    stack: list[str] = []

    for e in entries:
        while len(stack) > e.depth:
            stack.pop()

        full_path = "".join(stack) + e.name
        result[full_path] = e

        if e.is_dir():
            stack.append(e.name)

    return result


def _entries_equal(a: Entry, b: Entry) -> bool:
    """Compare two entries for equality in diff context.

    If both have C4 IDs, compare by C4 ID and mode.
    Otherwise compare all attributes.
    """
    if a.name != b.name:
        return False

    if a.c4id is not None and b.c4id is not None:
        return a.c4id == b.c4id and a.mode == b.mode

    return (
        a.mode == b.mode
        and a.size == b.size
        and a.timestamp == b.timestamp
        and a.target == b.target
    )


def _entries_identical(a: Entry, b: Entry) -> bool:
    """Check if two entries are exactly the same across all metadata fields.

    Used by patch semantics: an exact duplicate signals removal.
    """
    return (
        a.name == b.name
        and a.mode == b.mode
        and a.timestamp == b.timestamp
        and a.size == b.size
        and a.c4id == b.c4id
        and a.target == b.target
        and a.hard_link == b.hard_link
        and a.flow_direction == b.flow_direction
        and a.flow_target == b.flow_target
    )


def diff(a: Manifest, b: Manifest) -> DiffResult:
    """Compare two manifests and categorize entries.

    Args:
        a: the "old" manifest
        b: the "new" manifest

    Returns:
        DiffResult with entries categorized as added, removed, modified, or same.
        Entries are matched by full path (name + depth position in tree).
    """
    a_map = _entry_paths(a.entries)
    b_map = _entry_paths(b.entries)

    result = DiffResult()

    # Check entries in A
    for path, entry_a in a_map.items():
        if path in b_map:
            entry_b = b_map[path]
            if _entries_equal(entry_a, entry_b):
                result.same.append(entry_a)
            else:
                result.modified.append(entry_b)
        else:
            result.removed.append(entry_a)

    # Check entries only in B (added)
    for path, entry_b in b_map.items():
        if path not in a_map:
            result.added.append(entry_b)

    return result


def patch_diff(old: Manifest, new: Manifest) -> str:
    """Produce a c4m patch from old to new.

    The output is valid c4m text. Its first line is a bare C4 ID — the identity
    of the old (base) state. The remaining lines are patch entries.

    Patch entry semantics (matched by name/path):
    - Entry only in new -> addition (entry emitted as-is)
    - Entry in both, any metadata differs -> modification (new entry emitted)
    - Entry in both, all fields identical -> removal (exact duplicate signals deletion)

    This output can be appended to old.c4m to create a version chain:
        c4 diff before.c4m after.c4m >> before.c4m

    Returns:
        c4m patch text (str), starting with the base C4 ID line
    """
    old_id = old.compute_c4id()

    old_map = _entry_paths(old.entries)
    new_map = _entry_paths(new.entries)

    patch_entries: list[Entry] = []

    # Gather all unique paths
    all_paths = sorted(set(list(old_map.keys()) + list(new_map.keys())))

    for path in all_paths:
        old_entry = old_map.get(path)
        new_entry = new_map.get(path)

        if new_entry is not None and old_entry is None:
            # Addition — emit new entry
            patch_entries.append(new_entry)
        elif old_entry is not None and new_entry is None:
            # Removal — re-emit old entry (exact duplicate = removal)
            patch_entries.append(old_entry)
        elif old_entry is not None and new_entry is not None:
            if _entries_identical(old_entry, new_entry):
                # Identical — skip (no change)
                continue
            else:
                # Modification — emit new entry
                patch_entries.append(new_entry)

    # Build output: base C4 ID line + patch entries
    lines: list[str] = [str(old_id)]
    for entry in patch_entries:
        lines.append(entry.format())
    lines.append("")  # trailing newline
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Patch tree for apply_patch
# ---------------------------------------------------------------------------

class _PatchNode:
    """Tree node for patch application."""

    __slots__ = ("entry", "children")

    def __init__(self, entry: Entry | None = None) -> None:
        self.entry = entry
        self.children: dict[str, _PatchNode] = {}


def _build_patch_tree(entries: list[Entry]) -> _PatchNode:
    """Build a tree from a flat entry list."""
    root = _PatchNode()
    stack: list[_PatchNode] = [root]

    for e in entries:
        # Trim stack to correct depth
        if e.depth + 1 < len(stack):
            stack = stack[:e.depth + 1]
        parent = stack[e.depth]

        node = _PatchNode(entry=e)
        parent.children[e.name] = node

        if e.is_dir():
            while len(stack) <= e.depth + 1:
                stack.append(root)  # placeholder
            stack[e.depth + 1] = node

    return root


def _apply_patch_tree(base: _PatchNode, patch: _PatchNode) -> None:
    """Recursively apply patch changes to a base tree."""
    for name, p_node in list(patch.children.items()):
        b_node = base.children.get(name)

        if b_node is None:
            # Addition — graft entire subtree
            base.children[name] = p_node
            continue

        # Check if exact match (removal)
        if b_node.entry is not None and p_node.entry is not None:
            if _entries_identical(b_node.entry, p_node.entry):
                del base.children[name]
                continue

        # Clobber — replace entry
        b_node.entry = p_node.entry

        # For directories, recurse to apply child-level changes
        if (b_node.entry is not None and b_node.entry.is_dir()
                and p_node.entry is not None and p_node.entry.is_dir()):
            _apply_patch_tree(b_node, p_node)


def _flatten_patch_tree(node: _PatchNode, depth: int, result: list[Entry]) -> None:
    """Convert a tree back to a flat entry list."""
    for child in node.children.values():
        if child.entry is not None:
            e = deepcopy(child.entry)
            e.depth = depth
            result.append(e)
            if child.entry.is_dir():
                _flatten_patch_tree(child, depth + 1, result)


def apply_patch(base: Manifest, patch_entries: list[Entry]) -> Manifest:
    """Apply patch entries to a base manifest.

    Semantics:
    - Exact duplicate of base entry -> removal (entry and children deleted)
    - Same path, different content -> modification (replace entry, recurse for dirs)
    - New path -> addition

    Args:
        base: the manifest to patch
        patch_entries: entries from the patch section

    Returns:
        New manifest with patch applied
    """
    base_tree = _build_patch_tree(base.entries)
    patch_tree = _build_patch_tree(patch_entries)
    _apply_patch_tree(base_tree, patch_tree)

    entries: list[Entry] = []
    _flatten_patch_tree(base_tree, 0, entries)

    result = Manifest(entries=entries)
    result.sort_entries()
    return result


def resolve_chain(manifest: Manifest) -> Manifest:
    """Resolve all patches in a manifest to produce the final state.

    A c4m file can contain a base manifest followed by patches (separated by
    bare C4 ID lines). This function applies all patches sequentially.

    Equivalent to: c4 patch project.c4m

    Args:
        manifest: a manifest that may contain patch chains (patch_sections attribute)

    Returns:
        The fully resolved manifest (final state after all patches)
    """
    sections = getattr(manifest, "patch_sections", None)
    if not sections or len(sections) <= 1:
        return manifest

    # First section is the base
    current = Manifest(entries=list(sections[0]))

    # Apply subsequent patches
    for i in range(1, len(sections)):
        current = apply_patch(current, list(sections[i]))

    return current


def log_chain(manifest: Manifest) -> list[PatchInfo]:
    """Enumerate patches in a manifest chain.

    Equivalent to: c4 log project.c4m

    Returns:
        List of PatchInfo, one per version (base + each patch)
    """
    sections = getattr(manifest, "patch_sections", None)
    if not sections:
        # No patch chain — treat the entire manifest as the base
        info = PatchInfo(
            index=1,
            c4id=manifest.compute_c4id(),
            is_base=True,
            entry_count=len(manifest.entries),
        )
        return [info]

    result: list[PatchInfo] = []
    current = Manifest(entries=list(sections[0]))

    # Base section
    base_info = PatchInfo(
        index=1,
        c4id=current.compute_c4id(),
        is_base=True,
        entry_count=len(current.entries),
    )
    result.append(base_info)

    # Subsequent patches
    for i in range(1, len(sections)):
        patch_entries = list(sections[i])

        # Compute stats by diffing before/after
        prev_map = _entry_paths(current.entries)
        current = apply_patch(current, patch_entries)
        curr_map = _entry_paths(current.entries)

        added = 0
        removed = 0
        modified = 0

        for path in curr_map:
            if path not in prev_map:
                added += 1
        for path in prev_map:
            if path not in curr_map:
                removed += 1
            elif path in curr_map and not _entries_identical(prev_map[path], curr_map[path]):
                modified += 1

        patch_info = PatchInfo(
            index=i + 1,
            c4id=current.compute_c4id(),
            added=added,
            removed=removed,
            modified=modified,
            entry_count=len(current.entries),
        )
        result.append(patch_info)

    return result


# ---------------------------------------------------------------------------
# Three-way merge
# ---------------------------------------------------------------------------


@dataclass
class Conflict:
    """A merge conflict between two manifest entries at the same path.

    When two manifests modify the same entry differently, the winning entry
    (last-write-wins) goes into the merged manifest and the losing entry
    is recorded here. The Go CLI writes the loser with a .conflict suffix.
    """

    path: str
    local_entry: Entry | None
    remote_entry: Entry | None


def _merge_equal(a: Entry, b: Entry) -> bool:
    """Return True if two entries represent the same content.

    For files, this means the same C4 ID. For directories, existence is
    sufficient. For symlinks, same target. For flow links, same direction
    and target.
    """
    if a.is_dir() != b.is_dir():
        return False
    if a.is_dir():
        return a.flow_direction == b.flow_direction and a.flow_target == b.flow_target
    if a.is_symlink() or b.is_symlink():
        return a.is_symlink() == b.is_symlink() and a.target == b.target
    return a.c4id == b.c4id


def _conflict_name(path: str) -> str:
    """Append '.conflict' to a path, preserving directory trailing slash."""
    if path.endswith("/"):
        return path[:-1] + ".conflict/"
    return path + ".conflict"


def _path_to_depth(full_path: str) -> int:
    """Return the depth of an entry given its full path."""
    clean = full_path
    if clean.endswith("/"):
        clean = clean[:-1]
    return clean.count("/")


def _path_entry_name(full_path: str) -> str:
    """Return the Name field (last component) for a full path."""
    is_dir = full_path.endswith("/")
    clean = full_path.rstrip("/")
    idx = clean.rfind("/")
    name = clean if idx < 0 else clean[idx + 1:]
    if is_dir:
        name += "/"
    return name


def _ensure_dirs(m: dict[str, Entry]) -> None:
    """Create directory entries for any parent paths implied but not present."""
    dirs_to_add: list[str] = []
    for p in list(m.keys()):
        parts = p.rstrip("/").split("/")
        for i in range(1, len(parts)):
            dir_path = "/".join(parts[:i]) + "/"
            if dir_path not in m:
                dirs_to_add.append(dir_path)

    for d in dirs_to_add:
        if d in m:
            continue
        name = _path_entry_name(d)
        m[d] = Entry(
            name=name,
            mode=0o40755,
            size=-1,
            depth=_path_to_depth(d),
        )


def _rebuild_manifest(m: dict[str, Entry]) -> Manifest:
    """Construct a Manifest from a full-path -> entry map."""
    paths = sorted(m.keys())
    entries: list[Entry] = []
    for p in paths:
        e = deepcopy(m[p])
        e.name = _path_entry_name(p)
        e.depth = _path_to_depth(p)
        entries.append(e)

    return Manifest(entries=entries)


def _add_conflict(
    merged: dict[str, Entry],
    conflicts: list[Conflict],
    path: str,
    local: Entry | None,
    remote: Entry | None,
) -> None:
    """Add both versions to the merged map and record the conflict.

    The version with the newer timestamp keeps the original name; the other
    gets a '.conflict' suffix. If one side is None (delete-vs-modify), only
    the surviving version is included.
    """
    conflicts.append(Conflict(path=path, local_entry=local, remote_entry=remote))

    if local is None and remote is not None:
        merged[path] = deepcopy(remote)
        return
    if remote is None and local is not None:
        merged[path] = deepcopy(local)
        return
    if local is None or remote is None:
        return

    # LWW: newer timestamp keeps the original name
    winner, loser = local, remote
    if remote.timestamp > local.timestamp:
        winner, loser = remote, local

    merged[path] = deepcopy(winner)

    # Preserve the losing version with a .conflict suffix
    conflict_path = _conflict_name(path)
    merged[conflict_path] = deepcopy(loser)


def merge(
    base: Manifest | None,
    local: Manifest,
    remote: Manifest,
) -> tuple[Manifest, list[Conflict]]:
    """Three-way merge of two manifests against a common base.

    If base is None, uses an empty manifest as the base (first sync).
    Conflicts are resolved with last-write-wins (LWW): the newer entry
    keeps the original name, the older gets a .conflict suffix.

    Equivalent to: c4 merge path1 path2

    Args:
        base: common ancestor manifest (None for union)
        local: first manifest
        remote: second manifest

    Returns:
        Tuple of (merged manifest, list of conflicts)

    Reference: github.com/Avalanche-io/c4/c4m/merge.go
    """
    if base is None:
        base = Manifest()

    base_map = _entry_paths(base.entries)
    local_map = _entry_paths(local.entries)
    remote_map = _entry_paths(remote.entries)

    # Collect all unique paths
    all_paths = sorted(set(list(base_map.keys()) + list(local_map.keys()) + list(remote_map.keys())))

    merged: dict[str, Entry] = {}
    conflicts: list[Conflict] = []

    for p in all_paths:
        b: Optional[Entry] = base_map.get(p)
        loc: Optional[Entry] = local_map.get(p)
        r: Optional[Entry] = remote_map.get(p)

        # Only one side has it
        if b is None and loc is not None and r is None:
            merged[p] = deepcopy(loc)
        elif b is None and loc is None and r is not None:
            merged[p] = deepcopy(r)

        # Both added
        elif b is None and loc is not None and r is not None:
            if _merge_equal(loc, r):
                merged[p] = deepcopy(loc)
            else:
                _add_conflict(merged, conflicts, p, loc, r)

        # All three exist
        elif b is not None and loc is not None and r is not None:
            loc_changed = not _merge_equal(b, loc)
            r_changed = not _merge_equal(b, r)
            if not loc_changed and not r_changed:
                merged[p] = deepcopy(b)
            elif loc_changed and not r_changed:
                merged[p] = deepcopy(loc)
            elif not loc_changed and r_changed:
                merged[p] = deepcopy(r)
            else:
                if _merge_equal(loc, r):
                    merged[p] = deepcopy(loc)  # converged
                else:
                    _add_conflict(merged, conflicts, p, loc, r)

        # Remote deleted
        elif b is not None and loc is not None and r is None:
            if _merge_equal(b, loc):
                pass  # unchanged locally, remote deleted -> delete
            else:
                _add_conflict(merged, conflicts, p, loc, None)

        # Local deleted
        elif b is not None and loc is None and r is not None:
            if _merge_equal(b, r):
                pass  # unchanged remotely, local deleted -> delete
            else:
                _add_conflict(merged, conflicts, p, None, r)

        # Both deleted
        elif b is not None and loc is None and r is None:
            pass  # agreement: both deleted

    # Ensure parent directories exist
    _ensure_dirs(merged)

    result = _rebuild_manifest(merged)
    return result, conflicts
