"""C4M manifest — a collection of entries describing a filesystem.

A manifest is the in-memory representation of a c4m file. It holds entries
(files, directories, links) with their metadata and C4 IDs.

Reference: github.com/Avalanche-io/c4/c4m/manifest.go
"""

from __future__ import annotations

from collections.abc import Callable, Iterator
from datetime import datetime
from fnmatch import fnmatch

from .entry import NULL_SIZE, NULL_TIMESTAMP, Entry, _human_size
from .id import C4ID


class Manifest:
    """A c4m manifest — describes a filesystem as a list of entries.

    Entries are ordered: files before directories at each level,
    then natural sort within each group.
    """

    def __init__(self, entries: list[Entry] | None = None) -> None:
        self.entries: list[Entry] = entries or []
        self.base: C4ID | None = None  # external base reference (patch format)
        self._range_data: dict[C4ID, object] = {}  # inline ID list data keyed by C4 ID
        self.patch_sections: list[object] = []  # patch chain sections

    def __iter__(self) -> Iterator[Entry]:
        return iter(self.entries)

    def __len__(self) -> int:
        return len(self.entries)

    def __getitem__(self, key: int | str) -> Entry:
        if isinstance(key, int):
            return self.entries[key]
        # Path-based lookup
        for path, entry in self.flat_entries():
            if path == key:
                return entry
        raise KeyError(key)

    def __contains__(self, item: object) -> bool:
        if isinstance(item, str):
            for path, _ in self.flat_entries():
                if path == item:
                    return True
            return False
        return NotImplemented  # type: ignore[no-any-return]

    def filter(self, pattern_or_func: str | Callable[[str, Entry], bool]) -> Manifest:
        """Return a new Manifest with filtered entries.

        If pattern_or_func is a string, glob-match against full path.
        If callable, call with (path, entry) and keep if True.
        """
        kept: list[Entry] = []
        if isinstance(pattern_or_func, str):
            for path, entry in self.flat_entries():
                if fnmatch(path, pattern_or_func):
                    kept.append(entry)
        else:
            for path, entry in self.flat_entries():
                if pattern_or_func(path, entry):
                    kept.append(entry)
        return Manifest(entries=kept)

    def duplicates(self) -> dict[C4ID, list[str]]:
        """Find entries with the same C4 ID at different paths.

        Returns {c4id: [path1, path2, ...]} for IDs appearing more than once.
        """
        by_id: dict[C4ID, list[str]] = {}
        for path, entry in self.flat_entries():
            if entry.c4id is None or entry.c4id.is_nil():
                continue
            by_id.setdefault(entry.c4id, []).append(path)
        return {cid: paths for cid, paths in by_id.items() if len(paths) > 1}

    def files(self) -> Iterator[tuple[str, Entry]]:
        """Like flat_entries() but only yields non-directory entries."""
        for path, entry in self.flat_entries():
            if not entry.is_dir():
                yield path, entry

    def directories(self) -> Iterator[tuple[str, Entry]]:
        """Like flat_entries() but only yields directory entries."""
        for path, entry in self.flat_entries():
            if entry.is_dir():
                yield path, entry

    def file_count(self) -> int:
        """Number of non-directory entries."""
        return sum(1 for _ in self.files())

    def dir_count(self) -> int:
        """Number of directory entries."""
        return sum(1 for _ in self.directories())

    def total_size(self) -> int:
        """Total size of all file entries in bytes."""
        total = 0
        for _, entry in self.files():
            if entry.size != NULL_SIZE:
                total += entry.size
        return total

    def human_total(self) -> str:
        """Human-readable total size ('8.4 TB', '256 MB', etc.)."""
        return _human_size(self.total_size())

    def summary(self) -> str:
        """Human-readable summary string.

        Example: '47,123 files, 892 directories, 8.4 TB total, 3,201 unique C4 IDs'
        """
        fc = self.file_count()
        dc = self.dir_count()
        ht = self.human_total()
        unique_ids: set[C4ID] = set()
        for _, entry in self.files():
            if entry.c4id is not None and not entry.c4id.is_nil():
                unique_ids.add(entry.c4id)
        uid_count = len(unique_ids)
        return (
            f"{fc:,} files, {dc:,} directories, "
            f"{ht} total, {uid_count:,} unique C4 IDs"
        )

    def sort_entries(self) -> None:
        """Sort entries into canonical order (files before dirs, natural sort).

        Uses hierarchical sorting: sort siblings within each parent, not globally.
        Files sort before directories at each depth level.

        The algorithm first builds parent-child associations by scanning entries
        in their original order (a depth-N+1 entry belongs to the most recent
        depth-N directory preceding it), then sorts siblings and emits in
        depth-first order.

        Reference: github.com/Avalanche-io/c4/c4m/manifest.go
        """
        from .naturalsort import natural_sort_key

        if not self.entries:
            return

        # Phase 1: Build parent-child tree from entry order.
        # Each entry is associated with its parent directory based on position:
        # a depth-N+1 entry belongs to the nearest preceding depth-N directory.
        #
        # children_of maps a parent index (-1 for root) to a list of
        # (entry, original_index) tuples.
        children_of: dict[int, list[tuple[Entry, int]]] = {-1: []}
        # dir_stack tracks the most recent directory at each depth.
        # dir_stack[d] = original index of the current directory at depth d.
        dir_stack: list[int] = []

        for i, entry in enumerate(self.entries):
            # Trim stack to current depth (we've left deeper directories)
            while len(dir_stack) > entry.depth:
                dir_stack.pop()

            # Determine parent
            if entry.depth == 0:
                parent_key = -1
            elif dir_stack:
                parent_key = dir_stack[-1]
            else:
                # Orphan: deeper entry with no parent directory seen yet
                parent_key = -1

            children_of.setdefault(parent_key, []).append((entry, i))

            # If this entry is a directory, push it onto the stack
            if entry.is_dir():
                # Ensure stack is exactly at the right depth
                while len(dir_stack) <= entry.depth:
                    dir_stack.append(-1)
                dir_stack[entry.depth] = i
                children_of.setdefault(i, [])

        # Phase 2: Deduplicate siblings by name (last occurrence wins).
        used = set()
        for parent_key in children_of:
            siblings = children_of[parent_key]
            seen: dict[str, int] = {}
            deduped: list[tuple[Entry, int]] = []
            for child_entry, child_idx in siblings:
                if child_entry.name in seen:
                    old_pos = seen[child_entry.name]
                    used.add(deduped[old_pos][1])
                    deduped[old_pos] = (child_entry, child_idx)
                else:
                    seen[child_entry.name] = len(deduped)
                    deduped.append((child_entry, child_idx))
            children_of[parent_key] = deduped

        # Phase 3: Sort each sibling group and emit depth-first.
        def sort_key(item: tuple[Entry, int]) -> tuple[bool, list[object]]:
            e = item[0]
            return (e.is_dir(), natural_sort_key(e.name))

        result: list[Entry] = []
        emitted = set()

        def emit(parent_key: int) -> None:
            siblings = children_of.get(parent_key, [])
            siblings.sort(key=sort_key)
            for child_entry, child_idx in siblings:
                if child_idx in used:
                    continue
                emitted.add(child_idx)
                result.append(child_entry)
                if child_entry.is_dir():
                    emit(child_idx)

        emit(-1)

        # Append orphaned entries (e.g., from incomplete chunks)
        for i, entry in enumerate(self.entries):
            if i not in emitted and i not in used:
                result.append(entry)

        self.entries = result

    def compute_c4id(self) -> C4ID:
        """Compute the C4 ID of this manifest.

        Sorts entries into canonical order, formats each in canonical form,
        then computes the C4 ID of the resulting UTF-8 text.

        This matches the Go implementation: Canonicalize(), then Canonical(),
        then c4.Identify().
        """
        from .id import identify_bytes

        # Make a copy to avoid modifying the original
        canonical = self.copy()

        # Canonicalize: propagate metadata from children to parents
        _propagate_metadata(canonical.entries)

        # Sort into canonical order
        canonical.sort_entries()

        # Build canonical text (top-level entries only, in canonical format)
        canonical_text = canonical._canonical_text()

        return identify_bytes(canonical_text.encode("utf-8"))

    def _canonical_text(self) -> str:
        """Generate canonical form text for C4 ID computation.

        Matches Go's Manifest.Canonical(): only top-level entries.
        """
        if not self.entries:
            return ""

        # Find minimum depth
        min_depth = min(e.depth for e in self.entries)

        # Collect top-level entries
        top_level = [e for e in self.entries if e.depth == min_depth]

        # Sort: files before dirs, then natural sort
        from .naturalsort import natural_sort_key

        def sort_key(e: Entry) -> tuple[bool, list[object]]:
            return (e.is_dir(), natural_sort_key(e.name))

        top_level.sort(key=sort_key)

        # Write canonical form
        lines: list[str] = []
        for entry in top_level:
            lines.append(entry.canonical())
            lines.append("\n")
        return "".join(lines)

    def flat_entries(self) -> Iterator[tuple[str, Entry]]:
        """Iterate entries with their full reconstructed paths.

        Yields (path, entry) tuples where path is the full relative path
        from the manifest root. This is the natural way to walk a manifest
        in pipeline scripts:

            for path, entry in manifest.flat_entries():
                if entry.is_dir():
                    continue
                print(f"{path}: {entry.c4id}")
        """
        path_stack: list[str] = []
        for entry in self.entries:
            while len(path_stack) > entry.depth:
                path_stack.pop()
            name = entry.name.rstrip("/")
            full_path = "/".join(path_stack + [name]) if path_stack else name
            if entry.is_dir():
                path_stack.append(name)
            yield full_path, entry

    def copy(self) -> Manifest:
        """Deep copy of this manifest."""
        from copy import deepcopy
        return deepcopy(self)


def _propagate_metadata(entries: list[Entry]) -> None:
    """Propagate metadata from children to parents.

    Resolves null values in directory entries by computing from children.
    Processes deepest directories first (reverse order).
    """
    for i in range(len(entries) - 1, -1, -1):
        entry = entries[i]
        if not entry.is_dir():
            continue
        has_null = (
            (entry.mode == 0 and not entry.is_dir() and not entry.is_symlink())
            or entry.timestamp == NULL_TIMESTAMP
            or entry.size < 0
        )
        if not has_null:
            continue

        children = _get_directory_children(entries, entry, i)

        if entry.size < 0:
            entry.size = _calculate_directory_size(children)

        if entry.timestamp == NULL_TIMESTAMP:
            entry.timestamp = _get_most_recent_modtime(children)


def _get_directory_children(entries: list[Entry], dir_entry: Entry, dir_idx: int) -> list[Entry]:
    """Get direct children of a directory entry."""
    children: list[Entry] = []
    dir_depth = dir_entry.depth
    collecting = False
    for i, e in enumerate(entries):
        if i == dir_idx:
            collecting = True
            continue
        if collecting:
            if e.depth == dir_depth + 1:
                children.append(e)
            elif e.depth <= dir_depth:
                break
    return children


def _calculate_directory_size(children: list[Entry]) -> int:
    """Compute total size of direct children plus the byte length of the
    directory's own canonical c4m content.  Nil-infectious."""
    total = 0
    for e in children:
        if e.size < 0:
            return NULL_SIZE
        total += e.size
    total += _c4m_content_size(children)
    return total


def _c4m_content_size(children: list[Entry]) -> int:
    """Byte length of the canonical c4m text for one directory level.

    Each child's canonical line is encoded as UTF-8 and followed by a newline.
    This mirrors the Go reference ``c4mContentSize``.
    """
    n = 0
    for e in children:
        n += len(e.canonical().encode("utf-8")) + 1  # +1 for '\n'
    return n


def _get_most_recent_modtime(children: list[Entry]) -> datetime:
    """Find most recent modification time. Nil-infectious."""
    most_recent: datetime | None = None
    for e in children:
        if e.timestamp == NULL_TIMESTAMP:
            return NULL_TIMESTAMP
        if most_recent is None or e.timestamp > most_recent:
            most_recent = e.timestamp
    return most_recent if most_recent is not None else NULL_TIMESTAMP
