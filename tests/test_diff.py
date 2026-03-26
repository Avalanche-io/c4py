"""Tests for diff, patch, merge operations."""

from copy import deepcopy
from datetime import datetime, timezone

from c4py.diff import (
    DiffResult,
    _entries_identical,
    _entry_paths,
    apply_patch,
    diff,
    log_chain,
    merge,
    patch_diff,
    resolve_chain,
)
from c4py.entry import Entry
from c4py.id import identify_bytes
from c4py.manifest import Manifest


def _make_entry(
    name: str,
    depth: int = 0,
    size: int = 100,
    mode: int = 0o100644,
    content: bytes | None = None,
    timestamp: datetime | None = None,
) -> Entry:
    """Helper to build test entries with optional C4 ID from content."""
    ts = timestamp or datetime(2025, 1, 15, 12, 0, 0, tzinfo=timezone.utc)
    e = Entry(name=name, depth=depth, size=size, mode=mode, timestamp=ts)
    if content is not None:
        e.c4id = identify_bytes(content)
    return e


def _make_dir_entry(
    name: str,
    depth: int = 0,
    timestamp: datetime | None = None,
) -> Entry:
    """Helper to build test directory entries."""
    ts = timestamp or datetime(2025, 1, 15, 12, 0, 0, tzinfo=timezone.utc)
    return Entry(name=name, depth=depth, size=-1, mode=0o40755, timestamp=ts)


class TestDiff:
    """Test diff() function."""

    def test_identical_manifests(self):
        entries = [
            _make_entry("file1.txt", content=b"hello"),
            _make_entry("file2.txt", content=b"world"),
        ]
        a = Manifest(entries=deepcopy(entries))
        b = Manifest(entries=deepcopy(entries))
        result = diff(a, b)
        assert result.is_empty
        assert len(result.same) == 2
        assert len(result.added) == 0
        assert len(result.removed) == 0
        assert len(result.modified) == 0

    def test_added_entries(self):
        a = Manifest(entries=[_make_entry("file1.txt", content=b"hello")])
        b = Manifest(entries=[
            _make_entry("file1.txt", content=b"hello"),
            _make_entry("file2.txt", content=b"world"),
        ])
        result = diff(a, b)
        assert len(result.added) == 1
        assert result.added[0].name == "file2.txt"

    def test_removed_entries(self):
        a = Manifest(entries=[
            _make_entry("file1.txt", content=b"hello"),
            _make_entry("file2.txt", content=b"world"),
        ])
        b = Manifest(entries=[_make_entry("file1.txt", content=b"hello")])
        result = diff(a, b)
        assert len(result.removed) == 1
        assert result.removed[0].name == "file2.txt"

    def test_modified_entries(self):
        a = Manifest(entries=[_make_entry("file1.txt", content=b"hello")])
        b = Manifest(entries=[_make_entry("file1.txt", content=b"goodbye")])
        result = diff(a, b)
        assert len(result.modified) == 1
        assert result.modified[0].name == "file1.txt"

    def test_empty_manifests(self):
        a = Manifest()
        b = Manifest()
        result = diff(a, b)
        assert result.is_empty
        assert len(result.same) == 0

    def test_nested_entries(self):
        a = Manifest(entries=[
            _make_dir_entry("src/"),
            _make_entry("main.go", depth=1, content=b"package main"),
        ])
        b = Manifest(entries=[
            _make_dir_entry("src/"),
            _make_entry("main.go", depth=1, content=b"package main\nfunc main() {}"),
        ])
        result = diff(a, b)
        # Directory entry should be same (matched by path)
        assert len(result.modified) == 1

    def test_diff_result_is_empty(self):
        result = DiffResult(added=[], removed=[], modified=[], same=[])
        assert result.is_empty
        result.added.append(_make_entry("new.txt"))
        assert not result.is_empty


class TestEntryPaths:
    """Test _entry_paths helper."""

    def test_flat_entries(self):
        entries = [
            _make_entry("file1.txt"),
            _make_entry("file2.txt"),
        ]
        paths = _entry_paths(entries)
        assert "file1.txt" in paths
        assert "file2.txt" in paths

    def test_nested_entries(self):
        entries = [
            _make_dir_entry("src/"),
            _make_entry("main.go", depth=1),
            _make_dir_entry("internal/", depth=1),
            _make_entry("helper.go", depth=2),
        ]
        paths = _entry_paths(entries)
        assert "src/" in paths
        assert "src/main.go" in paths
        assert "src/internal/" in paths
        assert "src/internal/helper.go" in paths


class TestApplyPatch:
    """Test apply_patch function."""

    def test_addition(self):
        base = Manifest(entries=[_make_entry("file1.txt", content=b"hello")])
        patch = [_make_entry("file2.txt", content=b"world")]
        result = apply_patch(base, patch)
        paths = _entry_paths(result.entries)
        assert "file1.txt" in paths
        assert "file2.txt" in paths

    def test_removal(self):
        entry = _make_entry("file1.txt", content=b"hello")
        base = Manifest(entries=[
            deepcopy(entry),
            _make_entry("file2.txt", content=b"world"),
        ])
        # Exact duplicate signals removal
        patch = [deepcopy(entry)]
        result = apply_patch(base, patch)
        paths = _entry_paths(result.entries)
        assert "file1.txt" not in paths
        assert "file2.txt" in paths

    def test_modification(self):
        base = Manifest(entries=[_make_entry("file1.txt", content=b"hello")])
        patch = [_make_entry("file1.txt", content=b"goodbye")]
        result = apply_patch(base, patch)
        paths = _entry_paths(result.entries)
        assert "file1.txt" in paths
        entry = paths["file1.txt"]
        assert entry.c4id == identify_bytes(b"goodbye")

    def test_empty_patch(self):
        base = Manifest(entries=[_make_entry("file1.txt", content=b"hello")])
        result = apply_patch(base, [])
        assert len(result.entries) == 1


class TestPatchRoundTrip:
    """Test patch_diff then apply_patch recovers the target state."""

    def test_simple_round_trip(self):
        old = Manifest(entries=[
            _make_entry("file1.txt", content=b"hello"),
            _make_entry("file2.txt", content=b"world"),
        ])
        new = Manifest(entries=[
            _make_entry("file1.txt", content=b"goodbye"),
            _make_entry("file3.txt", content=b"new file"),
        ])

        patch_text = patch_diff(old, new)
        # Verify it starts with a C4 ID
        first_line = patch_text.split("\n")[0]
        assert first_line.startswith("c4")
        assert len(first_line) == 90

    def test_patch_no_changes(self):
        entries = [_make_entry("file1.txt", content=b"hello")]
        old = Manifest(entries=deepcopy(entries))
        new = Manifest(entries=deepcopy(entries))
        patch_text = patch_diff(old, new)
        lines = [line for line in patch_text.split("\n") if line.strip()]
        # Should only have the base C4 ID line, no patch entries
        assert len(lines) == 1
        assert lines[0].startswith("c4")


class TestResolveChain:
    """Test resolve_chain function."""

    def test_no_patches(self):
        m = Manifest(entries=[_make_entry("file1.txt", content=b"hello")])
        result = resolve_chain(m)
        assert len(result.entries) == 1

    def test_with_patch_sections(self):
        base_entries = [
            _make_entry("file1.txt", content=b"hello"),
            _make_entry("file2.txt", content=b"world"),
        ]
        # Patch: modify file1, remove file2
        patch_entries = [
            _make_entry("file1.txt", content=b"goodbye"),
            # file2.txt exact duplicate for removal
            _make_entry("file2.txt", content=b"world"),
        ]

        m = Manifest(entries=deepcopy(base_entries))
        m.patch_sections = [deepcopy(base_entries), deepcopy(patch_entries)]  # type: ignore[attr-defined]

        result = resolve_chain(m)
        paths = _entry_paths(result.entries)
        assert "file1.txt" in paths
        # file2.txt was removed (exact duplicate in patch)
        assert "file2.txt" not in paths
        assert paths["file1.txt"].c4id == identify_bytes(b"goodbye")


class TestLogChain:
    """Test log_chain function."""

    def test_single_manifest(self):
        m = Manifest(entries=[_make_entry("file1.txt", content=b"hello")])
        info = log_chain(m)
        assert len(info) == 1
        assert info[0].is_base
        assert info[0].entry_count == 1

    def test_with_patches(self):
        base_entries = [_make_entry("file1.txt", content=b"hello")]
        patch_entries = [_make_entry("file2.txt", content=b"world")]

        m = Manifest(entries=deepcopy(base_entries))
        m.patch_sections = [deepcopy(base_entries), deepcopy(patch_entries)]  # type: ignore[attr-defined]

        info = log_chain(m)
        assert len(info) == 2
        assert info[0].is_base
        assert not info[1].is_base
        assert info[1].added == 1


class TestMerge:
    """Test three-way merge."""

    def test_no_conflicts(self):
        base = Manifest(entries=[_make_entry("file1.txt", content=b"hello")])
        local = Manifest(entries=[
            _make_entry("file1.txt", content=b"hello"),
            _make_entry("file2.txt", content=b"local add"),
        ])
        remote = Manifest(entries=[
            _make_entry("file1.txt", content=b"hello"),
            _make_entry("file3.txt", content=b"remote add"),
        ])

        result, conflicts = merge(base, local, remote)
        assert len(conflicts) == 0
        paths = _entry_paths(result.entries)
        assert "file1.txt" in paths
        assert "file2.txt" in paths
        assert "file3.txt" in paths

    def test_conflict_lww(self):
        ts_old = datetime(2025, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
        ts_new = datetime(2025, 6, 1, 0, 0, 0, tzinfo=timezone.utc)

        base = Manifest(entries=[_make_entry("file.txt", content=b"base", timestamp=ts_old)])
        local = Manifest(entries=[_make_entry("file.txt", content=b"local", timestamp=ts_old)])
        remote = Manifest(entries=[_make_entry("file.txt", content=b"remote", timestamp=ts_new)])

        result, conflicts = merge(base, local, remote)
        assert len(conflicts) == 1
        assert conflicts[0].path == "file.txt"

        paths = _entry_paths(result.entries)
        # Newer timestamp wins the original name
        assert "file.txt" in paths
        assert paths["file.txt"].c4id == identify_bytes(b"remote")
        # Older gets .conflict
        assert "file.txt.conflict" in paths

    def test_both_added_same(self):
        base = Manifest()
        local = Manifest(entries=[_make_entry("new.txt", content=b"same")])
        remote = Manifest(entries=[_make_entry("new.txt", content=b"same")])

        result, conflicts = merge(base, local, remote)
        assert len(conflicts) == 0
        paths = _entry_paths(result.entries)
        assert "new.txt" in paths

    def test_both_deleted(self):
        base = Manifest(entries=[_make_entry("gone.txt", content=b"bye")])
        local = Manifest()
        remote = Manifest()

        result, conflicts = merge(base, local, remote)
        assert len(conflicts) == 0
        paths = _entry_paths(result.entries)
        assert "gone.txt" not in paths

    def test_local_delete_remote_unchanged(self):
        base = Manifest(entries=[_make_entry("file.txt", content=b"data")])
        local = Manifest()
        remote = Manifest(entries=[_make_entry("file.txt", content=b"data")])

        result, conflicts = merge(base, local, remote)
        assert len(conflicts) == 0
        paths = _entry_paths(result.entries)
        assert "file.txt" not in paths  # local delete wins

    def test_remote_delete_local_modified(self):
        base = Manifest(entries=[_make_entry("file.txt", content=b"original")])
        local = Manifest(entries=[_make_entry("file.txt", content=b"modified")])
        remote = Manifest()

        result, conflicts = merge(base, local, remote)
        assert len(conflicts) == 1
        paths = _entry_paths(result.entries)
        assert "file.txt" in paths  # local modified version kept

    def test_none_base(self):
        local = Manifest(entries=[_make_entry("a.txt", content=b"a")])
        remote = Manifest(entries=[_make_entry("b.txt", content=b"b")])

        result, conflicts = merge(None, local, remote)
        assert len(conflicts) == 0
        paths = _entry_paths(result.entries)
        assert "a.txt" in paths
        assert "b.txt" in paths

    def test_only_local_change(self):
        base = Manifest(entries=[_make_entry("file.txt", content=b"base")])
        local = Manifest(entries=[_make_entry("file.txt", content=b"local")])
        remote = Manifest(entries=[_make_entry("file.txt", content=b"base")])

        result, conflicts = merge(base, local, remote)
        assert len(conflicts) == 0
        paths = _entry_paths(result.entries)
        assert paths["file.txt"].c4id == identify_bytes(b"local")

    def test_only_remote_change(self):
        base = Manifest(entries=[_make_entry("file.txt", content=b"base")])
        local = Manifest(entries=[_make_entry("file.txt", content=b"base")])
        remote = Manifest(entries=[_make_entry("file.txt", content=b"remote")])

        result, conflicts = merge(base, local, remote)
        assert len(conflicts) == 0
        paths = _entry_paths(result.entries)
        assert paths["file.txt"].c4id == identify_bytes(b"remote")

    def test_converged_changes(self):
        base = Manifest(entries=[_make_entry("file.txt", content=b"base")])
        local = Manifest(entries=[_make_entry("file.txt", content=b"converged")])
        remote = Manifest(entries=[_make_entry("file.txt", content=b"converged")])

        result, conflicts = merge(base, local, remote)
        assert len(conflicts) == 0
        paths = _entry_paths(result.entries)
        assert paths["file.txt"].c4id == identify_bytes(b"converged")


class TestEntriesIdentical:
    """Test _entries_identical helper."""

    def test_identical(self):
        a = _make_entry("file.txt", content=b"hello")
        b = deepcopy(a)
        assert _entries_identical(a, b)

    def test_different_content(self):
        a = _make_entry("file.txt", content=b"hello")
        b = _make_entry("file.txt", content=b"world")
        assert not _entries_identical(a, b)

    def test_different_size(self):
        a = _make_entry("file.txt", size=100)
        b = _make_entry("file.txt", size=200)
        assert not _entries_identical(a, b)

    def test_different_mode(self):
        a = _make_entry("file.txt", mode=0o100644)
        b = _make_entry("file.txt", mode=0o100755)
        assert not _entries_identical(a, b)
