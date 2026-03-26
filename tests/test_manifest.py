"""Tests for manifest operations — sort_entries, compute_c4id, load, dump, diff, validate."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

import pytest

from c4py.entry import NULL_SIZE, Entry
from c4py.manifest import Manifest

VECTORS_PATH = Path(__file__).parent / "vectors" / "known_ids.json"


def load_vectors():
    with open(VECTORS_PATH) as f:
        return json.load(f)


class TestSortEntries:
    """Hierarchical sort: files before dirs, natural sort within groups."""

    def test_files_before_dirs(self):
        entries = [
            Entry(name="zdir/", mode=0o40755,
                  timestamp=datetime(2025, 6, 15, 12, 0, 0, tzinfo=timezone.utc),
                  size=0, c4id=None, depth=0),
            Entry(name="afile.txt", mode=0o100644,
                  timestamp=datetime(2025, 6, 15, 12, 0, 0, tzinfo=timezone.utc),
                  size=10, c4id=None, depth=0),
        ]
        m = Manifest(entries=entries)
        m.sort_entries()
        assert m[0].name == "afile.txt"
        assert m[1].name == "zdir/"

    def test_natural_sort_order(self):
        entries = [
            Entry(name="file10.txt", mode=0o100644,
                  timestamp=datetime(2025, 6, 15, 12, 0, 0, tzinfo=timezone.utc),
                  size=0, c4id=None, depth=0),
            Entry(name="file2.txt", mode=0o100644,
                  timestamp=datetime(2025, 6, 15, 12, 0, 0, tzinfo=timezone.utc),
                  size=0, c4id=None, depth=0),
            Entry(name="file1.txt", mode=0o100644,
                  timestamp=datetime(2025, 6, 15, 12, 0, 0, tzinfo=timezone.utc),
                  size=0, c4id=None, depth=0),
        ]
        m = Manifest(entries=entries)
        m.sort_entries()
        names = [e.name for e in m]
        assert names == ["file1.txt", "file2.txt", "file10.txt"]

    def test_hierarchical_sort_preserves_nesting(self):
        entries = [
            Entry(name="b.txt", mode=0o100644,
                  timestamp=datetime(2025, 6, 15, 12, 0, 0, tzinfo=timezone.utc),
                  size=0, c4id=None, depth=0),
            Entry(name="a/", mode=0o40755,
                  timestamp=datetime(2025, 6, 15, 12, 0, 0, tzinfo=timezone.utc),
                  size=0, c4id=None, depth=0),
            Entry(name="z.txt", mode=0o100644,
                  timestamp=datetime(2025, 6, 15, 12, 0, 0, tzinfo=timezone.utc),
                  size=0, c4id=None, depth=1),
            Entry(name="a.txt", mode=0o100644,
                  timestamp=datetime(2025, 6, 15, 12, 0, 0, tzinfo=timezone.utc),
                  size=0, c4id=None, depth=1),
        ]
        m = Manifest(entries=entries)
        m.sort_entries()
        # Files before dirs at root
        assert m[0].name == "b.txt"
        assert m[1].name == "a/"
        # Children of a/ sorted
        assert m[2].name == "a.txt"
        assert m[3].name == "z.txt"

    def test_empty_manifest(self):
        m = Manifest()
        m.sort_entries()  # Should not raise
        assert len(m) == 0

    def test_single_entry(self):
        m = Manifest(entries=[Entry(name="f", mode=0, size=0)])
        m.sort_entries()
        assert len(m) == 1

    def test_dirs_sorted_naturally(self):
        entries = [
            Entry(name="dir10/", mode=0o40755,
                  timestamp=datetime(2025, 6, 15, 12, 0, 0, tzinfo=timezone.utc),
                  size=0, c4id=None, depth=0),
            Entry(name="dir2/", mode=0o40755,
                  timestamp=datetime(2025, 6, 15, 12, 0, 0, tzinfo=timezone.utc),
                  size=0, c4id=None, depth=0),
            Entry(name="dir1/", mode=0o40755,
                  timestamp=datetime(2025, 6, 15, 12, 0, 0, tzinfo=timezone.utc),
                  size=0, c4id=None, depth=0),
        ]
        m = Manifest(entries=entries)
        m.sort_entries()
        names = [e.name for e in m]
        assert names == ["dir1/", "dir2/", "dir10/"]

    def test_sibling_dirs_children_not_misplaced(self):
        """Children of sibling directories must follow their own parent.

        Regression: sort_entries previously misplaced depth-1 file entries
        after the last directory when multiple sibling directories existed.
        """
        entries = [
            Entry(name="docs/", mode=0o40755, depth=0),
            Entry(name="guide.md", mode=0o100644, depth=1),
            Entry(name="src/", mode=0o40755, depth=0),
            Entry(name="main.py", mode=0o100644, depth=1),
            Entry(name="README.md", mode=0o100644, depth=0),
        ]
        m = Manifest(entries=entries)
        m.sort_entries()
        # Expected order: README.md, docs/, docs/guide.md, src/, src/main.py
        paths = [p for p, _ in m.flat_entries()]
        assert paths == ["README.md", "docs", "docs/guide.md", "src", "src/main.py"]


class TestComputeC4ID:
    """C4 ID computation from manifest content."""

    def test_manifest_c4id_matches_vector(self):
        vectors = load_vectors()
        for mv in vectors["manifest_vectors"]:
            import c4py
            manifest = c4py.loads(mv["canonical"])
            c4id = manifest.compute_c4id()
            assert str(c4id) == mv["manifest_c4id"], (
                f"C4 ID mismatch for: {mv['description']}\n"
                f"Expected: {mv['manifest_c4id']}\n"
                f"Got: {str(c4id)}"
            )

    def test_empty_manifest_c4id(self):
        m = Manifest()
        c4id = m.compute_c4id()
        # Empty manifest should produce the C4 ID of empty string
        from c4py.id import identify_bytes
        expected = identify_bytes(b"")
        assert c4id == expected

    def test_deterministic(self):
        """Same manifest always produces same C4 ID."""
        import c4py
        vectors = load_vectors()
        mv = vectors["manifest_vectors"][0]
        m1 = c4py.loads(mv["canonical"])
        m2 = c4py.loads(mv["canonical"])
        assert m1.compute_c4id() == m2.compute_c4id()


class TestManifestRoundTrip:
    """Parse canonical c4m text and re-encode — must produce identical output."""

    def test_simple_manifest(self):
        vectors = load_vectors()
        for mv in vectors["manifest_vectors"]:
            import c4py
            manifest = c4py.loads(mv["canonical"])
            output = c4py.dumps(manifest)
            assert output == mv["canonical"], (
                f"Round-trip failed for: {mv['description']}"
            )

    def test_manifest_c4id(self):
        vectors = load_vectors()
        for mv in vectors["manifest_vectors"]:
            import c4py
            manifest = c4py.loads(mv["canonical"])
            c4id = manifest.compute_c4id()
            assert str(c4id) == mv["manifest_c4id"], (
                f"C4 ID mismatch for: {mv['description']}"
            )


class TestDiff:
    """Manifest diff operations."""

    def test_identical_manifests(self):
        import c4py
        vectors = load_vectors()
        mv = vectors["manifest_vectors"][0]
        a = c4py.loads(mv["canonical"])
        b = c4py.loads(mv["canonical"])
        result = c4py.diff(a, b)
        assert len(result.added) == 0
        assert len(result.removed) == 0
        assert len(result.modified) == 0

    def test_empty_manifests(self):
        import c4py
        a = c4py.Manifest()
        b = c4py.Manifest()
        result = c4py.diff(a, b)
        assert len(result.added) == 0
        assert len(result.removed) == 0


class TestCopy:
    """Manifest copy."""

    def test_deep_copy(self):
        entries = [
            Entry(name="f.txt", mode=0o100644,
                  timestamp=datetime(2025, 6, 15, 12, 0, 0, tzinfo=timezone.utc),
                  size=10, c4id=None, depth=0),
        ]
        m = Manifest(entries=entries)
        cp = m.copy()
        assert len(cp) == len(m)
        assert cp[0].name == m[0].name
        # Modify copy should not affect original
        cp.entries[0].name = "modified.txt"
        assert m[0].name == "f.txt"


class TestFlatEntries:
    """flat_entries() path reconstruction."""

    def test_flat_paths(self):
        entries = [
            Entry(name="README.md", mode=0o100644,
                  timestamp=datetime(2025, 6, 15, 12, 0, 0, tzinfo=timezone.utc),
                  size=10, c4id=None, depth=0),
            Entry(name="src/", mode=0o40755,
                  timestamp=datetime(2025, 6, 15, 12, 0, 0, tzinfo=timezone.utc),
                  size=0, c4id=None, depth=0),
            Entry(name="main.go", mode=0o100644,
                  timestamp=datetime(2025, 6, 15, 12, 0, 0, tzinfo=timezone.utc),
                  size=20, c4id=None, depth=1),
        ]
        m = Manifest(entries=entries)
        paths = [path for path, _ in m.flat_entries()]
        assert paths == ["README.md", "src", "src/main.go"]


# ---------------------------------------------------------------------------
# Helper to build a manifest for collection tests
# ---------------------------------------------------------------------------

def _sample_manifest() -> Manifest:
    """Build a small manifest with known entries for testing collection methods."""
    from c4py.id import identify_bytes

    id_a = identify_bytes(b"hello")
    id_b = identify_bytes(b"world")
    id_c = identify_bytes(b"duplicate")  # will be reused

    entries = [
        Entry(name="README.md", mode=0o100644,
              timestamp=datetime(2025, 6, 15, 12, 0, 0, tzinfo=timezone.utc),
              size=100, c4id=id_a, depth=0),
        Entry(name="src/", mode=0o40755,
              timestamp=datetime(2025, 6, 15, 12, 0, 0, tzinfo=timezone.utc),
              size=0, c4id=None, depth=0),
        Entry(name="main.go", mode=0o100644,
              timestamp=datetime(2025, 6, 15, 12, 0, 0, tzinfo=timezone.utc),
              size=2000, c4id=id_b, depth=1),
        Entry(name="util.go", mode=0o100644,
              timestamp=datetime(2025, 6, 15, 12, 0, 0, tzinfo=timezone.utc),
              size=500, c4id=id_c, depth=1),
        Entry(name="assets/", mode=0o40755,
              timestamp=datetime(2025, 6, 15, 12, 0, 0, tzinfo=timezone.utc),
              size=0, c4id=None, depth=0),
        Entry(name="logo.exr", mode=0o100644,
              timestamp=datetime(2025, 6, 15, 12, 0, 0, tzinfo=timezone.utc),
              size=5_000_000_000, c4id=id_c, depth=1),  # duplicate of util.go
    ]
    return Manifest(entries=entries)


class TestGetItem:
    """Path-based __getitem__ lookup."""

    def test_path_lookup(self):
        m = _sample_manifest()
        entry = m["README.md"]
        assert entry.name == "README.md"

    def test_nested_path_lookup(self):
        m = _sample_manifest()
        entry = m["src/main.go"]
        assert entry.name == "main.go"
        assert entry.size == 2000

    def test_missing_path_raises_keyerror(self):
        m = _sample_manifest()
        with pytest.raises(KeyError):
            m["nonexistent.txt"]

    def test_int_index_still_works(self):
        m = _sample_manifest()
        entry = m[0]
        assert entry.name == "README.md"


class TestContains:
    """Path-based __contains__."""

    def test_existing_path(self):
        m = _sample_manifest()
        assert "README.md" in m

    def test_nested_path(self):
        m = _sample_manifest()
        assert "src/main.go" in m

    def test_missing_path(self):
        m = _sample_manifest()
        assert "nonexistent.txt" not in m

    def test_directory_path(self):
        m = _sample_manifest()
        assert "src" in m


class TestFilter:
    """filter() with glob patterns and callables."""

    def test_glob_pattern(self):
        m = _sample_manifest()
        result = m.filter("*.go")
        paths = [p for p, _ in result.flat_entries()]
        assert "main.go" in paths
        assert "util.go" in paths

    def test_glob_exr(self):
        m = _sample_manifest()
        result = m.filter("*.exr")
        assert len(result) == 1
        assert result[0].name == "logo.exr"

    def test_nested_glob(self):
        m = _sample_manifest()
        result = m.filter("src/*")
        paths = [p for p, _ in result.flat_entries()]
        assert "main.go" in paths

    def test_lambda_filter(self):
        m = _sample_manifest()
        big = m.filter(lambda p, e: e.size > 1_000_000_000)
        assert len(big) == 1
        assert big[0].name == "logo.exr"

    def test_lambda_filter_no_match(self):
        m = _sample_manifest()
        result = m.filter(lambda p, e: e.size > 100_000_000_000)
        assert len(result) == 0


class TestDuplicates:
    """duplicates() finds entries with the same C4 ID."""

    def test_finds_duplicates(self):
        m = _sample_manifest()
        dupes = m.duplicates()
        # id_c is shared by util.go and logo.exr
        assert len(dupes) == 1
        paths = list(dupes.values())[0]
        assert "src/util.go" in paths
        assert "assets/logo.exr" in paths

    def test_no_duplicates(self):
        from c4py.id import identify_bytes
        entries = [
            Entry(name="a.txt", mode=0o100644,
                  timestamp=datetime(2025, 6, 15, 12, 0, 0, tzinfo=timezone.utc),
                  size=10, c4id=identify_bytes(b"unique1"), depth=0),
            Entry(name="b.txt", mode=0o100644,
                  timestamp=datetime(2025, 6, 15, 12, 0, 0, tzinfo=timezone.utc),
                  size=10, c4id=identify_bytes(b"unique2"), depth=0),
        ]
        m = Manifest(entries=entries)
        assert m.duplicates() == {}


class TestFilesAndDirectories:
    """files() and directories() iterators."""

    def test_files_only(self):
        m = _sample_manifest()
        file_paths = [p for p, _ in m.files()]
        assert "README.md" in file_paths
        assert "src/main.go" in file_paths
        # Directories excluded
        assert "src" not in file_paths
        assert "assets" not in file_paths

    def test_directories_only(self):
        m = _sample_manifest()
        dir_paths = [p for p, _ in m.directories()]
        assert "src" in dir_paths
        assert "assets" in dir_paths
        # Files excluded
        assert "README.md" not in dir_paths

    def test_files_count_matches(self):
        m = _sample_manifest()
        assert len(list(m.files())) == m.file_count()

    def test_dirs_count_matches(self):
        m = _sample_manifest()
        assert len(list(m.directories())) == m.dir_count()


class TestSummary:
    """summary() format."""

    def test_summary_format(self):
        m = _sample_manifest()
        s = m.summary()
        # Should contain files, directories, total, unique C4 IDs
        assert "4 files" in s
        assert "2 directories" in s
        assert "unique C4 IDs" in s
        assert "total" in s

    def test_empty_manifest_summary(self):
        m = Manifest()
        s = m.summary()
        assert "0 files" in s
        assert "0 directories" in s
        assert "0 B total" in s
        assert "0 unique C4 IDs" in s


class TestHumanSize:
    """_human_size and Entry.human_size()."""

    def test_bytes(self):
        from c4py.entry import _human_size
        assert _human_size(0) == "0 B"
        assert _human_size(512) == "512 B"
        assert _human_size(1023) == "1023 B"

    def test_kilobytes(self):
        from c4py.entry import _human_size
        assert _human_size(1024) == "1.0 KB"
        assert _human_size(1536) == "1.5 KB"

    def test_megabytes(self):
        from c4py.entry import _human_size
        assert _human_size(1024 * 1024) == "1.0 MB"

    def test_gigabytes(self):
        from c4py.entry import _human_size
        assert _human_size(1024 ** 3) == "1.0 GB"
        result = _human_size(int(4.2 * 1024 ** 3))
        assert result == "4.2 GB"

    def test_terabytes(self):
        from c4py.entry import _human_size
        result = _human_size(int(8.4 * 1024 ** 4))
        assert result == "8.4 TB"

    def test_entry_human_size(self):
        e = Entry(name="big.exr", size=5_000_000_000)
        hs = e.human_size()
        assert "GB" in hs

    def test_entry_null_size(self):
        e = Entry(name="unknown.bin", size=NULL_SIZE)
        assert e.human_size() == "-"


class TestFileDirCountTotalSize:
    """file_count(), dir_count(), total_size(), human_total()."""

    def test_file_count(self):
        m = _sample_manifest()
        assert m.file_count() == 4

    def test_dir_count(self):
        m = _sample_manifest()
        assert m.dir_count() == 2

    def test_total_size(self):
        m = _sample_manifest()
        # 100 + 2000 + 500 + 5_000_000_000
        assert m.total_size() == 100 + 2000 + 500 + 5_000_000_000

    def test_human_total(self):
        m = _sample_manifest()
        ht = m.human_total()
        assert "GB" in ht
