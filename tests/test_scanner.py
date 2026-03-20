"""Tests for the directory scanner."""

import os
import stat
from io import BytesIO
from pathlib import Path

import pytest

from c4py.id import identify_bytes
from c4py.manifest import Manifest
from c4py.scanner import scan
from c4py.store import FSStore


def _create_test_dir(tmp_path: Path) -> Path:
    """Create a test directory structure."""
    root = tmp_path / "project"
    root.mkdir()

    # Regular files
    (root / "readme.txt").write_text("Hello world")
    (root / "data.bin").write_bytes(b"\x00\x01\x02\x03")

    # Subdirectory with files
    src = root / "src"
    src.mkdir()
    (src / "main.py").write_text("print('hello')")
    (src / "util.py").write_text("def helper(): pass")

    # Nested subdirectory
    lib = src / "lib"
    lib.mkdir()
    (lib / "core.py").write_text("class Core: pass")

    return root


class TestScan:
    """Test scan() function."""

    def test_scan_basic(self, tmp_path: Path):
        root = _create_test_dir(tmp_path)
        m = scan(root)
        assert isinstance(m, Manifest)
        assert len(m.entries) > 0

    def test_scan_finds_all_files(self, tmp_path: Path):
        root = _create_test_dir(tmp_path)
        m = scan(root)

        names = [e.name for e in m.entries]
        assert "readme.txt" in names
        assert "data.bin" in names
        assert "main.py" in names
        assert "util.py" in names
        assert "core.py" in names

    def test_scan_finds_directories(self, tmp_path: Path):
        root = _create_test_dir(tmp_path)
        m = scan(root)

        dir_names = [e.name for e in m.entries if e.is_dir()]
        assert "src/" in dir_names

    def test_scan_computes_ids(self, tmp_path: Path):
        root = _create_test_dir(tmp_path)
        m = scan(root)

        # All regular files should have C4 IDs
        for entry in m.entries:
            if not entry.is_dir() and not entry.is_symlink():
                assert entry.c4id is not None, f"{entry.name} has no C4 ID"

    def test_scan_correct_id(self, tmp_path: Path):
        root = tmp_path / "simple"
        root.mkdir()
        (root / "hello.txt").write_bytes(b"hello")

        m = scan(root)
        entry = next(e for e in m.entries if e.name == "hello.txt")
        expected = identify_bytes(b"hello")
        assert entry.c4id == expected

    def test_scan_no_ids(self, tmp_path: Path):
        root = _create_test_dir(tmp_path)
        m = scan(root, compute_ids=False)

        for entry in m.entries:
            if not entry.is_dir():
                assert entry.c4id is None, f"{entry.name} should not have C4 ID"

    def test_scan_file_metadata(self, tmp_path: Path):
        root = tmp_path / "meta"
        root.mkdir()
        content = b"test content"
        (root / "file.txt").write_bytes(content)

        m = scan(root)
        entry = next(e for e in m.entries if e.name == "file.txt")

        assert entry.size == len(content)
        assert entry.mode != 0
        assert not entry.has_null_timestamp()

    def test_scan_empty_directory(self, tmp_path: Path):
        root = tmp_path / "empty"
        root.mkdir()

        m = scan(root)
        assert len(m.entries) == 0

    def test_scan_skips_hidden_files(self, tmp_path: Path):
        root = tmp_path / "hidden"
        root.mkdir()
        (root / ".hidden").write_text("secret")
        (root / "visible.txt").write_text("public")

        m = scan(root)
        names = [e.name for e in m.entries]
        assert ".hidden" not in names
        assert "visible.txt" in names

    def test_scan_skips_hidden_dirs(self, tmp_path: Path):
        root = tmp_path / "project"
        root.mkdir()
        (root / ".git").mkdir()
        (root / ".git" / "config").write_text("core")
        (root / "src").mkdir()
        (root / "src" / "main.py").write_text("code")

        m = scan(root)
        dir_names = [e.name for e in m.entries if e.is_dir()]
        assert ".git/" not in dir_names

    def test_scan_entry_depth(self, tmp_path: Path):
        root = tmp_path / "depths"
        root.mkdir()
        (root / "top.txt").write_text("top")
        sub = root / "sub"
        sub.mkdir()
        (sub / "middle.txt").write_text("middle")
        deep = sub / "deep"
        deep.mkdir()
        (deep / "bottom.txt").write_text("bottom")

        m = scan(root)

        top = next(e for e in m.entries if e.name == "top.txt")
        assert top.depth == 0

        mid = next(e for e in m.entries if e.name == "middle.txt")
        assert mid.depth == 1

        bot = next(e for e in m.entries if e.name == "bottom.txt")
        assert bot.depth == 2

    def test_scan_not_a_directory(self, tmp_path: Path):
        f = tmp_path / "file.txt"
        f.write_text("not a dir")
        with pytest.raises(ValueError, match="not a directory"):
            scan(f)


class TestScanWithStore:
    """Test scan with store integration."""

    def test_scan_stores_content(self, tmp_path: Path):
        root = tmp_path / "project"
        root.mkdir()
        content = b"stored content"
        (root / "file.txt").write_bytes(content)

        store_dir = tmp_path / "store"
        store = FSStore(store_dir)

        m = scan(root, store=store)

        entry = next(e for e in m.entries if e.name == "file.txt")
        assert entry.c4id is not None
        assert store.has(entry.c4id)

        # Verify stored content matches
        reader = store.get(entry.c4id)
        assert reader.read() == content
        reader.close()

    def test_scan_store_multiple_files(self, tmp_path: Path):
        root = tmp_path / "multi"
        root.mkdir()
        (root / "a.txt").write_bytes(b"aaa")
        (root / "b.txt").write_bytes(b"bbb")

        store_dir = tmp_path / "store"
        store = FSStore(store_dir)

        m = scan(root, store=store)

        for entry in m.entries:
            if not entry.is_dir() and entry.c4id is not None:
                assert store.has(entry.c4id)


class TestScanSymlinks:
    """Test scan with symlink handling."""

    def test_scan_symlink_target(self, tmp_path: Path):
        root = tmp_path / "links"
        root.mkdir()
        target = root / "target.txt"
        target.write_text("real content")
        link = root / "link.txt"
        link.symlink_to(target)

        m = scan(root, follow_symlinks=False)

        link_entry = next((e for e in m.entries if e.name == "link.txt"), None)
        if link_entry is not None:
            assert link_entry.target != ""

    def test_scan_follow_symlinks(self, tmp_path: Path):
        root = tmp_path / "follow"
        root.mkdir()
        target = root / "target.txt"
        target.write_bytes(b"real content")
        link = root / "link.txt"
        link.symlink_to(target)

        m = scan(root, follow_symlinks=True)

        # When following symlinks, the link should appear as a regular file
        link_entry = next((e for e in m.entries if e.name == "link.txt"), None)
        assert link_entry is not None
        assert link_entry.c4id == identify_bytes(b"real content")


class TestScanProgress:
    """Test scan with progress callback."""

    def test_progress_called_for_each_file(self, tmp_path: Path):
        root = tmp_path / "progress"
        root.mkdir()
        (root / "a.txt").write_bytes(b"aaa")
        (root / "b.txt").write_bytes(b"bbb")
        sub = root / "sub"
        sub.mkdir()
        (sub / "c.txt").write_bytes(b"ccc")

        calls = []
        def on_progress(path, completed, total):
            calls.append((path, completed, total))

        scan(root, progress=on_progress)

        # Should be called once per file (3 files)
        assert len(calls) == 3
        # Total is always 0 (unknown)
        for _, _, total in calls:
            assert total == 0
        # Completed should increment
        completed_values = [c for _, c, _ in calls]
        assert completed_values == [1, 2, 3]

    def test_progress_no_callback(self, tmp_path: Path):
        root = tmp_path / "nocb"
        root.mkdir()
        (root / "file.txt").write_bytes(b"data")
        # Should not raise when progress is None (default)
        m = scan(root)
        assert len(m.entries) == 1

    def test_progress_empty_dir(self, tmp_path: Path):
        root = tmp_path / "empty"
        root.mkdir()

        calls = []
        scan(root, progress=lambda p, c, t: calls.append((p, c, t)))
        assert len(calls) == 0
