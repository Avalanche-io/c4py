"""Tests for the directory scanner."""

import shutil
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

    def test_scan_empty_subdir_has_size_zero(self, tmp_path: Path):
        """An empty leaf directory scanned from disk has size 0, not null."""
        root = tmp_path / "project"
        root.mkdir()
        (root / "file.txt").write_text("content")
        empty_sub = root / "empty_dir"
        empty_sub.mkdir()

        m = scan(root)

        empty_entry = next(e for e in m.entries if e.name == "empty_dir/")
        assert empty_entry.size == 0, (
            f"empty directory size should be 0, got {empty_entry.size}"
        )
        assert not empty_entry.has_null_size()

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


def _paths(m: Manifest) -> dict:
    """Map full path to entry, walking the depth-ordered entry list."""
    out = {}
    stack: list[str] = []
    for e in m.entries:
        del stack[e.depth:]
        out["".join(stack) + e.name] = e
        if e.is_dir():
            stack.append(e.name)
    return out


def _direct_children(entries: list, dir_idx: int) -> list:
    d = entries[dir_idx].depth
    out = []
    for e in entries[dir_idx + 1:]:
        if e.depth <= d:
            break
        if e.depth == d + 1:
            out.append(e)
    return out


def _nested_tree(tmp_path: Path) -> Path:
    """Files at depth 0, a dir two deep (x/y/z/f.txt), a dir with two
    subdirs that both have children (two/a/a.txt, two/b/b.txt)."""
    root = tmp_path / "tree"
    root.mkdir()
    (root / "top.txt").write_text("top level file\n")
    (root / "x").mkdir()
    (root / "x" / "m.txt").write_text("m\n")
    (root / "x" / "y" / "z").mkdir(parents=True)
    (root / "x" / "y" / "z" / "f.txt").write_text("deep file\n")
    (root / "two" / "a").mkdir(parents=True)
    (root / "two" / "a" / "a.txt").write_text("aaa\n")
    (root / "two" / "b").mkdir()
    (root / "two" / "b" / "b.txt").write_text("bbbb\n")
    return root


class TestNestedDirectories:
    """Directory sizes and IDs for trees nested two or more levels deep."""

    def test_sibling_subdirs_nest_under_their_parent(self, tmp_path: Path):
        # os.walk order is not tree order once two sibling directories both
        # have children; the scanner must still place each child under its
        # own parent.
        m = scan(_nested_tree(tmp_path))
        assert set(_paths(m)) == {
            "top.txt",
            "two/", "two/a/", "two/a/a.txt", "two/b/", "two/b/b.txt",
            "x/", "x/m.txt", "x/y/", "x/y/z/", "x/y/z/f.txt",
        }

    def test_dir_size_sums_children_with_final_ids(self, tmp_path: Path):
        # size(dir) = sum over direct children of child.size +
        # len(child.canonical()) + 1, where a child directory's canonical
        # line carries its final 90-char C4 ID, not the null '-'.
        m = scan(_nested_tree(tmp_path))
        entries = m.entries
        for i, d in enumerate(entries):
            if not d.is_dir():
                continue
            children = _direct_children(entries, i)
            assert children, d.name
            for c in children:
                assert c.c4id is not None, f"{c.name} under {d.name} lacks an ID"
                assert len(c.canonical().split(" ")[-1]) == 90
            expected = sum(
                c.size + len(c.canonical().encode("utf-8")) + 1 for c in children
            )
            assert d.size == expected, f"{d.name}: {d.size} != {expected}"

        paths = _paths(m)
        z, y = paths["x/y/z/"], paths["x/y/"]
        assert z.size == 10 + len(paths["x/y/z/f.txt"].canonical()) + 1
        assert y.size == z.size + len(z.canonical()) + 1

    @pytest.mark.skipif(
        not (shutil.which("c4") or Path("/opt/homebrew/bin/c4").is_file()),
        reason="c4 CLI not installed",
    )
    def test_matches_go_cli_byte_for_byte(self, tmp_path: Path):
        import io
        import os
        import subprocess

        import c4py

        root = _nested_tree(tmp_path)
        c4 = shutil.which("c4") or "/opt/homebrew/bin/c4"
        env = dict(os.environ, HOME=str(tmp_path), C4_STORE=str(tmp_path / "store"))
        go = subprocess.run(
            [c4, "id", str(root)], capture_output=True, text=True, check=True, env=env
        ).stdout

        buf = io.StringIO()
        c4py.dump(c4py.scan(root), buf)
        assert buf.getvalue() == go
