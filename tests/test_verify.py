"""Tests for the verification pipeline (verify_tree)."""

from pathlib import Path

import pytest

from c4py.encoder import dump
from c4py.id import identify_file
from c4py.scanner import scan
from c4py.verify import CorruptEntry, VerifyReport, verify_tree


def _create_project(tmp_path: Path) -> Path:
    """Create a small test project directory."""
    root = tmp_path / "project"
    root.mkdir()
    (root / "readme.txt").write_text("Hello world")
    (root / "data.bin").write_bytes(b"\x00\x01\x02\x03")
    sub = root / "src"
    sub.mkdir()
    (sub / "main.py").write_text("print('hello')")
    return root


class TestVerifyTreeMatching:
    """Directory fully matches manifest."""

    def test_all_ok(self, tmp_path: Path):
        root = _create_project(tmp_path)
        manifest = scan(root)
        report = verify_tree(manifest, root)

        assert report.is_ok
        assert len(report.ok) > 0
        assert report.missing == []
        assert report.corrupt == []
        assert report.extra == []

    def test_all_ok_file_names(self, tmp_path: Path):
        root = _create_project(tmp_path)
        manifest = scan(root)
        report = verify_tree(manifest, root)

        ok_set = set(report.ok)
        assert "readme.txt" in ok_set
        assert "data.bin" in ok_set
        assert "src/main.py" in ok_set

    def test_from_c4m_file(self, tmp_path: Path):
        """Load manifest from a .c4m file path."""
        root = _create_project(tmp_path)
        manifest = scan(root)
        c4m_path = tmp_path / "project.c4m"
        dump(manifest, str(c4m_path))

        report = verify_tree(str(c4m_path), root)
        assert report.is_ok

    def test_from_pathlib_path(self, tmp_path: Path):
        """Accept pathlib.Path for both arguments."""
        root = _create_project(tmp_path)
        manifest = scan(root)
        c4m_path = tmp_path / "project.c4m"
        dump(manifest, str(c4m_path))

        report = verify_tree(c4m_path, root)
        assert report.is_ok


class TestVerifyTreeMissing:
    """Files in manifest but not on disk."""

    def test_missing_file(self, tmp_path: Path):
        root = _create_project(tmp_path)
        manifest = scan(root)

        # Delete a file after scanning
        (root / "readme.txt").unlink()

        report = verify_tree(manifest, root)
        assert not report.is_ok
        assert "readme.txt" in report.missing

    def test_missing_nested_file(self, tmp_path: Path):
        root = _create_project(tmp_path)
        manifest = scan(root)

        (root / "src" / "main.py").unlink()

        report = verify_tree(manifest, root)
        assert not report.is_ok
        assert "src/main.py" in report.missing

    def test_missing_all_files(self, tmp_path: Path):
        root = _create_project(tmp_path)
        manifest = scan(root)

        # Remove all files
        for p in root.rglob("*"):
            if p.is_file():
                p.unlink()

        report = verify_tree(manifest, root)
        assert not report.is_ok
        assert len(report.missing) == 3  # readme.txt, data.bin, src/main.py


class TestVerifyTreeCorrupt:
    """Files on disk with wrong C4 ID."""

    def test_corrupt_file(self, tmp_path: Path):
        root = _create_project(tmp_path)
        manifest = scan(root)

        # Modify file content after scanning
        (root / "readme.txt").write_text("TAMPERED CONTENT")

        report = verify_tree(manifest, root)
        assert not report.is_ok
        assert len(report.corrupt) == 1
        assert report.corrupt[0].path == "readme.txt"
        assert isinstance(report.corrupt[0].expected, type(report.corrupt[0].actual))
        assert report.corrupt[0].expected != report.corrupt[0].actual

    def test_corrupt_preserves_ids(self, tmp_path: Path):
        root = _create_project(tmp_path)
        manifest = scan(root)

        original_id = identify_file(root / "data.bin")
        (root / "data.bin").write_bytes(b"\xff\xff\xff")
        tampered_id = identify_file(root / "data.bin")

        report = verify_tree(manifest, root)
        corrupt = [c for c in report.corrupt if c.path == "data.bin"]
        assert len(corrupt) == 1
        assert corrupt[0].expected == original_id
        assert corrupt[0].actual == tampered_id

    def test_multiple_corrupt(self, tmp_path: Path):
        root = _create_project(tmp_path)
        manifest = scan(root)

        (root / "readme.txt").write_text("changed1")
        (root / "data.bin").write_bytes(b"changed2")

        report = verify_tree(manifest, root)
        assert len(report.corrupt) == 2


class TestVerifyTreeExtra:
    """Files on disk but not in manifest."""

    def test_extra_file(self, tmp_path: Path):
        root = _create_project(tmp_path)
        manifest = scan(root)

        # Add a file after scanning
        (root / "extra.txt").write_text("I should not be here")

        report = verify_tree(manifest, root)
        assert not report.is_ok
        assert "extra.txt" in report.extra

    def test_extra_nested_file(self, tmp_path: Path):
        root = _create_project(tmp_path)
        manifest = scan(root)

        (root / "src" / "bonus.py").write_text("surprise!")

        report = verify_tree(manifest, root)
        assert "src/bonus.py" in report.extra

    def test_extra_in_new_directory(self, tmp_path: Path):
        root = _create_project(tmp_path)
        manifest = scan(root)

        new_dir = root / "logs"
        new_dir.mkdir()
        (new_dir / "output.log").write_text("log data")

        report = verify_tree(manifest, root)
        assert "logs/output.log" in report.extra


class TestVerifyTreeEmpty:
    """Edge cases with empty manifests and directories."""

    def test_empty_manifest_empty_dir(self, tmp_path: Path):
        root = tmp_path / "empty"
        root.mkdir()
        manifest = scan(root)

        report = verify_tree(manifest, root)
        assert report.is_ok
        assert report.ok == []
        assert report.missing == []
        assert report.corrupt == []
        assert report.extra == []

    def test_empty_manifest_nonempty_dir(self, tmp_path: Path):
        root = tmp_path / "dir"
        root.mkdir()
        (root / "file.txt").write_text("content")

        from c4py.manifest import Manifest
        empty = Manifest()

        report = verify_tree(empty, root)
        assert not report.is_ok
        assert "file.txt" in report.extra


class TestVerifyTreeProgress:
    """Progress callback tests."""

    def test_progress_called(self, tmp_path: Path):
        root = _create_project(tmp_path)
        manifest = scan(root)

        calls: list[tuple[str, int, int]] = []

        def on_progress(path: str, index: int, total: int) -> None:
            calls.append((path, index, total))

        report = verify_tree(manifest, root, progress=on_progress)
        assert len(calls) > 0
        # All calls should have consistent total
        totals = {t for _, _, t in calls}
        assert len(totals) == 1

    def test_progress_indices_sequential(self, tmp_path: Path):
        root = _create_project(tmp_path)
        manifest = scan(root)

        indices: list[int] = []

        def on_progress(path: str, index: int, total: int) -> None:
            indices.append(index)

        verify_tree(manifest, root, progress=on_progress)
        assert indices == list(range(len(indices)))

    def test_progress_covers_all_paths(self, tmp_path: Path):
        root = _create_project(tmp_path)
        manifest = scan(root)

        # Add extra and remove one
        (root / "extra.txt").write_text("extra")
        (root / "readme.txt").unlink()

        paths: list[str] = []

        def on_progress(path: str, index: int, total: int) -> None:
            paths.append(path)

        report = verify_tree(manifest, root, progress=on_progress)
        # Progress should be called for every unique path
        expected_count = len(report.ok) + len(report.missing) + len(report.corrupt) + len(report.extra)
        assert len(paths) == expected_count


class TestVerifyTreeMixed:
    """Combinations of ok, missing, corrupt, and extra."""

    def test_all_categories(self, tmp_path: Path):
        root = _create_project(tmp_path)
        manifest = scan(root)

        # Corrupt one
        (root / "readme.txt").write_text("tampered")
        # Remove one
        (root / "data.bin").unlink()
        # Add one
        (root / "new.txt").write_text("new file")

        report = verify_tree(manifest, root)
        assert not report.is_ok
        assert "src/main.py" in report.ok
        assert "data.bin" in report.missing
        assert any(c.path == "readme.txt" for c in report.corrupt)
        assert "new.txt" in report.extra
