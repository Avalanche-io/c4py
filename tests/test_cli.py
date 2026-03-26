"""Tests for the CLI module (python -m c4py)."""

import subprocess
import sys
from pathlib import Path

from c4py.encoder import dump
from c4py.id import identify_bytes, identify_file
from c4py.scanner import scan


def _run_c4py(*args: str, input_data: bytes | None = None) -> subprocess.CompletedProcess[str]:
    """Run `python -m c4py` with the given arguments."""
    return subprocess.run(
        [sys.executable, "-m", "c4py", *args],
        capture_output=True,
        text=True,
        input=input_data.decode() if input_data else None,
        timeout=30,
    )


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


class TestVersion:
    """Test `python -m c4py version`."""

    def test_version_output(self):
        result = _run_c4py("version")
        assert result.returncode == 0
        assert "c4py" in result.stdout
        assert "1.0.12" in result.stdout

    def test_version_no_stderr(self):
        result = _run_c4py("version")
        assert result.stderr == ""


class TestNoCommand:
    """Test running with no subcommand."""

    def test_no_args_shows_help(self):
        result = _run_c4py()
        assert result.returncode == 1
        # Should print help/usage to stdout
        assert "usage" in result.stdout.lower() or "c4py" in result.stdout


class TestIdCommand:
    """Test `python -m c4py id`."""

    def test_id_file(self, tmp_path: Path):
        f = tmp_path / "hello.txt"
        f.write_text("Hello world")
        expected = identify_file(f)

        result = _run_c4py("id", "-i", str(f))
        assert result.returncode == 0
        assert str(expected) in result.stdout.strip()

    def test_id_file_bare(self, tmp_path: Path):
        f = tmp_path / "test.bin"
        f.write_bytes(b"\x00\x01\x02")
        expected = identify_file(f)

        result = _run_c4py("id", "-i", str(f))
        assert result.returncode == 0
        output = result.stdout.strip()
        # Bare mode: should be exactly the C4 ID
        assert output == str(expected)

    def test_id_directory_c4m(self, tmp_path: Path):
        root = _create_project(tmp_path)

        result = _run_c4py("id", str(root))
        assert result.returncode == 0
        # Should contain c4m format lines
        assert result.stdout.strip() != ""

    def test_id_directory_bare(self, tmp_path: Path):
        root = _create_project(tmp_path)
        manifest = scan(root)
        expected = manifest.compute_c4id()

        result = _run_c4py("id", "-i", str(root))
        assert result.returncode == 0
        assert result.stdout.strip() == str(expected)

    def test_id_nonexistent(self, tmp_path: Path):
        result = _run_c4py("id", str(tmp_path / "no_such_file"))
        assert result.returncode == 1
        assert "error" in result.stderr.lower()

    def test_id_file_no_flag(self, tmp_path: Path):
        """Without -i flag on a file, outputs the C4 ID."""
        f = tmp_path / "hello.txt"
        f.write_text("Hello world")
        expected = identify_file(f)

        result = _run_c4py("id", str(f))
        assert result.returncode == 0
        assert str(expected) in result.stdout


class TestDiffCommand:
    """Test `python -m c4py diff`."""

    def test_diff_identical(self, tmp_path: Path):
        root = _create_project(tmp_path)
        manifest = scan(root)

        c4m_a = tmp_path / "a.c4m"
        c4m_b = tmp_path / "b.c4m"
        dump(manifest, str(c4m_a))
        dump(manifest, str(c4m_b))

        result = _run_c4py("diff", str(c4m_a), str(c4m_b))
        assert result.returncode == 0
        assert "no differences" in result.stdout.lower()

    def test_diff_added_file(self, tmp_path: Path):
        root = _create_project(tmp_path)
        m_old = scan(root)
        c4m_old = tmp_path / "old.c4m"
        dump(m_old, str(c4m_old))

        # Add a file
        (root / "new_file.txt").write_text("new content")
        m_new = scan(root)
        c4m_new = tmp_path / "new.c4m"
        dump(m_new, str(c4m_new))

        result = _run_c4py("diff", str(c4m_old), str(c4m_new))
        assert result.returncode == 0
        assert "+" in result.stdout

    def test_diff_removed_file(self, tmp_path: Path):
        root = _create_project(tmp_path)
        m_old = scan(root)
        c4m_old = tmp_path / "old.c4m"
        dump(m_old, str(c4m_old))

        (root / "readme.txt").unlink()
        m_new = scan(root)
        c4m_new = tmp_path / "new.c4m"
        dump(m_new, str(c4m_new))

        result = _run_c4py("diff", str(c4m_old), str(c4m_new))
        assert result.returncode == 0
        assert "-" in result.stdout

    def test_diff_nonexistent_file(self, tmp_path: Path):
        result = _run_c4py("diff", str(tmp_path / "nope.c4m"), str(tmp_path / "nope2.c4m"))
        assert result.returncode == 1
        assert "error" in result.stderr.lower()


class TestVerifyCommand:
    """Test `python -m c4py verify`."""

    def test_verify_matching(self, tmp_path: Path):
        root = _create_project(tmp_path)
        manifest = scan(root)
        c4m_path = tmp_path / "project.c4m"
        dump(manifest, str(c4m_path))

        result = _run_c4py("verify", str(c4m_path), str(root))
        assert result.returncode == 0
        assert "ok" in result.stdout.lower()

    def test_verify_corrupt(self, tmp_path: Path):
        root = _create_project(tmp_path)
        manifest = scan(root)
        c4m_path = tmp_path / "project.c4m"
        dump(manifest, str(c4m_path))

        (root / "readme.txt").write_text("TAMPERED")

        result = _run_c4py("verify", str(c4m_path), str(root))
        assert result.returncode == 1
        assert "corrupt" in result.stdout.lower()

    def test_verify_missing(self, tmp_path: Path):
        root = _create_project(tmp_path)
        manifest = scan(root)
        c4m_path = tmp_path / "project.c4m"
        dump(manifest, str(c4m_path))

        (root / "data.bin").unlink()

        result = _run_c4py("verify", str(c4m_path), str(root))
        assert result.returncode == 1
        assert "missing" in result.stdout.lower()

    def test_verify_nonexistent_manifest(self, tmp_path: Path):
        root = _create_project(tmp_path)
        result = _run_c4py("verify", str(tmp_path / "nope.c4m"), str(root))
        assert result.returncode == 1
        assert "error" in result.stderr.lower()

    def test_verify_nonexistent_directory(self, tmp_path: Path):
        root = _create_project(tmp_path)
        manifest = scan(root)
        c4m_path = tmp_path / "project.c4m"
        dump(manifest, str(c4m_path))

        result = _run_c4py("verify", str(c4m_path), str(tmp_path / "no_dir"))
        assert result.returncode == 1
        assert "error" in result.stderr.lower()


class TestCatCommand:
    """Test `python -m c4py cat`."""

    def test_cat_invalid_id(self):
        result = _run_c4py("cat", "not-a-valid-c4id")
        assert result.returncode == 1
        assert "error" in result.stderr.lower()

    def test_cat_no_store(self):
        """With no store configured, cat should fail gracefully."""
        # Use a valid-looking C4 ID but expect no store
        valid_id = str(identify_bytes(b"test"))
        result = subprocess.run(
            [sys.executable, "-m", "c4py", "cat", valid_id],
            capture_output=True,
            text=True,
            timeout=30,
            env={  # noqa: E501
                **dict(__import__("os").environ),
                "C4_STORE": "",
                "HOME": "/tmp/c4py_test_no_home",
            },
        )
        assert result.returncode == 1
        assert "error" in result.stderr.lower()
