"""The bundle's extract.sh never makes its own folder match the c4m.

extract.sh makes its target match the bundle's c4m. Through 1.0.26 the
target defaulted to ".", so `sh extract.sh` run inside the bundle deleted
the bundle's own extract.sh, store and c4m. The default is now a folder
named after the c4m, as c4sh's extract.sh does; an explicit target is
used as given.
"""

import os
import subprocess
import sys
from pathlib import Path

import pytest

from c4py.encoder import dump
from c4py.pool import pool
from c4py.scanner import scan
from c4py.store import FSStore

FILES = {
    "readme.txt": b"This is the readme",
    "data.bin": b"\xde\xad\xbe\xef",
    "assets/logo.png": b"PNG FAKE HEADER",
}


@pytest.fixture(autouse=True)
def _run_in_tmp_cwd(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.chdir(tmp_path)


def _bundle(tmp_path: Path, c4m_name: str) -> Path:
    """Scan a small project, write its c4m under c4m_name, and pool it."""
    src = tmp_path / "project"
    for rel, data in FILES.items():
        (src / rel).parent.mkdir(parents=True, exist_ok=True)
        (src / rel).write_bytes(data)
    store = FSStore(tmp_path / "store")
    manifest = scan(src, store=store)
    c4m_path = tmp_path / c4m_name
    dump(manifest, str(c4m_path))
    bundle = tmp_path / "bundle"
    pool(str(c4m_path), bundle, store=store)
    return bundle


def _run(script: Path, cwd: Path, *args: str) -> subprocess.CompletedProcess:
    """Run extract.sh with this interpreter as its python3 and no c4 CLI."""
    bin_dir = cwd.parent / "bin"
    bin_dir.mkdir(exist_ok=True)
    py = bin_dir / "python3"
    if not py.exists():
        # A wrapper, not a symlink: a symlinked venv interpreter would not
        # find its pyvenv.cfg, so c4py would not import.
        py.write_text(f'#!/bin/sh\nexec "{sys.executable}" "$@"\n')
        py.chmod(0o755)
    env = {"PATH": f"{bin_dir}{os.pathsep}/usr/bin{os.pathsep}/bin", "HOME": str(cwd.parent)}
    return subprocess.run(["sh", str(script), *args], cwd=cwd, env=env, capture_output=True, text=True)


def _assert_tree(root: Path) -> None:
    for rel, data in FILES.items():
        assert (root / rel).read_bytes() == data, rel


def _assert_bundle_intact(bundle: Path, c4m_name: str) -> None:
    assert (bundle / "extract.sh").is_file()
    assert (bundle / c4m_name).is_file()
    assert (bundle / "store").is_dir() and any((bundle / "store").rglob("c4*"))


@pytest.mark.skipif(sys.platform == "win32", reason="extract.sh is a POSIX shell script")
@pytest.mark.parametrize("c4m_name,folder", [("delivery.c4m", "delivery"), ("my delivery.c4m", "my delivery")])
def test_no_argument_inside_the_bundle_extracts_to_a_named_folder(tmp_path: Path, c4m_name: str, folder: str):
    bundle = _bundle(tmp_path, c4m_name)

    r = _run(bundle / "extract.sh", bundle)

    assert r.returncode == 0, r.stderr
    _assert_bundle_intact(bundle, c4m_name)
    _assert_tree(bundle / folder)


@pytest.mark.skipif(sys.platform == "win32", reason="extract.sh is a POSIX shell script")
def test_an_explicit_target_is_used_as_given(tmp_path: Path):
    bundle = _bundle(tmp_path, "delivery.c4m")
    out = tmp_path / "received"

    r = _run(bundle / "extract.sh", tmp_path, str(out))

    assert r.returncode == 0, r.stderr
    _assert_bundle_intact(bundle, "delivery.c4m")
    _assert_tree(out)
    assert not (bundle / "delivery").exists()


@pytest.mark.skipif(sys.platform == "win32", reason="extract.sh is a POSIX shell script")
def test_a_default_folder_that_holds_files_is_refused(tmp_path: Path):
    bundle = _bundle(tmp_path, "delivery.c4m")
    (bundle / "delivery").mkdir()
    (bundle / "delivery" / "notes.txt").write_text("unrelated")

    r = _run(bundle / "extract.sh", bundle)

    assert r.returncode == 1, r.stdout + r.stderr
    assert "delivery" in r.stderr and "empty target folder" in r.stderr, r.stderr
    assert (bundle / "delivery" / "notes.txt").read_text() == "unrelated"
    assert sorted(p.name for p in (bundle / "delivery").iterdir()) == ["notes.txt"]
    _assert_bundle_intact(bundle, "delivery.c4m")
