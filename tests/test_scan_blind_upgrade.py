"""1.0.27 must upgrade blind, and a scan's C4 ID must be the C4 ID of its
own written text.

Blind upgrade: scan() then dumps() in both ID modes writes what c4py 1.0.26
(release/1.0.x 44dceb3) wrote, on a flat tree with an empty directory and a
nested one with an empty directory two levels down. vectors/blind-upgrade/
holds 1.0.26's output. A childless directory with no ID keeps a null size,
but a scan states 0 for a directory it listed as empty, so nothing changes.

The one deliberate change is a directory os.walk cannot list: 1.0.26
recorded it as empty (size 0, and in ID mode the empty listing's ID), a
claim nothing observed. It now keeps a null size and ID, as the Go
reference and c4py main record it, and the scan's C4 ID still equals the C4
ID of its own written text (test_unreadable_directory_keeps_nulls).

To rewrite the 1.0.26 goldens, point C4PY_BLIND_UPGRADE_WRITE at a 1.0.26
source tree's src/ directory.
"""

from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

import pytest

from c4py.decoder import loads
from c4py.encoder import dumps
from c4py.id import identify_bytes
from c4py.manifest import Manifest
from c4py.scanner import scan

GOLDEN = Path(__file__).parent / "vectors" / "blind-upgrade"
SRC = Path(__file__).resolve().parents[1] / "src"
WHEN = 1767323045  # 2026-01-02T03:04:05Z

_DUMP = (
    "import sys\n"
    "sys.path.insert(0, sys.argv[1])\n"
    "from c4py.scanner import scan\n"
    "from c4py.encoder import dumps\n"
    "sys.stdout.write(dumps(scan(sys.argv[2], compute_ids=sys.argv[3] == 'ids')))\n"
)


def _build(root: Path, files: dict[str, str], dirs: list[str]) -> Path:
    for d in dirs:
        (root / d).mkdir(parents=True, exist_ok=True)
    for rel, content in files.items():
        p = root / rel
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_text(content)
    paths = sorted((p for p in root.rglob("*")), key=lambda p: -len(p.parts))
    for p in paths:
        p.chmod(0o755 if p.is_dir() else 0o644)
        os.utime(p, (WHEN, WHEN))
    return root


def _dump(src: Path, tree: Path, mode: str) -> str:
    out = subprocess.run(
        [sys.executable, "-I", "-c", _DUMP, str(src), str(tree), mode],
        check=True, capture_output=True, text=True,
    )
    return out.stdout


@pytest.fixture
def trees(tmp_path: Path) -> dict[str, Path]:
    return {
        "flat": _build(tmp_path / "flat", {"a.txt": "a\n"}, ["empty"]),
        "nested": _build(tmp_path / "nested", {"z": "z\n", "a/f": "f\n"}, ["a/b/empty"]),
    }


@pytest.mark.parametrize("mode", ["ids", "noids"])
def test_scan_output_is_byte_identical_to_1_0_26(trees: dict[str, Path], mode: str):
    write_with = os.environ.get("C4PY_BLIND_UPGRADE_WRITE")
    for name, tree in trees.items():
        golden = GOLDEN / f"{name}-{mode}.c4m"
        if write_with:
            GOLDEN.mkdir(parents=True, exist_ok=True)
            golden.write_text(_dump(Path(write_with), tree, mode))
            continue
        assert _dump(SRC, tree, mode) == golden.read_text(), (name, mode)


@pytest.mark.skipif(sys.platform == "win32" or os.geteuid() == 0, reason="needs Unix permissions, non-root")
@pytest.mark.parametrize("mode", ["ids", "noids"])
def test_unreadable_directory_keeps_nulls(tmp_path: Path, mode: str):
    tree = _build(tmp_path / "unread", {"f": "f\n", "locked/inner": "in\n"}, ["empty"])
    (tree / "locked").chmod(0)
    try:
        m = scan(tree, compute_ids=(mode == "ids"))
    finally:
        (tree / "locked").chmod(0o755)
    by_name = {e.name: e for e in m.entries}
    assert by_name["locked/"].size < 0
    assert by_name["locked/"].c4id is None
    assert by_name["empty/"].size == 0
    _assert_self_id(m)


def _root_listing(m: Manifest) -> Manifest:
    return Manifest(entries=[e for e in m.entries if e.depth == 0])


def _assert_self_id(m: Manifest) -> None:
    """A scan's C4 ID is the C4 ID of its root's one-level written listing
    (for a flat scan, its whole written text)."""
    text = dumps(_root_listing(m))
    assert str(identify_bytes(text.encode("utf-8"))) == str(m.compute_c4id()), text
    assert str(loads(text).compute_c4id()) == str(m.compute_c4id())


@pytest.mark.parametrize("mode", ["ids", "noids"])
def test_scan_id_is_the_id_of_its_text(trees: dict[str, Path], mode: str):
    for tree in trees.values():
        _assert_self_id(scan(tree, compute_ids=(mode == "ids")))
