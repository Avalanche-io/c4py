"""A folder's recorded mode is applied after everything under it is written.

Through 1.0.26 ``reconcile`` gave a folder its mode as soon as it made it.
For a folder recorded without owner write or search, such as ``dr-xr-xr-x``
(0555) or ``d---------`` (0000), every file and folder under it then failed
to be written, and the failures came back only in the result's ``errors``
list, which a caller may never read. Now folder modes (and times) are set in
one pass after all writes, deepest first, as c4py main does.

The 0555 tree is real, built on disk and scanned. A 0000 folder can't be
scanned, so that case scans a 0755 tree and sets the folder's mode on its
entry through the API, never in c4m text.
"""

from __future__ import annotations

import os
import stat
from pathlib import Path

import pytest

from c4py import reconcile, scan
from c4py.store import FSStore

KNOWN = 1_700_000_000  # 2023-11-14T22:13:20Z


@pytest.fixture
def tmp(tmp_path):
    """tmp_path, with every folder given owner rwx afterwards so pytest can
    remove it. A top-down walk unlocks each folder before descending."""
    yield tmp_path
    for dirpath, dirnames, _ in os.walk(tmp_path, topdown=True):
        for d in dirnames:
            try:
                (Path(dirpath) / d).chmod(0o755)
            except OSError:
                pass


def _tree(root: Path, top: str) -> Path:
    """<top>/ holding f.txt and sub/g.txt, every path dated KNOWN."""
    (root / top / "sub").mkdir(parents=True)
    (root / top / "f.txt").write_bytes(b"f\n")
    (root / top / "sub" / "g.txt").write_bytes(b"g\n")
    for p in [root / top / "f.txt", root / top / "sub" / "g.txt", root / top / "sub", root / top]:
        os.utime(p, (KNOWN, KNOWN))
    return root


def _mode(p: Path) -> int:
    return stat.S_IMODE(os.stat(p).st_mode)


def test_a_read_only_folder_keeps_its_contents(tmp):
    src = _tree(tmp / "src", "ro")
    (src / "ro").chmod(0o555)
    store = FSStore(tmp / "store")
    m = scan(src, store=store)
    target = tmp / "out"
    target.mkdir()
    result = reconcile(m, target, store=store)
    assert result.errors == []
    assert (target / "ro" / "f.txt").read_bytes() == b"f\n"
    assert (target / "ro" / "sub" / "g.txt").read_bytes() == b"g\n"
    assert _mode(target / "ro") == 0o555


def test_a_read_only_tree_reads_back_at_its_id(tmp):
    src = _tree(tmp / "src", "ro")
    (src / "ro").chmod(0o555)
    store = FSStore(tmp / "store")
    m = scan(src, store=store)
    target = tmp / "out"
    target.mkdir()
    reconcile(m, target, store=store)
    assert scan(target).compute_c4id() == m.compute_c4id()


def test_a_folder_with_no_permissions_keeps_its_contents(tmp):
    src = _tree(tmp / "src", "locked")
    store = FSStore(tmp / "store")
    m = scan(src, store=store)
    locked = next(e for p, e in m.flat_entries() if p == "locked")
    locked.mode = stat.S_IFDIR  # d---------, set through the API
    target = tmp / "out"
    target.mkdir()
    result = reconcile(m, target, store=store)
    assert result.errors == []
    assert _mode(target / "locked") == 0o000
    (target / "locked").chmod(0o755)  # look inside
    assert (target / "locked" / "f.txt").read_bytes() == b"f\n"
    assert (target / "locked" / "sub" / "g.txt").read_bytes() == b"g\n"
    # The subfolder got its own recorded time before its parent was locked.
    assert os.stat(target / "locked" / "sub").st_mtime == KNOWN
