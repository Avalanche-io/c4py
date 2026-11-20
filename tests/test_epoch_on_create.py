"""A file or folder reconcile creates from a null timestamp gets the epoch.

A null timestamp (``-``) means the time is unknown. Applied to a path that
already exists it never overwrites (the path keeps its time). Applied to a
path this reconcile creates, the time becomes the epoch, which scans back as
null, so a tree materialised from a null-dated description reads back at the
description's C4 ID. Through 1.0.26 a created file or folder kept the time it
was written, so it read back dated, and at a different ID. Core's
``reconcile/apply.go`` (dfd6e12) is the rule followed here: a create, and an
overwrite of different content, are fresh; a folder is fresh when this apply
made it.

The null-dated inputs are real trees dated at the epoch and scanned, never
written by hand.
"""

from __future__ import annotations

import os
from pathlib import Path

from c4py import reconcile, scan
from c4py.entry import is_null_timestamp
from c4py.store import FSStore

KNOWN = 1_700_000_000  # 2023-11-14T22:13:20Z


def _epoch_tree(root: Path) -> Path:
    """a.txt and sub/deep/b.txt, every file and folder dated at the epoch."""
    (root / "sub" / "deep").mkdir(parents=True)
    (root / "a.txt").write_bytes(b"a\n")
    (root / "sub" / "deep" / "b.txt").write_bytes(b"b\n")
    for p in [root / "a.txt", root / "sub" / "deep" / "b.txt", root / "sub" / "deep", root / "sub"]:
        os.utime(p, (0, 0))
    return root


def _scan_to_store(src: Path, store: FSStore):
    m = scan(src, store=store)
    assert all(is_null_timestamp(e.timestamp) for e in m.entries), "source not null-dated"
    return m


def test_created_files_and_folders_get_the_epoch(tmp_path):
    store = FSStore(tmp_path / "store")
    m = _scan_to_store(_epoch_tree(tmp_path / "src"), store)
    target = tmp_path / "out"
    target.mkdir()
    result = reconcile(m, target, store=store)
    assert not result.errors
    for rel in ["a.txt", "sub", "sub/deep", "sub/deep/b.txt"]:
        assert os.stat(target / rel).st_mtime == 0, rel


def test_a_null_dated_tree_reads_back_at_its_id(tmp_path):
    store = FSStore(tmp_path / "store")
    m = _scan_to_store(_epoch_tree(tmp_path / "src"), store)
    target = tmp_path / "out"
    target.mkdir()
    reconcile(m, target, store=store)
    back = scan(target)
    assert all(is_null_timestamp(e.timestamp) for e in back.entries)
    assert back.compute_c4id() == m.compute_c4id()


def test_an_overwritten_file_is_fresh_too(tmp_path):
    store = FSStore(tmp_path / "store")
    m = _scan_to_store(_epoch_tree(tmp_path / "src"), store)
    target = tmp_path / "out"
    target.mkdir()
    (target / "a.txt").write_bytes(b"old content\n")
    os.utime(target / "a.txt", (KNOWN, KNOWN))
    reconcile(m, target, store=store)
    assert (target / "a.txt").read_bytes() == b"a\n"
    assert os.stat(target / "a.txt").st_mtime == 0


def test_a_null_never_overwrites_an_existing_path(tmp_path):
    store = FSStore(tmp_path / "store")
    m = _scan_to_store(_epoch_tree(tmp_path / "src"), store)
    target = tmp_path / "out"
    (target / "sub" / "deep").mkdir(parents=True)
    (target / "a.txt").write_bytes(b"a\n")  # already the right content
    for p in [target / "a.txt", target / "sub" / "deep", target / "sub"]:
        os.utime(p, (KNOWN, KNOWN))
    reconcile(m, target, store=store)
    # The existing file and folders are not given the epoch; only the file
    # this reconcile wrote, sub/deep/b.txt, takes it. sub/ keeps its time.
    # sub/deep/ gained a child, which moves a folder's time on a real
    # filesystem, and a null doesn't set it back.
    assert os.stat(target / "a.txt").st_mtime == KNOWN
    assert os.stat(target / "sub").st_mtime == KNOWN
    assert os.stat(target / "sub" / "deep").st_mtime != 0
    assert os.stat(target / "sub" / "deep" / "b.txt").st_mtime == 0


def test_a_known_time_is_still_applied(tmp_path):
    src = tmp_path / "src"
    (src / "sub").mkdir(parents=True)
    (src / "sub" / "c.txt").write_bytes(b"c\n")
    os.utime(src / "sub" / "c.txt", (KNOWN, KNOWN))
    os.utime(src / "sub", (KNOWN, KNOWN))
    store = FSStore(tmp_path / "store")
    m = scan(src, store=store)
    target = tmp_path / "out"
    target.mkdir()
    reconcile(m, target, store=store)
    assert os.stat(target / "sub" / "c.txt").st_mtime == KNOWN
    assert os.stat(target / "sub").st_mtime == KNOWN
    assert scan(target).compute_c4id() == m.compute_c4id()
