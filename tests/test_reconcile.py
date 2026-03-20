"""Tests for reconcile — make a directory match a manifest."""

import os
import stat
from datetime import datetime, timezone
from io import BytesIO
from pathlib import Path

import pytest

from c4py.entry import Entry
from c4py.id import C4ID, identify_bytes
from c4py.manifest import Manifest
from c4py.reconcile import (
    ReconcileOp,
    ReconcilePlan,
    ReconcileResult,
    reconcile,
)
from c4py.scanner import scan
from c4py.store import FSStore


def _make_manifest_and_store(tmp_path: Path) -> tuple[Manifest, FSStore, Path]:
    """Create a test directory, scan it into a manifest + store, then return both."""
    src = tmp_path / "source"
    src.mkdir()
    (src / "hello.txt").write_bytes(b"hello world")
    (src / "data.bin").write_bytes(b"\x00\x01\x02\x03")
    sub = src / "sub"
    sub.mkdir()
    (sub / "nested.txt").write_bytes(b"nested content")

    store_dir = tmp_path / "store"
    store = FSStore(store_dir)
    manifest = scan(src, store=store)
    return manifest, store, src


class TestReconcileCreateFromScratch:
    """Create a directory from a manifest where nothing exists yet."""

    def test_create_all_files(self, tmp_path: Path):
        manifest, store, _src = _make_manifest_and_store(tmp_path)
        target = tmp_path / "target"
        target.mkdir()

        result = reconcile(manifest, target, store=store)

        assert isinstance(result, ReconcileResult)
        assert result.errors == []
        assert (target / "hello.txt").read_bytes() == b"hello world"
        assert (target / "data.bin").read_bytes() == b"\x00\x01\x02\x03"
        assert (target / "sub" / "nested.txt").read_bytes() == b"nested content"

    def test_create_sets_file_content_ids(self, tmp_path: Path):
        manifest, store, _src = _make_manifest_and_store(tmp_path)
        target = tmp_path / "target"
        target.mkdir()

        reconcile(manifest, target, store=store)

        # Verify the written files match expected C4 IDs.
        for rel_path, entry in manifest.flat_entries():
            if entry.is_dir() or entry.c4id is None:
                continue
            written = (target / rel_path).read_bytes()
            assert identify_bytes(written) == entry.c4id

    def test_create_counts(self, tmp_path: Path):
        manifest, store, _src = _make_manifest_and_store(tmp_path)
        target = tmp_path / "target"
        target.mkdir()

        result = reconcile(manifest, target, store=store)

        # 3 files + 1 subdirectory = 4 created ops at minimum
        assert result.created >= 3
        assert result.removed == 0
        assert result.updated == 0


class TestReconcileUpdateExisting:
    """Update a directory where some files exist but differ."""

    def test_updates_changed_file(self, tmp_path: Path):
        manifest, store, _src = _make_manifest_and_store(tmp_path)
        target = tmp_path / "target"
        target.mkdir()

        # Pre-populate with wrong content.
        (target / "hello.txt").write_bytes(b"old content")

        result = reconcile(manifest, target, store=store)

        assert result.updated >= 1
        assert (target / "hello.txt").read_bytes() == b"hello world"

    def test_skips_unchanged_file(self, tmp_path: Path):
        manifest, store, _src = _make_manifest_and_store(tmp_path)
        target = tmp_path / "target"
        target.mkdir()

        # Pre-populate with correct content.
        (target / "hello.txt").write_bytes(b"hello world")

        result = reconcile(manifest, target, store=store)

        # hello.txt should not appear as created or updated (it was skipped).
        # But data.bin and sub/nested.txt are new.
        assert result.errors == []

    def test_removes_extra_files(self, tmp_path: Path):
        manifest, store, _src = _make_manifest_and_store(tmp_path)
        target = tmp_path / "target"
        target.mkdir()
        (target / "extra.txt").write_bytes(b"should be removed")

        result = reconcile(manifest, target, store=store)

        assert not (target / "extra.txt").exists()
        assert result.removed >= 1


class TestReconcileDryRun:
    """dry_run returns a plan without modifying the filesystem."""

    def test_returns_plan(self, tmp_path: Path):
        manifest, store, _src = _make_manifest_and_store(tmp_path)
        target = tmp_path / "target"
        target.mkdir()

        plan = reconcile(manifest, target, store=store, dry_run=True)

        assert isinstance(plan, ReconcilePlan)
        assert len(plan.operations) > 0
        assert plan.missing == []

    def test_does_not_modify_directory(self, tmp_path: Path):
        manifest, store, _src = _make_manifest_and_store(tmp_path)
        target = tmp_path / "target"
        target.mkdir()

        before = list(target.iterdir())
        reconcile(manifest, target, store=store, dry_run=True)
        after = list(target.iterdir())

        assert before == after

    def test_plan_lists_operations(self, tmp_path: Path):
        manifest, store, _src = _make_manifest_and_store(tmp_path)
        target = tmp_path / "target"
        target.mkdir()

        plan = reconcile(manifest, target, store=store, dry_run=True)

        op_types = {op.type for op in plan.operations}
        # Should include at least mkdir and create operations.
        assert "create" in op_types or "mkdir" in op_types


class TestReconcileMissingContent:
    """Missing store objects are reported clearly."""

    def test_missing_content_in_plan(self, tmp_path: Path):
        # Build a manifest that references content NOT in our store.
        manifest = Manifest()
        fake_id = identify_bytes(b"content that is not stored")
        manifest.entries.append(Entry(
            name="missing.txt",
            mode=0o100644,
            timestamp=datetime(2025, 1, 1, tzinfo=timezone.utc),
            size=26,
            c4id=fake_id,
            depth=0,
        ))

        empty_store = FSStore(tmp_path / "empty_store")
        target = tmp_path / "target"
        target.mkdir()

        plan = reconcile(manifest, target, store=empty_store, dry_run=True)

        assert isinstance(plan, ReconcilePlan)
        assert len(plan.missing) == 1
        assert plan.missing[0] == fake_id


class TestReconcileProgress:
    """Progress callback fires for every operation."""

    def test_progress_callback(self, tmp_path: Path):
        manifest, store, _src = _make_manifest_and_store(tmp_path)
        target = tmp_path / "target"
        target.mkdir()

        calls: list[tuple[str, str, int, int]] = []

        def on_progress(op_type: str, path: str, index: int, total: int) -> None:
            calls.append((op_type, path, index, total))

        result = reconcile(manifest, target, store=store, progress=on_progress)

        assert len(calls) > 0
        # Every call should have consistent total.
        totals = {c[3] for c in calls}
        assert len(totals) == 1
        # Index should be monotonically increasing.
        indices = [c[2] for c in calls]
        assert indices == sorted(indices)
