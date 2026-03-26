"""Tests for pool and ingest — portable content bundles."""

import os
from pathlib import Path

from c4py.manifest import Manifest
from c4py.pool import IngestResult, PoolResult, ingest, pool
from c4py.scanner import scan
from c4py.store import FSStore


def _make_project(tmp_path: Path) -> tuple[Manifest, FSStore, Path]:
    """Create a small project, scan it into a manifest + store."""
    src = tmp_path / "project"
    src.mkdir()
    (src / "readme.txt").write_bytes(b"This is the readme")
    (src / "data.bin").write_bytes(b"\xde\xad\xbe\xef")
    sub = src / "assets"
    sub.mkdir()
    (sub / "logo.png").write_bytes(b"PNG FAKE HEADER")

    store_dir = tmp_path / "store"
    store = FSStore(store_dir)
    manifest = scan(src, store=store)
    return manifest, store, src


class TestPool:
    """pool() bundles a manifest + store objects."""

    def test_creates_bundle_directory(self, tmp_path: Path):
        manifest, store, _src = _make_project(tmp_path)
        bundle = tmp_path / "bundle"

        result = pool(manifest, bundle, store=store)

        assert isinstance(result, PoolResult)
        assert bundle.is_dir()

    def test_bundle_contains_store(self, tmp_path: Path):
        manifest, store, _src = _make_project(tmp_path)
        bundle = tmp_path / "bundle"

        pool(manifest, bundle, store=store)

        pool_store = bundle / "store"
        assert pool_store.is_dir()

    def test_bundle_contains_c4m(self, tmp_path: Path):
        manifest, store, _src = _make_project(tmp_path)
        bundle = tmp_path / "bundle"

        pool(manifest, bundle, store=store)

        c4m_files = [f for f in bundle.iterdir() if f.suffix == ".c4m"]
        assert len(c4m_files) == 1

    def test_bundle_contains_extract_script(self, tmp_path: Path):
        manifest, store, _src = _make_project(tmp_path)
        bundle = tmp_path / "bundle"

        pool(manifest, bundle, store=store)

        script = bundle / "extract.sh"
        assert script.is_file()
        assert os.access(str(script), os.X_OK)

    def test_copies_all_objects(self, tmp_path: Path):
        manifest, store, _src = _make_project(tmp_path)
        bundle = tmp_path / "bundle"

        result = pool(manifest, bundle, store=store)

        # 3 files in the manifest.
        assert result.copied == 3
        assert result.skipped == 0
        assert result.missing == 0

    def test_skips_existing_objects(self, tmp_path: Path):
        manifest, store, _src = _make_project(tmp_path)
        bundle = tmp_path / "bundle"

        # Pool twice — second time should skip everything.
        pool(manifest, bundle, store=store)
        result2 = pool(manifest, bundle, store=store)

        assert result2.copied == 0
        assert result2.skipped == 3

    def test_reports_missing_objects(self, tmp_path: Path):
        manifest, _store, _src = _make_project(tmp_path)

        # Use a completely empty store.
        empty_store = FSStore(tmp_path / "empty_store")
        bundle = tmp_path / "bundle"

        result = pool(manifest, bundle, store=empty_store)

        assert result.missing == 3
        assert result.copied == 0

    def test_pool_from_c4m_path(self, tmp_path: Path):
        manifest, store, _src = _make_project(tmp_path)
        bundle = tmp_path / "bundle"

        # Write c4m file first, then pool from path.
        from c4py.encoder import dump
        c4m_path = tmp_path / "project.c4m"
        dump(manifest, str(c4m_path))

        result = pool(c4m_path, bundle, store=store)

        assert result.copied == 3
        c4m_in_bundle = bundle / "project.c4m"
        assert c4m_in_bundle.is_file()

    def test_pool_store_objects_are_valid(self, tmp_path: Path):
        manifest, store, _src = _make_project(tmp_path)
        bundle = tmp_path / "bundle"

        pool(manifest, bundle, store=store)

        pool_store = FSStore(bundle / "store")
        for _rel_path, entry in manifest.flat_entries():
            if entry.is_dir() or entry.c4id is None:
                continue
            assert pool_store.has(entry.c4id)
            # Verify round-trip content matches.
            original = store.get(entry.c4id).read()
            pooled = pool_store.get(entry.c4id).read()
            assert original == pooled


class TestIngest:
    """ingest() absorbs a bundle into the local store."""

    def test_ingest_copies_objects(self, tmp_path: Path):
        manifest, store, _src = _make_project(tmp_path)
        bundle = tmp_path / "bundle"
        pool(manifest, bundle, store=store)

        # Ingest into a fresh local store.
        local_store = FSStore(tmp_path / "local_store")

        result = ingest(bundle, store=local_store)

        assert isinstance(result, IngestResult)
        assert result.copied == 3
        assert result.skipped == 0

    def test_ingest_skips_existing(self, tmp_path: Path):
        manifest, store, _src = _make_project(tmp_path)
        bundle = tmp_path / "bundle"
        pool(manifest, bundle, store=store)

        # Ingest twice — second time should skip.
        local_store = FSStore(tmp_path / "local_store")
        ingest(bundle, store=local_store)
        result2 = ingest(bundle, store=local_store)

        assert result2.copied == 0
        assert result2.skipped == 3

    def test_ingest_copies_c4m_to_cwd(self, tmp_path: Path, monkeypatch):
        manifest, store, _src = _make_project(tmp_path)
        bundle = tmp_path / "bundle"
        pool(manifest, bundle, store=store)

        work_dir = tmp_path / "workdir"
        work_dir.mkdir()
        monkeypatch.chdir(work_dir)

        local_store = FSStore(tmp_path / "local_store")
        result = ingest(bundle, store=local_store)

        assert len(result.manifests) == 1
        c4m_name = result.manifests[0]
        assert (work_dir / c4m_name).is_file()

    def test_ingest_objects_are_valid(self, tmp_path: Path):
        manifest, store, _src = _make_project(tmp_path)
        bundle = tmp_path / "bundle"
        pool(manifest, bundle, store=store)

        local_store = FSStore(tmp_path / "local_store")
        ingest(bundle, store=local_store)

        for _rel_path, entry in manifest.flat_entries():
            if entry.is_dir() or entry.c4id is None:
                continue
            assert local_store.has(entry.c4id)
            original = store.get(entry.c4id).read()
            local = local_store.get(entry.c4id).read()
            assert original == local


class TestPoolIngestRoundTrip:
    """End-to-end: pool then ingest, verify content is intact."""

    def test_round_trip(self, tmp_path: Path):
        manifest, store, src = _make_project(tmp_path)
        bundle = tmp_path / "bundle"

        # Pool from original store.
        pool_result = pool(manifest, bundle, store=store)
        assert pool_result.copied == 3

        # Ingest into a new store.
        new_store = FSStore(tmp_path / "new_store")
        ingest_result = ingest(bundle, store=new_store)
        assert ingest_result.copied == 3

        # All objects from the original store should now exist in the new store.
        for _rel_path, entry in manifest.flat_entries():
            if entry.is_dir() or entry.c4id is None:
                continue
            assert new_store.has(entry.c4id)
            original = store.get(entry.c4id).read()
            roundtripped = new_store.get(entry.c4id).read()
            assert original == roundtripped

    def test_round_trip_then_reconcile(self, tmp_path: Path):
        """Full cycle: scan -> pool -> ingest -> reconcile."""
        from c4py.reconcile import ReconcileResult, reconcile

        manifest, store, src = _make_project(tmp_path)
        bundle = tmp_path / "bundle"

        pool(manifest, bundle, store=store)

        new_store = FSStore(tmp_path / "new_store")
        ingest(bundle, store=new_store)

        target = tmp_path / "restored"
        target.mkdir()
        result = reconcile(manifest, target, store=new_store)

        assert isinstance(result, ReconcileResult)
        assert result.errors == []
        assert (target / "readme.txt").read_bytes() == b"This is the readme"
        assert (target / "data.bin").read_bytes() == b"\xde\xad\xbe\xef"
        assert (target / "assets" / "logo.png").read_bytes() == b"PNG FAKE HEADER"
