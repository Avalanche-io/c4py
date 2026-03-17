"""Tests for manifest operations — load, dump, diff, validate."""

import json
from pathlib import Path

VECTORS_PATH = Path(__file__).parent / "vectors" / "known_ids.json"


def load_vectors():
    with open(VECTORS_PATH) as f:
        return json.load(f)


class TestManifestRoundTrip:
    """Parse canonical c4m text and re-encode — must produce identical output."""

    def test_simple_manifest(self):
        vectors = load_vectors()
        for mv in vectors["manifest_vectors"]:
            import pyc4
            manifest = pyc4.loads(mv["canonical"])
            output = pyc4.dumps(manifest)
            assert output == mv["canonical"], (
                f"Round-trip failed for: {mv['description']}"
            )

    def test_manifest_c4id(self):
        vectors = load_vectors()
        for mv in vectors["manifest_vectors"]:
            import pyc4
            manifest = pyc4.loads(mv["canonical"])
            c4id = manifest.compute_c4id()
            assert str(c4id) == mv["manifest_c4id"], (
                f"C4 ID mismatch for: {mv['description']}"
            )


class TestDiff:
    """Manifest diff operations."""

    def test_identical_manifests(self):
        import pyc4
        vectors = load_vectors()
        mv = vectors["manifest_vectors"][0]
        a = pyc4.loads(mv["canonical"])
        b = pyc4.loads(mv["canonical"])
        result = pyc4.diff(a, b)
        assert len(result.added) == 0
        assert len(result.removed) == 0
        assert len(result.modified) == 0

    def test_empty_manifests(self):
        import pyc4
        a = pyc4.Manifest()
        b = pyc4.Manifest()
        result = pyc4.diff(a, b)
        assert len(result.added) == 0
        assert len(result.removed) == 0
