"""Tests for natural sort algorithm."""


from c4py.naturalsort import natural_sort_key


class TestNaturalSort:
    """Natural sort must match Go reference implementation ordering."""

    def test_numeric_segments(self):
        names = ["file10.txt", "file2.txt", "file1.txt"]
        result = sorted(names, key=natural_sort_key)
        assert result == ["file1.txt", "file2.txt", "file10.txt"]

    def test_leading_zeros(self):
        """Equal numeric value: shorter representation sorts first."""
        names = ["render.01.exr", "render.1.exr", "render.001.exr"]
        result = sorted(names, key=natural_sort_key)
        assert result == ["render.1.exr", "render.01.exr", "render.001.exr"]

    def test_mixed_numeric(self):
        names = ["render.1.exr", "render.2.exr", "render.10.exr"]
        result = sorted(names, key=natural_sort_key)
        assert result == ["render.1.exr", "render.2.exr", "render.10.exr"]

    def test_pure_alpha(self):
        names = ["banana", "apple", "cherry"]
        result = sorted(names, key=natural_sort_key)
        assert result == ["apple", "banana", "cherry"]

    def test_empty(self):
        assert sorted([], key=natural_sort_key) == []

    def test_single(self):
        assert sorted(["a"], key=natural_sort_key) == ["a"]


class TestNaturalSortVectors:
    """The conformance vectors shared with c4 (Go) and c4ts: digit runs of
    17, 20, 21 and 39 digits, which must compare by integer value of any
    length so that every permutation of the names sorts to one order."""

    @staticmethod
    def _vectors() -> list[str]:
        import json
        from pathlib import Path

        path = Path(__file__).parent / "vectors" / "natural_sort_vectors.json"
        return json.loads(path.read_text())["sorted"]

    def test_pairwise_order(self):
        names = self._vectors()
        for i, a in enumerate(names):
            for b in names[i + 1 :]:
                assert natural_sort_key(a) < natural_sort_key(b), f"want {a!r} < {b!r}"

    def test_every_permutation_sorts_the_same(self):
        import random

        expected = self._vectors()
        rng = random.Random(1)
        for _ in range(200):
            names = list(expected)
            rng.shuffle(names)
            assert sorted(names, key=natural_sort_key) == expected
