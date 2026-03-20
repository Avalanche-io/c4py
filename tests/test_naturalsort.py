"""Tests for natural sort algorithm."""

import pytest

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
