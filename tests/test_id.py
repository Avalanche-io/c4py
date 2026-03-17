"""Tests for C4 ID computation — must match Go reference implementation exactly."""

import json
from io import BytesIO
from pathlib import Path

import pyc4
from pyc4.id import C4ID, BASE58_ALPHABET, identify, identify_bytes, parse, tree_id

VECTORS_PATH = Path(__file__).parent / "vectors" / "known_ids.json"


def load_vectors():
    with open(VECTORS_PATH) as f:
        return json.load(f)


class TestC4ID:
    """Core C4 ID type tests."""

    def test_digest_length(self):
        c4id = identify_bytes(b"foo")
        assert len(c4id.digest) == 64

    def test_string_length(self):
        c4id = identify_bytes(b"foo")
        assert len(str(c4id)) == 90

    def test_string_prefix(self):
        c4id = identify_bytes(b"foo")
        assert str(c4id).startswith("c4")

    def test_string_base58_only(self):
        c4id = identify_bytes(b"foo")
        s = str(c4id)
        for c in s[2:]:  # skip "c4" prefix
            assert c in BASE58_ALPHABET, f"Invalid char '{c}' in C4 ID"

    def test_equality(self):
        a = identify_bytes(b"foo")
        b = identify_bytes(b"foo")
        assert a == b

    def test_inequality(self):
        a = identify_bytes(b"foo")
        b = identify_bytes(b"bar")
        assert a != b

    def test_hash(self):
        a = identify_bytes(b"foo")
        b = identify_bytes(b"foo")
        assert hash(a) == hash(b)
        assert len({a, b}) == 1

    def test_ordering(self):
        a = identify_bytes(b"foo")
        b = identify_bytes(b"bar")
        # One must be less than the other
        assert (a < b) or (b < a)

    def test_nil_id(self):
        nil_id = C4ID(b"\x00" * 64)
        assert nil_id.is_nil()

    def test_non_nil(self):
        c4id = identify_bytes(b"foo")
        assert not c4id.is_nil()

    def test_bytes_conversion(self):
        c4id = identify_bytes(b"foo")
        assert bytes(c4id) == c4id.digest

    def test_hex(self):
        c4id = identify_bytes(b"foo")
        assert len(c4id.hex()) == 128  # 64 bytes = 128 hex chars

    def test_repr(self):
        c4id = identify_bytes(b"foo")
        assert repr(c4id).startswith("C4ID('c4")


class TestIdentify:
    """Streaming identification tests."""

    def test_empty_stream(self):
        c4id = identify(BytesIO(b""))
        vectors = load_vectors()
        expected = next(v for v in vectors["single_ids"] if v["input_repr"] == "empty string")
        assert str(c4id) == expected["c4id"]

    def test_stream_matches_bytes(self):
        data = b"hello world"
        assert identify(BytesIO(data)) == identify_bytes(data)

    def test_large_stream(self):
        """Verify streaming works with data larger than buffer size."""
        data = b"x" * 1_000_000
        assert identify(BytesIO(data)) == identify_bytes(data)


class TestCrossLanguage:
    """Cross-language compatibility with Go reference implementation."""

    def test_all_single_ids(self):
        vectors = load_vectors()
        for v in vectors["single_ids"]:
            input_bytes = bytes.fromhex(v["input_bytes_hex"]) if v["input_bytes_hex"] else b""
            c4id = identify_bytes(input_bytes)
            assert str(c4id) == v["c4id"], (
                f"Mismatch for {v['input_repr']}: got {c4id}, expected {v['c4id']}"
            )

    def test_all_single_ids_hex(self):
        vectors = load_vectors()
        for v in vectors["single_ids"]:
            input_bytes = bytes.fromhex(v["input_bytes_hex"]) if v["input_bytes_hex"] else b""
            c4id = identify_bytes(input_bytes)
            assert c4id.hex() == v["digest_hex"], (
                f"Digest mismatch for {v['input_repr']}"
            )


class TestParse:
    """Parsing C4 ID strings."""

    def test_round_trip(self):
        c4id = identify_bytes(b"foo")
        parsed = parse(str(c4id))
        assert parsed == c4id

    def test_all_vectors_round_trip(self):
        vectors = load_vectors()
        for v in vectors["single_ids"]:
            parsed = parse(v["c4id"])
            assert str(parsed) == v["c4id"]
            assert parsed.hex() == v["digest_hex"]

    def test_invalid_length(self):
        import pytest
        with pytest.raises(ValueError, match="90 characters"):
            parse("c4short")

    def test_invalid_prefix(self):
        import pytest
        with pytest.raises(ValueError, match="start with"):
            parse("xx" + "1" * 88)

    def test_invalid_character(self):
        import pytest
        # '0' is not in base58 alphabet
        with pytest.raises(ValueError, match="Invalid base58"):
            parse("c4" + "0" * 88)


class TestTreeID:
    """Tree (set) ID computation."""

    def test_cross_language_tree_ids(self):
        vectors = load_vectors()
        for tv in vectors["tree_ids"]:
            ids = [identify_bytes(s.encode()) for s in tv["inputs"]]
            result = tree_id(ids)
            assert str(result) == tv["tree_id"], (
                f"Tree ID mismatch for {tv['description']}: got {result}, expected {tv['tree_id']}"
            )

    def test_order_independence(self):
        a = identify_bytes(b"foo")
        b = identify_bytes(b"bar")
        assert tree_id([a, b]) == tree_id([b, a])

    def test_single_id(self):
        a = identify_bytes(b"foo")
        assert tree_id([a]) == a

    def test_deduplication(self):
        a = identify_bytes(b"foo")
        assert tree_id([a, a]) == tree_id([a])
