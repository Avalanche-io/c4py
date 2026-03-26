"""Tests for C4 ID computation — must match Go reference implementation exactly."""

import json
from io import BytesIO
from pathlib import Path

import c4py
from c4py.id import (
    BASE58_ALPHABET,
    C4ID,
    identify,
    identify_bytes,
    identify_file,
    identify_files,
    parse,
    tree_id,
    verify,
)

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

    def test_overflow_value(self):
        """C4 ID with all 'z' chars decodes to > 2^512 — must raise ValueError."""
        import pytest
        with pytest.raises(ValueError, match="exceeds 512-bit"):
            parse("c4" + "z" * 88)


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


class TestC4IDStringConstructor:
    """C4ID should accept both bytes and string."""

    def test_from_string(self):
        c4id = identify_bytes(b"foo")
        from_str = C4ID(str(c4id))
        assert from_str == c4id

    def test_repr_round_trip(self):
        """repr() output should be copy-pasteable."""
        c4id = identify_bytes(b"hello world")
        restored = eval(repr(c4id))
        assert restored == c4id

    def test_invalid_string(self):
        import pytest
        with pytest.raises(ValueError):
            C4ID("not a c4 id")

    def test_invalid_type(self):
        import pytest
        with pytest.raises(TypeError):
            C4ID(42)


class TestC4IDBool:
    """Nil C4ID should be falsy, non-nil truthy."""

    def test_nil_is_falsy(self):
        nil_id = C4ID(b"\x00" * 64)
        assert not nil_id
        assert bool(nil_id) is False

    def test_non_nil_is_truthy(self):
        c4id = identify_bytes(b"foo")
        assert c4id
        assert bool(c4id) is True


class TestIdentifyFile:
    """Convenience function for file identification."""

    def test_identify_file(self, tmp_path):
        p = tmp_path / "test.txt"
        p.write_bytes(b"hello world")
        c4id = identify_file(p)
        assert c4id == identify_bytes(b"hello world")

    def test_identify_file_string_path(self, tmp_path):
        p = tmp_path / "test.txt"
        p.write_bytes(b"foo")
        c4id = identify_file(str(p))
        assert c4id == identify_bytes(b"foo")

    def test_identify_file_not_found(self):
        import pytest
        with pytest.raises(FileNotFoundError):
            identify_file("/nonexistent/file")


class TestVerify:
    """Quick file verification."""

    def test_verify_match(self, tmp_path):
        p = tmp_path / "test.txt"
        p.write_bytes(b"hello world")
        expected = identify_bytes(b"hello world")
        assert verify(p, expected) is True

    def test_verify_mismatch(self, tmp_path):
        p = tmp_path / "test.txt"
        p.write_bytes(b"hello world")
        wrong = identify_bytes(b"different content")
        assert verify(p, wrong) is False

    def test_verify_not_found(self):
        import pytest
        fake_id = identify_bytes(b"whatever")
        with pytest.raises(FileNotFoundError):
            verify("/nonexistent/file", fake_id)


class TestIdentifyFiles:
    """Parallel batch identification tests."""

    def test_multiple_files(self, tmp_path):
        files = {}
        for name, content in [("a.txt", b"aaa"), ("b.txt", b"bbb"), ("c.txt", b"ccc")]:
            p = tmp_path / name
            p.write_bytes(content)
            files[p.resolve()] = identify_bytes(content)

        result = identify_files([tmp_path / "a.txt", tmp_path / "b.txt", tmp_path / "c.txt"])
        for p, expected in files.items():
            assert result[p] == expected

    def test_single_worker(self, tmp_path):
        p = tmp_path / "only.txt"
        p.write_bytes(b"only")
        result = identify_files([p], workers=1)
        assert result[p.resolve()] == identify_bytes(b"only")

    def test_progress_callback(self, tmp_path):
        for name in ["x.txt", "y.txt", "z.txt"]:
            (tmp_path / name).write_bytes(name.encode())

        calls = []
        def on_progress(path, completed, total):
            calls.append((path, completed, total))

        paths = [tmp_path / "x.txt", tmp_path / "y.txt", tmp_path / "z.txt"]
        identify_files(paths, progress=on_progress)

        assert len(calls) == 3
        # Each call should have total == 3
        for _, _, total in calls:
            assert total == 3
        # Completed values should be {1, 2, 3} (order may vary due to concurrency)
        completed_values = {c for _, c, _ in calls}
        assert completed_values == {1, 2, 3}

    def test_nonexistent_file(self, tmp_path):
        good = tmp_path / "good.txt"
        good.write_bytes(b"good")
        bad = tmp_path / "nonexistent.txt"

        result = identify_files([good, bad])
        assert result[good.resolve()] == identify_bytes(b"good")
        assert result[bad.resolve()] is None

    def test_empty_list(self):
        result = identify_files([])
        assert result == {}

    def test_exported_from_package(self):
        assert hasattr(c4py, "identify_files")
        assert c4py.identify_files is identify_files
