"""Tests for manifest validation."""

import stat as stat_mod
from datetime import datetime, timezone

from c4py.entry import Entry, NULL_TIMESTAMP, NULL_SIZE
from c4py.id import identify_bytes
from c4py.manifest import Manifest
from c4py.validator import (
    Issue,
    Severity,
    ValidationResult,
    validate,
    validate_mode_string,
    validate_c4id_string,
)


def _make_entry(
    name: str,
    depth: int = 0,
    size: int = 100,
    mode: int = 0o100644,
    content: bytes | None = None,
    timestamp: datetime | None = None,
) -> Entry:
    ts = timestamp or datetime(2025, 1, 15, 12, 0, 0, tzinfo=timezone.utc)
    e = Entry(name=name, depth=depth, size=size, mode=mode, timestamp=ts)
    if content is not None:
        e.c4id = identify_bytes(content)
    return e


def _make_dir_entry(name: str, depth: int = 0) -> Entry:
    return Entry(
        name=name,
        depth=depth,
        size=-1,
        mode=0o40755,
        timestamp=datetime(2025, 1, 15, 12, 0, 0, tzinfo=timezone.utc),
    )


class TestValidate:
    """Test validate() function."""

    def test_valid_manifest(self):
        m = Manifest(entries=[
            _make_entry("file1.txt", content=b"hello"),
            _make_entry("file2.txt", content=b"world"),
        ])
        result = validate(m)
        assert result.is_valid

    def test_empty_manifest(self):
        m = Manifest()
        result = validate(m)
        assert result.is_valid

    def test_valid_with_directories(self):
        m = Manifest(entries=[
            _make_entry("readme.txt"),
            _make_dir_entry("src/"),
            _make_entry("main.py", depth=1),
        ])
        result = validate(m)
        assert result.is_valid


class TestDuplicatePaths:
    """Test duplicate path detection."""

    def test_duplicate_files(self):
        m = Manifest(entries=[
            _make_entry("file.txt"),
            _make_entry("file.txt"),
        ])
        result = validate(m)
        assert not result.is_valid
        assert any("duplicate" in e.message.lower() for e in result.errors)

    def test_duplicate_in_subdirectory(self):
        m = Manifest(entries=[
            _make_dir_entry("src/"),
            _make_entry("main.py", depth=1),
            _make_entry("main.py", depth=1),
        ])
        result = validate(m)
        assert not result.is_valid

    def test_same_name_different_dirs(self):
        m = Manifest(entries=[
            _make_dir_entry("src/"),
            _make_entry("main.py", depth=1),
            _make_dir_entry("test/"),
            _make_entry("main.py", depth=1),
        ])
        result = validate(m)
        assert result.is_valid  # different full paths


class TestPathTraversal:
    """Test path traversal detection."""

    def test_dot_dot(self):
        m = Manifest(entries=[_make_entry("..")])
        result = validate(m)
        assert not result.is_valid
        assert any("path component" in e.message for e in result.errors)

    def test_dot(self):
        m = Manifest(entries=[_make_entry(".")])
        result = validate(m)
        assert not result.is_valid

    def test_slash_in_name(self):
        m = Manifest(entries=[_make_entry("path/to/file.txt")])
        result = validate(m)
        assert not result.is_valid
        assert any("path separator" in e.message for e in result.errors)

    def test_backslash_in_name(self):
        m = Manifest(entries=[_make_entry("file\\name.txt")])
        result = validate(m)
        assert not result.is_valid
        assert any("backslash" in e.message for e in result.errors)

    def test_empty_name(self):
        m = Manifest(entries=[Entry(name="", depth=0)])
        result = validate(m)
        assert not result.is_valid
        assert any("empty" in e.message for e in result.errors)

    def test_slash_only(self):
        m = Manifest(entries=[_make_dir_entry("/")])
        result = validate(m)
        assert not result.is_valid

    def test_null_byte_in_name(self):
        m = Manifest(entries=[_make_entry("file\x00.txt")])
        result = validate(m)
        assert not result.is_valid
        assert any("null" in e.message.lower() for e in result.errors)


class TestDirectoryNames:
    """Test directory name validation."""

    def test_directory_without_slash(self):
        e = Entry(
            name="src",
            depth=0,
            size=-1,
            mode=0o40755,
            timestamp=datetime(2025, 1, 1, tzinfo=timezone.utc),
        )
        m = Manifest(entries=[e])
        result = validate(m)
        # Should get a warning about directory not ending with /
        assert any("should end with '/'" in w.message for w in result.warnings)


class TestDepthValidation:
    """Test depth progression validation."""

    def test_valid_depth_progression(self):
        m = Manifest(entries=[
            _make_entry("file.txt"),
            _make_dir_entry("src/"),
            _make_entry("main.py", depth=1),
        ])
        result = validate(m)
        assert result.is_valid

    def test_invalid_depth_jump(self):
        m = Manifest(entries=[
            _make_entry("file.txt"),
            _make_entry("deep.txt", depth=3),
        ])
        result = validate(m)
        assert not result.is_valid
        assert any("depth jump" in e.message for e in result.errors)


class TestSizeValidation:
    """Test size field validation."""

    def test_valid_size(self):
        m = Manifest(entries=[_make_entry("file.txt", size=1024)])
        result = validate(m)
        assert result.is_valid

    def test_null_size(self):
        m = Manifest(entries=[_make_entry("file.txt", size=-1)])
        result = validate(m)
        assert result.is_valid

    def test_invalid_size(self):
        m = Manifest(entries=[_make_entry("file.txt", size=-2)])
        result = validate(m)
        assert not result.is_valid
        assert any("size" in e.message for e in result.errors)


class TestC4IDValidation:
    """Test C4 ID format validation."""

    def test_valid_c4id(self):
        e = _make_entry("file.txt", content=b"hello")
        m = Manifest(entries=[e])
        result = validate(m)
        assert result.is_valid

    def test_null_c4id(self):
        e = _make_entry("file.txt")
        e.c4id = None
        m = Manifest(entries=[e])
        result = validate(m)
        assert result.is_valid


class TestValidateModeString:
    """Test validate_mode_string helper."""

    def test_valid_file_mode(self):
        assert validate_mode_string("-rw-r--r--") == []

    def test_valid_dir_mode(self):
        assert validate_mode_string("drwxr-xr-x") == []

    def test_valid_symlink_mode(self):
        assert validate_mode_string("lrwxrwxrwx") == []

    def test_null_mode_dash(self):
        assert validate_mode_string("-") == []

    def test_null_mode_dashes(self):
        assert validate_mode_string("----------") == []

    def test_wrong_length(self):
        errors = validate_mode_string("rwx")
        assert len(errors) > 0
        assert "10 characters" in errors[0]

    def test_invalid_type(self):
        errors = validate_mode_string("zrwxr-xr-x")
        assert len(errors) > 0
        assert "invalid file type" in errors[0]

    def test_invalid_perm(self):
        errors = validate_mode_string("-rwzr-xr-x")
        assert len(errors) > 0
        assert "invalid permission" in errors[0]


class TestValidateC4IDString:
    """Test validate_c4id_string helper."""

    def test_valid_id(self):
        c4id = identify_bytes(b"test")
        assert validate_c4id_string(str(c4id)) == []

    def test_null_id(self):
        assert validate_c4id_string("-") == []

    def test_wrong_prefix(self):
        errors = validate_c4id_string("xx" + "1" * 88)
        assert len(errors) > 0
        assert "c4" in errors[0]

    def test_wrong_length(self):
        errors = validate_c4id_string("c4short")
        assert len(errors) > 0
        assert "90" in errors[0]


class TestValidationResult:
    """Test ValidationResult properties."""

    def test_is_valid_no_issues(self):
        result = ValidationResult()
        assert result.is_valid

    def test_is_valid_with_warnings(self):
        result = ValidationResult(issues=[
            Issue(severity=Severity.WARNING, message="minor issue"),
        ])
        assert result.is_valid

    def test_not_valid_with_errors(self):
        result = ValidationResult(issues=[
            Issue(severity=Severity.ERROR, message="bad thing"),
        ])
        assert not result.is_valid

    def test_errors_property(self):
        result = ValidationResult(issues=[
            Issue(severity=Severity.ERROR, message="err1"),
            Issue(severity=Severity.WARNING, message="warn1"),
            Issue(severity=Severity.ERROR, message="err2"),
        ])
        assert len(result.errors) == 2
        assert len(result.warnings) == 1
