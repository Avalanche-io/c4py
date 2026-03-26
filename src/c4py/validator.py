"""C4M validation — check manifest correctness.

Validation rules (from SPECIFICATION.md):
- Reject lines beginning with @
- Reject invalid UTF-8
- Reject CR characters
- Reject path traversal (../, ./, /, \\)
- Reject duplicate paths in same scope
- Verify indentation consistency
- Verify entry field formats

Reference: github.com/Avalanche-io/c4/c4m/validator.go
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from enum import Enum

from .entry import NULL_SIZE, NULL_TIMESTAMP, Entry
from .id import BASE58_ALPHABET, C4_ID_LENGTH, C4_PREFIX
from .manifest import Manifest


class Severity(Enum):
    ERROR = "error"
    WARNING = "warning"


@dataclass
class Issue:
    """A validation issue found in a manifest."""

    severity: Severity
    message: str
    line: int = 0  # 0 = manifest-level issue


@dataclass
class ValidationResult:
    """Result of validating a manifest."""

    issues: list[Issue] = field(default_factory=list)

    @property
    def errors(self) -> list[Issue]:
        return [i for i in self.issues if i.severity == Severity.ERROR]

    @property
    def warnings(self) -> list[Issue]:
        return [i for i in self.issues if i.severity == Severity.WARNING]

    @property
    def is_valid(self) -> bool:
        return len(self.errors) == 0


# Valid base58 character set for C4 IDs (excludes 0, O, I, l)
_C4ID_PATTERN = re.compile(r"^c4[" + re.escape(BASE58_ALPHABET) + r"]+$")

# Valid mode characters
_MODE_TYPES = set("-dlbcps")
_MODE_PERMS = set("-rwxstST")


def validate(manifest: Manifest) -> ValidationResult:
    """Validate a manifest for correctness.

    Checks:
    - No duplicate paths
    - No path traversal
    - Consistent depth progression
    - Valid mode strings
    - Valid timestamps
    - Valid C4 ID format
    - Directory names end with /
    - Entries properly nested under parent directories
    """
    result = ValidationResult()

    if not manifest.entries:
        return result

    seen_paths: dict[str, int] = {}
    depth_stack: list[str] = []
    last_depth = -1

    for idx, entry in enumerate(manifest.entries):
        line = idx + 1  # 1-based line number

        # Validate name
        _validate_name(entry, line, result)

        # Validate mode
        _validate_mode(entry, line, result)

        # Validate timestamp
        _validate_timestamp(entry, line, result)

        # Validate size
        _validate_size(entry, line, result)

        # Validate C4 ID format
        _validate_c4id(entry, line, result)

        # Validate depth progression
        if entry.depth > last_depth + 1:
            result.issues.append(Issue(
                severity=Severity.ERROR,
                message=f"invalid depth jump from {last_depth} to {entry.depth}",
                line=line,
            ))

        # Build full path and check for duplicates
        if entry.depth < len(depth_stack):
            depth_stack = depth_stack[:entry.depth]

        full_path = "".join(depth_stack) + entry.name
        if full_path in seen_paths:
            result.issues.append(Issue(
                severity=Severity.ERROR,
                message=f"duplicate path '{full_path}' (first at entry {seen_paths[full_path]})",
                line=line,
            ))
        else:
            seen_paths[full_path] = line

        # Track directory structure
        if entry.is_dir():
            while len(depth_stack) <= entry.depth:
                depth_stack.append("")
            depth_stack[entry.depth] = entry.name

        last_depth = entry.depth

    return result


def _validate_name(entry: Entry, line: int, result: ValidationResult) -> None:
    """Validate an entry's name field."""
    name = entry.name

    if not name:
        result.issues.append(Issue(
            severity=Severity.ERROR,
            message="name cannot be empty",
            line=line,
        ))
        return

    # Strip trailing slash for base name validation
    base = name.rstrip("/")

    if not base:
        result.issues.append(Issue(
            severity=Severity.ERROR,
            message="name cannot be '/' — the c4m file itself is the root",
            line=line,
        ))
        return

    # Path traversal checks
    if base == "." or base == "..":
        result.issues.append(Issue(
            severity=Severity.ERROR,
            message=f"'{name}' is a path component, not a valid entry name",
            line=line,
        ))
        return

    if "/" in base:
        result.issues.append(Issue(
            severity=Severity.ERROR,
            message=f"name contains path separator '/': '{name}' — use depth for hierarchy",
            line=line,
        ))

    if "\\" in base:
        result.issues.append(Issue(
            severity=Severity.ERROR,
            message=f"name contains backslash: '{name}' — backslash is not allowed in c4m names",
            line=line,
        ))

    if "\x00" in base:
        result.issues.append(Issue(
            severity=Severity.ERROR,
            message="null bytes not allowed in names",
            line=line,
        ))

    # Directory names must end with /
    if entry.is_dir() and not name.endswith("/"):
        result.issues.append(Issue(
            severity=Severity.WARNING,
            message=f"directory entry '{name}' should end with '/'",
            line=line,
        ))


def _validate_mode(entry: Entry, line: int, result: ValidationResult) -> None:
    """Validate an entry's mode field."""
    mode = entry.mode

    # Null mode (0) is valid
    if mode == 0:
        return

    # Extract the file type bits
    # We validate the mode integer is sensible, but we can't validate
    # the string format since we only have the integer representation.
    # The validator checks that the mode makes sense for the entry type.
    import stat as stat_mod

    fmt = stat_mod.S_IFMT(mode)
    valid_types = {
        stat_mod.S_IFREG,
        stat_mod.S_IFDIR,
        stat_mod.S_IFLNK,
        stat_mod.S_IFBLK,
        stat_mod.S_IFCHR,
        stat_mod.S_IFIFO,
        stat_mod.S_IFSOCK,
        0,  # no type bits set is acceptable
    }

    if fmt not in valid_types:
        result.issues.append(Issue(
            severity=Severity.ERROR,
            message=f"invalid file type bits in mode: 0o{mode:o}",
            line=line,
        ))

    # Check consistency: if name ends with /, mode should indicate directory
    if entry.name.endswith("/") and fmt != 0 and fmt != stat_mod.S_IFDIR:
        result.issues.append(Issue(
            severity=Severity.WARNING,
            message=f"directory name '{entry.name}' but mode indicates non-directory type",
            line=line,
        ))


def _validate_timestamp(entry: Entry, line: int, result: ValidationResult) -> None:
    """Validate an entry's timestamp field."""
    ts = entry.timestamp

    # Null timestamp is valid
    if ts == NULL_TIMESTAMP:
        return

    # Check timezone awareness
    if ts.tzinfo is None:
        result.issues.append(Issue(
            severity=Severity.WARNING,
            message="timestamp should be timezone-aware (UTC preferred)",
            line=line,
        ))


def _validate_size(entry: Entry, line: int, result: ValidationResult) -> None:
    """Validate an entry's size field."""
    size = entry.size

    # Null size (-1) is valid
    if size == NULL_SIZE:
        return

    if size < -1:
        result.issues.append(Issue(
            severity=Severity.ERROR,
            message=f"size cannot be less than -1, got {size}",
            line=line,
        ))


def _validate_c4id(entry: Entry, line: int, result: ValidationResult) -> None:
    """Validate an entry's C4 ID format."""
    c4id = entry.c4id

    # Null C4 ID is valid
    if c4id is None:
        return

    id_str = str(c4id)

    # Check prefix
    if not id_str.startswith(C4_PREFIX):
        result.issues.append(Issue(
            severity=Severity.ERROR,
            message=f"C4 ID must start with '{C4_PREFIX}', got: {id_str[:10]}...",
            line=line,
        ))
        return

    # Check length
    if len(id_str) != C4_ID_LENGTH:
        result.issues.append(Issue(
            severity=Severity.ERROR,
            message=f"C4 ID must be {C4_ID_LENGTH} characters, got {len(id_str)}",
            line=line,
        ))

    # Check valid base58 characters
    if not _C4ID_PATTERN.match(id_str):
        result.issues.append(Issue(
            severity=Severity.ERROR,
            message=f"C4 ID contains invalid base58 characters: {id_str}",
            line=line,
        ))


def validate_mode_string(mode_str: str) -> list[str]:
    """Validate a 10-character Unix mode string.

    Returns a list of error messages (empty if valid).
    Used for validating mode strings from c4m text before parsing.
    """
    errors: list[str] = []

    # Null mode
    if mode_str in ("-", "----------"):
        return errors

    if len(mode_str) != 10:
        errors.append(f"mode must be 10 characters, got {len(mode_str)}")
        return errors

    # Check first character (file type)
    if mode_str[0] not in _MODE_TYPES:
        errors.append(f"invalid file type: {mode_str[0]}")

    # Check permission characters
    for i in range(1, 10):
        if mode_str[i] not in _MODE_PERMS:
            errors.append(f"invalid permission character at position {i}: {mode_str[i]}")

    return errors


def validate_c4id_string(id_str: str) -> list[str]:
    """Validate a C4 ID string format.

    Returns a list of error messages (empty if valid).
    Used for validating C4 IDs from c4m text before parsing.
    """
    errors: list[str] = []

    if id_str == "-":
        return errors

    if not id_str.startswith(C4_PREFIX):
        errors.append(f"C4 ID must start with '{C4_PREFIX}'")
        return errors

    if len(id_str) != C4_ID_LENGTH:
        errors.append(f"C4 ID must be {C4_ID_LENGTH} characters, got {len(id_str)}")

    if not _C4ID_PATTERN.match(id_str):
        errors.append("C4 ID contains invalid base58 characters")

    return errors
