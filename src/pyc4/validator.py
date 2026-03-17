"""C4M validation — check manifest correctness.

Validation rules (from SPECIFICATION.md):
- Reject lines beginning with @
- Reject invalid UTF-8
- Reject CR characters
- Reject path traversal (../, ./, /, \\)
- Reject duplicate paths in same scope
- Verify indentation consistency
- Verify entry field formats

Reference: /Users/joshua/ws/active/c4/oss/c4/c4m/validator.go
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum

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


def validate(manifest: Manifest) -> ValidationResult:
    """Validate a manifest for correctness.

    Checks:
    - No duplicate paths
    - No path traversal
    - Consistent indentation
    - Valid mode strings
    - Valid timestamps
    - Valid C4 ID format
    - Directory names end with /
    - Entries properly nested under parent directories
    """
    # TODO: implement validation
    raise NotImplementedError
