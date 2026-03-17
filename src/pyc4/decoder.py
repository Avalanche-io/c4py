"""C4M decoder — parse c4m text into Manifest objects.

The parser is lenient: it accepts ergonomic format variations (padded sizes,
local timestamps, varying indentation) and normalizes internally. Only
canonical form is required for C4 ID computation.

Reference: /Users/joshua/ws/active/c4/oss/c4/c4m/decoder.go
           /Users/joshua/ws/active/c4/oss/c4/c4m/SPECIFICATION.md
"""

from __future__ import annotations

from typing import IO, Union

from .manifest import Manifest


def load(source: Union[str, IO[str]]) -> Manifest:
    """Load a c4m manifest from a file path or text stream.

    Args:
        source: file path (str) or text-mode file object
    """
    if isinstance(source, str):
        with open(source, "r", encoding="utf-8") as f:
            return loads(f.read())
    return loads(source.read())


def loads(text: str) -> Manifest:
    """Parse c4m text into a Manifest.

    Parser rules:
    - UTF-8 only, no BOM
    - LF line endings only (CR forbidden)
    - Blank lines ignored
    - Lines starting with @ rejected
    - Indentation: spaces only, auto-detected width
    - Fields: mode timestamp size name [link-op target] c4id
    - Null values: '-' for mode, timestamp, size, c4id

    Reference: /Users/joshua/ws/active/c4/oss/c4/c4m/decoder.go
    """
    # TODO: implement c4m parser
    # Key steps:
    # 1. Split into lines, skip blank lines
    # 2. Reject @ directives
    # 3. Check for bare C4 ID lines (patch boundaries)
    # 4. Detect indentation width from first indented line
    # 5. Parse each entry line: mode, timestamp, size, name, [link], c4id
    # 6. Build entry tree with depth tracking
    # 7. Handle sequences, symlinks, hard links, flow links
    raise NotImplementedError
