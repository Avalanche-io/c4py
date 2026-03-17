"""C4M filename encoding — Universal Filename Encoding.

Three-tier encoding system for representing any byte sequence as printable UTF-8:

Tier 1: Printable UTF-8 passes through unchanged (except currency sign and backslash)
Tier 2: Backslash escapes for common control characters:
    \\0 -> null (0x00), \\t -> tab (0x09), \\n -> newline (0x0A),
    \\r -> CR (0x0D), \\\\ -> backslash (0x5C)
Tier 3: Non-printable bytes as braille codepoints (U+2800-U+28FF) between currency signs

C4M field-boundary escaping (applied after SafeName):
    space -> '\\ ', double-quote -> '\\"', [ -> '\\[', ] -> '\\]'

Reference: /Users/joshua/ws/active/c4/oss/c4/c4m/safename.go
"""

from __future__ import annotations


def safe_name(raw: str) -> str:
    """Encode a raw filename to printable UTF-8 (Tier 1-3).

    Args:
        raw: raw filename bytes as a string

    Returns:
        Encoded filename safe for c4m representation
    """
    # TODO: implement three-tier encoding
    raise NotImplementedError


def unsafe_name(encoded: str) -> str:
    """Decode a SafeName-encoded filename back to the raw form.

    Args:
        encoded: SafeName-encoded string

    Returns:
        Original raw filename
    """
    # TODO: implement three-tier decoding
    raise NotImplementedError


def escape_field(name: str, is_sequence: bool = False) -> str:
    """Apply c4m field-boundary escaping after SafeName encoding.

    Escapes space, double-quote, and (for non-sequences) square brackets.
    """
    # TODO: implement field escaping
    raise NotImplementedError


def unescape_field(name: str) -> str:
    """Reverse c4m field-boundary escaping."""
    # TODO: implement field unescaping
    raise NotImplementedError
