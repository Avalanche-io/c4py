"""Natural sort for c4m entries.

C4M canonical order:
1. Files sort before directories at each depth level
2. Within each group, names are split into text/numeric segments
3. Segments compared left-to-right:
   - Numeric: compare as integers (1 < 10 < 100)
   - Equal integers: shorter representation first (1 < 01 < 001)
   - Text: UTF-8 codepoint comparison
   - Mixed: text sorts before numeric

Examples:
    file1.txt < file2.txt < file10.txt
    render.1.exr < render.01.exr < render.2.exr
    README < src/  (files before dirs)

Reference: github.com/Avalanche-io/c4/c4m/naturalsort.go
"""

from __future__ import annotations

from typing import Any


def natural_sort_key(name: str) -> list[Any]:
    """Generate a sort key for natural ordering.

    Splits name into alternating text and numeric segments,
    returns a list suitable for comparison.

    Each segment becomes a tuple:
      - text:    (0, segment_str)
      - numeric: (1, int_value, len(segment_str))

    Text (0) sorts before numeric (1) when mixed. For equal integers,
    shorter representation sorts first via the length tiebreaker.
    """
    segments = _segment_string(name)
    key: list[Any] = []
    for text, is_numeric, num_value in segments:
        if is_numeric:
            key.append((1, num_value, len(text)))
        else:
            key.append((0, text))
    return key


def _segment_string(s: str) -> list[tuple[str, bool, int]]:
    """Split a string into alternating text/numeric segments.

    Returns a list of (text, is_numeric, num_value) tuples.
    For text segments, num_value is 0 (unused).
    """
    if not s:
        return []

    segments: list[tuple[str, bool, int]] = []
    current: list[str] = []
    is_numeric = s[0].isdigit()

    for ch in s:
        is_digit = ch.isdigit()
        if is_digit != is_numeric:
            # Transition: flush current segment
            text = "".join(current)
            num_value = _parse_number(text) if is_numeric else 0
            segments.append((text, is_numeric, num_value))
            current = [ch]
            is_numeric = is_digit
        else:
            current.append(ch)

    # Final segment
    if current:
        text = "".join(current)
        num_value = _parse_number(text) if is_numeric else 0
        segments.append((text, is_numeric, num_value))

    return segments


def _parse_number(s: str) -> int:
    """Convert a numeric string to int."""
    result = 0
    for ch in s:
        if "0" <= ch <= "9":
            result = result * 10 + (ord(ch) - ord("0"))
    return result
