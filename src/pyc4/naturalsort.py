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

Reference: /Users/joshua/ws/active/c4/oss/c4/c4m/naturalsort.go
"""

from __future__ import annotations

import re
from typing import Any


def natural_sort_key(name: str) -> list[Any]:
    """Generate a sort key for natural ordering.

    Splits name into alternating text and numeric segments,
    returns a list suitable for comparison.
    """
    # TODO: implement segment splitting and key generation
    # Split on boundaries between digits and non-digits
    # For each segment:
    #   - numeric: (0, int_value, len(segment)) — 0 prefix sorts numeric after text
    #     Wait, text sorts BEFORE numeric, so: text=(0, ...), numeric=(1, ...)
    #   - text: (0, segment)
    #   - numeric: (1, int_value, len(segment))
    raise NotImplementedError
