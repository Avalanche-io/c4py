"""C4M encoder — write Manifest objects as c4m text.

Two modes:
- Canonical: no padding, single spaces, UTC timestamps, for C4 ID computation
- Pretty: column-aligned, local timestamps, padded sizes, human-readable

Reference: /Users/joshua/ws/active/c4/oss/c4/c4m/encoder.go
"""

from __future__ import annotations

from typing import IO, Union

from .manifest import Manifest


def dump(manifest: Manifest, dest: Union[str, IO[str]], *, pretty: bool = False) -> None:
    """Write a manifest to a file path or text stream.

    Args:
        manifest: the manifest to write
        dest: file path (str) or text-mode file object
        pretty: if True, use ergonomic formatting
    """
    text = dumps(manifest, pretty=pretty)
    if isinstance(dest, str):
        with open(dest, "w", encoding="utf-8") as f:
            f.write(text)
    else:
        dest.write(text)


def dumps(manifest: Manifest, *, pretty: bool = False) -> str:
    """Encode a manifest as c4m text.

    Args:
        manifest: the manifest to encode
        pretty: if True, use ergonomic formatting (padded, local time)

    Returns:
        c4m text (UTF-8, LF line endings)

    Canonical format rules:
    - No indentation
    - Single space between fields
    - No padding
    - UTC timestamps with Z suffix
    - Null values as '-'
    - Natural sort order

    Pretty format:
    - Indented (2 spaces per level)
    - Sizes right-aligned and padded
    - C4 IDs column-aligned (col 80, or next 10-col boundary)
    - Local timestamps

    Reference: /Users/joshua/ws/active/c4/oss/c4/c4m/encoder.go
    """
    # TODO: implement encoder
    raise NotImplementedError
