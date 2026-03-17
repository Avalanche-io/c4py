"""Directory scanner — walk a filesystem and produce a Manifest.

Scans a directory tree, computing C4 IDs for each file and building
a c4m manifest that describes the complete filesystem state.

Reference: /Users/joshua/ws/active/c4/oss/c4/c4m/builder.go
"""

from __future__ import annotations

from pathlib import Path
from typing import Union

from .manifest import Manifest


def scan(
    path: Union[str, Path],
    *,
    follow_symlinks: bool = False,
    compute_ids: bool = True,
) -> Manifest:
    """Scan a directory and produce a c4m Manifest.

    Args:
        path: directory to scan
        follow_symlinks: if True, follow symlinks when scanning
        compute_ids: if True, compute C4 IDs for all files (slower but complete)

    Returns:
        Manifest describing the directory contents

    The scanner:
    1. Walks the directory tree (os.walk or pathlib)
    2. For each file: stat for metadata, optionally compute C4 ID
    3. For each directory: compute recursive size, collect children
    4. For symlinks: record target, optionally identify target content
    5. Sort entries into canonical order
    6. Compute directory C4 IDs bottom-up
    """
    # TODO: implement directory scanner
    raise NotImplementedError
