"""C4M manifest — a collection of entries describing a filesystem.

A manifest is the in-memory representation of a c4m file. It holds entries
(files, directories, links) with their metadata and C4 IDs.

Reference: /Users/joshua/ws/active/c4/oss/c4/c4m/manifest.go
"""

from __future__ import annotations

from typing import Iterator, Optional

from .entry import Entry
from .id import C4ID


class Manifest:
    """A c4m manifest — describes a filesystem as a list of entries.

    Entries are ordered: files before directories at each level,
    then natural sort within each group.
    """

    def __init__(self, entries: Optional[list[Entry]] = None) -> None:
        self.entries: list[Entry] = entries or []
        self.base: Optional[C4ID] = None  # external base reference (patch format)

    def __iter__(self) -> Iterator[Entry]:
        return iter(self.entries)

    def __len__(self) -> int:
        return len(self.entries)

    def __getitem__(self, index: int) -> Entry:
        return self.entries[index]

    def sort_entries(self) -> None:
        """Sort entries into canonical order (files before dirs, natural sort).

        Uses the natural sort algorithm: numeric segments compared as integers,
        text segments compared as UTF-8 codepoints.

        Reference: /Users/joshua/ws/active/c4/oss/c4/c4m/naturalsort.go
        """
        from .naturalsort import natural_sort_key
        # TODO: implement hierarchical sort (depth-aware, files before dirs)
        raise NotImplementedError

    def compute_c4id(self) -> C4ID:
        """Compute the C4 ID of this manifest.

        Sorts entries into canonical order, formats each in canonical form,
        then computes the C4 ID of the resulting UTF-8 text.
        """
        from .id import identify_bytes
        # TODO: sort, format canonical, hash
        raise NotImplementedError

    def copy(self) -> Manifest:
        """Deep copy of this manifest."""
        from copy import deepcopy
        return deepcopy(self)
