"""Cross-implementation deepen-vs-remove conformance (erratum, 2026-07-31).

The shared fixtures under ``tests/vectors/deepen-vector/`` are byte-identical
copies of ``c4m/testdata/deepen-vector/`` in the Go reference repo (see that
directory's README.md). Both files restate the same directory line
byte-identically after a checkpoint; they differ only in whether a child
follows it, and must therefore resolve to different manifests:

- ``deepen.c4m``  resolves to a.txt, docs/, readme.md, guide.md
                  (a section that declares a child is not removing its parent)
- ``remove.c4m``  resolves to a.txt alone
                  (a directory restated alone still deletes its subtree)

Before the erratum, deepen.c4m resolved to a.txt and an orphaned guide.md:
the directory and readme.md, which the patch never mentioned, were silently
discarded.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from c4py.decoder import TrailingIDError, loads
from c4py.diff import apply_patch

DEEPEN_VECTOR = Path(__file__).parent / "vectors" / "deepen-vector"

CASES = [
    ("deepen.c4m", "deepen-root-id.txt", {"a.txt", "docs/", "readme.md", "guide.md"}),
    ("remove.c4m", "remove-root-id.txt", {"a.txt"}),
]


def _read(name: str) -> str:
    return (DEEPEN_VECTOR / name).read_text(encoding="utf-8")


def _is_bare_id(line: str) -> bool:
    return line.startswith("c4") and len(line) == 90 and " " not in line


def _names(m) -> set[str]:
    return {e.name for e in m.entries}


class TestDeepenVector:
    """Conformance is on the resolved entry set, not only on the ID.

    A decoder carrying the old rule resolves deepen.c4m to remove.c4m's
    state at exit 0: since 2026-11-08 nothing in the file verifies the final
    state (the trailing ID line was removed), so the entry set and the
    root-id file are the check.
    """

    def test_resolves_through_the_decoder(self):
        ids = {}
        for file, id_file, want in CASES:
            m = loads(_read(file))
            assert _names(m) == want, file
            assert str(m.compute_c4id()) == _read(id_file).strip(), file
            ids[file] = str(m.compute_c4id())
        # the same restated directory means opposite things depending on
        # whether children follow it
        assert ids["deepen.c4m"] != ids["remove.c4m"]

    def test_vectors_end_on_an_entry_and_refuse_their_own_id(self):
        """Each vector ends on an entry; its root ID appended is a trailing ID."""
        for file, id_file, _ in CASES:
            text = _read(file)
            assert not _is_bare_id(text.rstrip("\n").split("\n")[-1]), file
            with pytest.raises(TrailingIDError):
                loads(text + _read(id_file).strip() + "\n")

    def test_apply_patch_directly(self):
        """The rule itself, independent of chain decoding: base, then patch."""
        ids = {}
        for file, id_file, want in CASES:
            sections: list[list[str]] = [[]]
            for line in _read(file).rstrip("\n").split("\n"):
                if _is_bare_id(line):
                    sections.append([])
                else:
                    sections[-1].append(line)
            sections = [s for s in sections if s]
            assert len(sections) == 2, file
            base = loads("\n".join(sections[0]) + "\n")
            patch = loads("\n".join(sections[1]) + "\n")
            m = apply_patch(base, patch.entries)
            assert _names(m) == want, file
            assert str(m.compute_c4id()) == _read(id_file).strip(), file
            ids[file] = str(m.compute_c4id())
        assert ids["deepen.c4m"] != ids["remove.c4m"]
