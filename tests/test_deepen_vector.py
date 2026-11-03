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

from c4py.decoder import loads
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


def _without_closing_validator(text: str) -> str:
    """Drop a bare C4 ID line at EOF.

    The shared fixture ends with a closing validator (a bare ID at EOF naming
    the resolved state). The 1.0.x grammar has no closing validator and reads
    one as an empty patch section, so it is dropped here; the ID file it
    repeats is what the resolved manifest is checked against. The fixture
    bytes stay identical to every other implementation's copy.
    """
    lines = text.rstrip("\n").split("\n")
    if _is_bare_id(lines[-1]):
        lines = lines[:-1]
    return "\n".join(lines) + "\n"


def _names(m) -> set[str]:
    return {e.name for e in m.entries}


class TestDeepenVector:
    """Conformance is on the resolved entry set, not only on the ID."""

    def test_resolves_through_the_decoder(self):
        ids = {}
        for file, id_file, want in CASES:
            m = loads(_without_closing_validator(_read(file)))
            assert _names(m) == want, file
            assert str(m.compute_c4id()) == _read(id_file).strip(), file
            ids[file] = str(m.compute_c4id())
        # the same restated directory means opposite things depending on
        # whether children follow it
        assert ids["deepen.c4m"] != ids["remove.c4m"]

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
