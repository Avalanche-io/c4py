"""Every shared vector file round-trips as its README's "Round trip" table says.

A c4m file's byte ID is a C4 ID, so a decode-and-encode round trip that
changes a file's bytes, or drops some, is data loss. Until 2026-11-13
nothing compared bytes, and the encoder dropped a c4m's inline range data
unseen (sequence-null-size vector; decided 2026-11-13 by the release lead;
reversible).

Each vendored vector README carries a ``## Round trip`` table (c4 commit
770ca07, #158), which Go's ``TestVectorRoundTrip`` reads and every port
runs. This test parses the same table; it hardcodes no classification.
Each ``.c4m`` file is decoded and encoded canonically, and SHA-512 digests
from ``hashlib`` (never c4py's own C4 ID code) are compared. The second
cell of a row is one of:

- ``itself``: the output is the file, byte for byte;
- a backticked file: the output is that file (its canonical partner);
- ``ID in`` a backticked file: a chain whose resolved state has that C4 ID;
- ``refused``: decoding fails;
- ``unpaired: <why>``: it decodes, and there is no byte check.

Every vendored ``.c4m`` must be named exactly once (a glob row counts). A
row naming a file c4py does not vendor is ignored.
"""

from __future__ import annotations

import hashlib
from pathlib import Path

import pytest

from c4py.decoder import loads
from c4py.encoder import dumps

VECTORS = Path(__file__).parent / "vectors"


def _round_trip_rows(readme: Path) -> list[tuple[str, str]]:
    """The (file, want) rows of a README's "## Round trip" table."""
    rows: list[tuple[str, str]] = []
    inside, header = False, 0
    for line in readme.read_text(encoding="utf-8").splitlines():
        if line.startswith("## "):
            inside = line == "## Round trip"
            continue
        if not inside or not line.startswith("|"):
            continue
        if header < 2:  # the header row and its separator
            header += 1
            continue
        cells = line.strip("|").split("|")
        assert len(cells) == 2, f"{readme}: a Round trip row needs two cells: {line!r}"
        rows.append((cells[0].strip().strip("`"), cells[1].strip()))
    return rows


def _quoted(s: str) -> str:
    i, j = s.find("`"), s.rfind("`")
    return s[i + 1 : j] if 0 <= i < j else ""


def _cases() -> list[tuple[str, str]]:
    """(path relative to VECTORS, want) for every vendored .c4m a row names."""
    cases = []
    for readme in sorted(VECTORS.glob("*/README.md")):
        d = readme.parent
        for pattern, want in _round_trip_rows(readme):
            for p in sorted(d.glob(pattern)):
                if p.suffix == ".c4m":
                    cases.append((p.relative_to(VECTORS).as_posix(), want))
    return cases


CASES = _cases()


def _sha512(b: bytes) -> bytes:
    return hashlib.sha512(b).digest()


def test_every_vendored_c4m_is_named_once():
    named: dict[str, int] = {}
    for rel, _ in CASES:
        named[rel] = named.get(rel, 0) + 1
    vendored = sorted(p.relative_to(VECTORS).as_posix() for p in VECTORS.rglob("*.c4m"))
    wrong = {rel: named.get(rel, 0) for rel in vendored if named.get(rel, 0) != 1}
    assert wrong == {}, "named this many times in the README's Round trip table, want once"


def test_every_vector_dir_with_c4m_files_has_a_table():
    for d in sorted(p for p in VECTORS.iterdir() if p.is_dir()):
        if any(d.rglob("*.c4m")):
            readme = d / "README.md"
            assert readme.exists() and _round_trip_rows(readme), f"{d.name}: no Round trip table"


@pytest.mark.parametrize("rel, want", CASES, ids=[rel for rel, _ in CASES])
def test_round_trip(rel, want):
    path = VECTORS / rel
    data = path.read_bytes()

    if want == "refused":
        with pytest.raises(ValueError):
            loads(data.decode("utf-8"))
        return

    m = loads(data.decode("utf-8"))
    out = dumps(m).encode("utf-8")

    if want == "itself":
        assert _sha512(out) == _sha512(data), f"{rel} does not re-encode to itself"
    elif want.startswith("unpaired:"):
        assert want.removeprefix("unpaired:").strip(), f"{rel}: unpaired without a reason"
    elif want.startswith("ID in "):
        expected = (path.parent / _quoted(want)).read_text().strip()
        assert str(m.compute_c4id()) == expected
    elif _quoted(want) and _quoted(want) == want.strip("`"):
        partner = (path.parent / _quoted(want)).read_bytes()
        assert _sha512(out) == _sha512(partner), f"{rel} does not re-encode to {_quoted(want)}"
    else:
        pytest.fail(f"{rel}: unknown Round trip entry {want!r}")
