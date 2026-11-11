"""Cross-implementation conformance: a directory with no listed children
and a null size (vectors/childless-dir-vector, byte-identical to the Go
reference's c4m/testdata/childless-dir-vector; README there).

What the size derives to depends on the directory's C4 ID alone:

    empty/      the empty listing's ID  -> 0: known to hold nothing
    collapsed/  any other ID           -> null: its listing lives elsewhere
    unread/     no ID                  -> 0 on 1.0.x; null on main

Through 1.0.26 a collapsed directory derived to 0, and compute_c4id hashed
that invented size. The unread/ case is a known 1.0.x difference, as in the
Go reference's 1.0.27 line: a childless directory with no ID keeps size 0
here, and main keeps it null. So the committed normalized.c4m and root-id.txt
are main's, and this line's own normalised form and ID are checked against
themselves.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from c4py.decoder import loads
from c4py.encoder import dumps
from c4py.id import identify_bytes
from c4py.manifest import _propagate_metadata

VECTOR = Path(__file__).parent / "vectors" / "childless-dir-vector"
WANT = {"empty/": 0, "collapsed/": -1, "unread/": 0}
# The 1.0.x line's root ID, the same in every implementation's 1.0.27 line (the Go
# reference's included): unread/ at 0, collapsed/ null.
LINE_ROOT_ID = "c43hjzfaERSHQSgDYwRPabKdXuC1gbHSW1BnXPVhvCm8LNgYj5LEH6a3VoV2gckqiLNMBQEh4dTVKszriwrmZryUkh"


def _root_id() -> str:
    return (VECTOR / "root-id.txt").read_text().strip()


def _line_normalized() -> str:
    m = loads((VECTOR / "stated.c4m").read_text())
    _propagate_metadata(m.entries)
    return dumps(m)


@pytest.mark.parametrize("form", ["stated.c4m", "normalized.c4m"])
def test_sizes_derive_from_the_id_alone(form: str):
    m = loads((VECTOR / form).read_text())
    _propagate_metadata(m.entries)
    assert {e.name: e.size for e in m.entries} == WANT


def test_mains_normalized_file_differs_only_in_unread_size():
    main = loads((VECTOR / "normalized.c4m").read_text())
    for e in main.entries:
        if e.name == "unread/":
            e.size = 0
    assert dumps(main) == _line_normalized()


def test_this_lines_id_agrees_with_itself_and_differs_from_mains():
    stated = loads((VECTOR / "stated.c4m").read_text())
    line_id = str(stated.compute_c4id())
    assert line_id == LINE_ROOT_ID
    normalized = _line_normalized()
    assert str(loads(normalized).compute_c4id()) == line_id
    assert str(identify_bytes(normalized.encode("utf-8"))) == line_id
    # If this ever matches, the unread/ difference is gone: update the test.
    assert line_id != _root_id()


def test_normalized_file_bytes_are_the_description():
    assert str(identify_bytes((VECTOR / "normalized.c4m").read_bytes())) == _root_id()
