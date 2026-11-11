"""Cross-implementation conformance: a directory with no listed children
and a null size (vectors/childless-dir-vector, byte-identical to the Go
reference's c4m/testdata/childless-dir-vector; README there).

What the size derives to depends on the directory's C4 ID alone:

    empty/      the empty listing's ID  -> 0: known to hold nothing
    collapsed/  any other ID           -> null: its listing lives elsewhere
    unread/     no ID                  -> null: nobody listed it

Through 2026-11-11 a collapsed directory derived to 0, and compute_c4id
hashed that invented size.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from c4py.decoder import loads
from c4py.encoder import dumps
from c4py.id import identify_bytes
from c4py.manifest import _propagate_metadata

VECTOR = Path(__file__).parent / "vectors" / "childless-dir-vector"
WANT = {"empty/": 0, "collapsed/": -1, "unread/": -1}


def _root_id() -> str:
    return (VECTOR / "root-id.txt").read_text().strip()


@pytest.mark.parametrize("form", ["stated.c4m", "normalized.c4m"])
def test_both_forms_resolve_to_the_root_id(form: str):
    m = loads((VECTOR / form).read_text())
    assert str(m.compute_c4id()) == _root_id()


@pytest.mark.parametrize("form", ["stated.c4m", "normalized.c4m"])
def test_sizes_derive_from_the_id_alone(form: str):
    m = loads((VECTOR / form).read_text())
    _propagate_metadata(m.entries)
    assert {e.name: e.size for e in m.entries} == WANT


def test_propagated_stated_form_writes_the_normalized_file():
    m = loads((VECTOR / "stated.c4m").read_text())
    _propagate_metadata(m.entries)
    assert dumps(m) == (VECTOR / "normalized.c4m").read_text()


def test_normalized_file_bytes_are_the_description():
    assert str(identify_bytes((VECTOR / "normalized.c4m").read_bytes())) == _root_id()
