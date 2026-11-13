"""A patch keeps range data: a chain's ID lists survive decode and re-encode.

Range data is a folded sequence's member ID list, an inline line after the
entries. Go's ``ApplyPatch`` (``c4m/operations.go``) merges the base's and
the patch's ``RangeData`` into the result. Through 2026-11-13 c4py's
``apply_patch`` kept neither, and the decoder's ``_apply_patch`` ignored a
patch section's (decided 2026-11-13 by the release lead; reversible).

Every c4m here was written by the Go library at c4 release/1.0.27
(``tests/fixtures/range-data-1027/``): the folded range and its list, and a
chain whose patch section adds that range with its list.
"""

from __future__ import annotations

from pathlib import Path

from c4py.decoder import _apply_patch, load, loads
from c4py.diff import apply_patch, resolve_chain
from c4py.encoder import dumps
from c4py.manifest import Manifest

FIXTURE = Path(__file__).parent / "fixtures" / "range-data-1027"


def _plate():
    folded = load(str(FIXTURE / "folded.c4m"))
    plate = next(e for e in folded.entries if e.is_sequence)
    return plate, folded._range_data[plate.c4id]


def _notes():
    return next(e for e in load(str(FIXTURE / "state2.c4m")).entries if e.name == "notes.txt")


def test_chain_patch_section_id_list_survives_decode_and_encode():
    plate, id_list = _plate()
    chain = (FIXTURE / "chain.c4m").read_text()
    assert chain.endswith(id_list + "\n")  # the list is in the patch section

    resolved = loads(chain)
    assert resolved._range_data == {plate.c4id: id_list}

    # The resolved state, re-encoded, is the second state as Go wrote it.
    assert dumps(resolved) == (FIXTURE / "state2.c4m").read_text()
    assert dumps(resolved).endswith(id_list + "\n")


def test_apply_patch_keeps_the_base_range_data():
    plate, id_list = _plate()
    base = Manifest(entries=[plate])
    base._range_data = {plate.c4id: id_list}

    result = apply_patch(base, [_notes()])
    assert result._range_data == {plate.c4id: id_list}
    assert result._range_data is not base._range_data


def test_apply_patch_merges_a_patch_manifest_range_data():
    plate, id_list = _plate()
    patch = Manifest(entries=[plate])
    patch._range_data = {plate.c4id: id_list}

    result = apply_patch(Manifest(entries=[_notes()]), patch)
    assert result._range_data == {plate.c4id: id_list}
    assert dumps(result).endswith(id_list + "\n")


def test_decoder_apply_patch_keeps_the_patch_section_range_data():
    plate, id_list = _plate()
    base = Manifest(entries=[_notes()])
    patch = Manifest(entries=[plate])
    patch._range_data = {plate.c4id: id_list}

    assert _apply_patch(base, patch)._range_data == {plate.c4id: id_list}


def test_resolve_chain_keeps_the_range_data():
    plate, id_list = _plate()
    m = Manifest()
    m.patch_sections = [[_notes()], [plate]]
    m._range_data = {plate.c4id: id_list}

    assert resolve_chain(m)._range_data == {plate.c4id: id_list}
