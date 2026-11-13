"""A patch keeps range data: a chain's ID lists survive decode and re-encode.

Range data is a folded sequence's member ID list, an inline line after the
entries. Go's ``ApplyPatch`` (``c4m/operations.go``) merges the base's and
the patch's ``RangeData`` into the result. Through 2026-11-13 c4py's
``apply_patch`` kept neither, and the decoder's ``_apply_patch`` ignored a
patch section's (decided 2026-11-13 by the release lead; reversible).

Every c4m here is written by the library: the sequence entry and its list
come from decoding the shared ``sequence-null-size-vector/folded.c4m``, and
the chain is the encoder's output for a base, then for a patch section
(a manifest whose ``base`` is the first state's ID).
"""

from __future__ import annotations

from pathlib import Path

from c4py.decoder import _apply_patch, load, loads
from c4py.diff import apply_patch, resolve_chain
from c4py.encoder import dumps
from c4py.manifest import Manifest

VECTOR = Path(__file__).parent / "vectors" / "sequence-null-size-vector"


def _plate():
    folded = load(str(VECTOR / "folded.c4m"))
    plate = next(e for e in folded.entries if e.name == "plate.[1001-1004].exr")
    return plate, folded._range_data[plate.c4id]


def _frame():
    return load(str(VECTOR / "frames.c4m")).entries[4]  # shot.1001.exr


def test_chain_patch_section_id_list_survives_decode_and_encode():
    plate, id_list = _plate()
    first = Manifest(entries=[_frame()])

    patch = Manifest(entries=[plate])
    patch.base = first.compute_c4id()
    patch._range_data = {plate.c4id: id_list}
    chain = dumps(first) + dumps(patch)
    assert chain.endswith(id_list + "\n")  # the list is in the patch section

    resolved = loads(chain)
    assert resolved._range_data == {plate.c4id: id_list}

    # The resolved state, re-encoded, is the second state written whole.
    second = Manifest(entries=[_frame(), plate])
    second._range_data = {plate.c4id: id_list}
    assert dumps(resolved) == dumps(second)
    assert dumps(resolved).endswith(id_list + "\n")


def test_apply_patch_keeps_the_base_range_data():
    plate, id_list = _plate()
    base = Manifest(entries=[plate])
    base._range_data = {plate.c4id: id_list}

    result = apply_patch(base, [_frame()])
    assert result._range_data == {plate.c4id: id_list}
    assert result._range_data is not base._range_data


def test_apply_patch_merges_a_patch_manifest_range_data():
    plate, id_list = _plate()
    patch = Manifest(entries=[plate])
    patch._range_data = {plate.c4id: id_list}

    result = apply_patch(Manifest(entries=[_frame()]), patch)
    assert result._range_data == {plate.c4id: id_list}
    assert dumps(result).endswith(id_list + "\n")


def test_decoder_apply_patch_keeps_the_patch_section_range_data():
    plate, id_list = _plate()
    base = Manifest(entries=[_frame()])
    patch = Manifest(entries=[plate])
    patch._range_data = {plate.c4id: id_list}

    assert _apply_patch(base, patch)._range_data == {plate.c4id: id_list}


def test_resolve_chain_keeps_the_range_data():
    plate, id_list = _plate()
    m = Manifest()
    m.patch_sections = [[_frame()], [plate]]
    m._range_data = {plate.c4id: id_list}

    assert resolve_chain(m)._range_data == {plate.c4id: id_list}
