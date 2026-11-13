"""Range data survives a decode-and-encode round trip, byte for byte.

data loss fixed: re-encoded c4m files kept no range data; they now keep it,
so their bytes and byte IDs match the input (decided 2026-11-13 by the
release lead; reversible). The fixture under
``tests/fixtures/range-data-1027/`` was written by the Go library at c4
release/1.0.27; see its README. SHA-512 comes from ``hashlib``, and the
byte ID check is the C4 ID of the bytes the encoder writes.
"""

from __future__ import annotations

import hashlib
from pathlib import Path

from c4py.decoder import loads
from c4py.encoder import dumps
from c4py.id import identify_bytes

FIXTURE = Path(__file__).parent / "fixtures" / "range-data-1027"


def test_folded_range_reencodes_byte_for_byte():
    data = (FIXTURE / "folded.c4m").read_bytes()
    out = dumps(loads(data.decode("utf-8"))).encode("utf-8")
    assert hashlib.sha512(out).digest() == hashlib.sha512(data).digest()
    assert str(identify_bytes(out)) == (FIXTURE / "folded-id.txt").read_text().strip()


def test_chain_resolved_state_keeps_the_patch_section_list():
    chain = (FIXTURE / "chain.c4m").read_bytes()
    assert str(identify_bytes(chain)) == (FIXTURE / "chain-id.txt").read_text().strip()

    folded = loads((FIXTURE / "folded.c4m").read_text())
    resolved = loads(chain.decode("utf-8"))
    assert resolved._range_data == folded._range_data

    state2 = (FIXTURE / "state2.c4m").read_bytes()
    out = dumps(resolved).encode("utf-8")
    assert hashlib.sha512(out).digest() == hashlib.sha512(state2).digest()


def test_description_id_does_not_depend_on_the_list():
    folded = loads((FIXTURE / "folded.c4m").read_text())
    without = folded.copy()
    without._range_data = {}
    assert folded.compute_c4id() == without.compute_c4id()
