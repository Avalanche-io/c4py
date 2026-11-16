# Changelog

## Unreleased (1.1)

### ⚠ Scan and diff work at the format's resolution: whole seconds

data loss fixed: `patch_diff(load(old), scan(dir))`, the documented
workflow, wrote every unchanged entry of the scan into the patch, because
the scan kept sub-second times and the loaded c4m holds whole seconds.
Each such line, once encoded, was byte-identical to the base's, and
`c4 patch` (and `resolve_chain`) rightly read an identical line as a
removal, so unchanged files were deleted from the resolved state. `diff`
and `patch_diff` now compare timestamps to the second, as Go's c4m does
(`entriesIdentical` and `entriesEqual`, `c4m/operations.go`).
correctness fixed: `scan` converted the float `st_mtime`, which rounds to
the microsecond, so an mtime in the last half microsecond of a second was
recorded as the next second, where Go writes the second it falls in. It
now floors `st_mtime_ns` to whole seconds, so a scan's entries hold
exactly what their c4m lines carry. **C4 IDs change** for a tree holding
such a file: they now match the Go CLI's. Tested by the shared
subsecond-vector, vendored from c4 #173.

### `patch_diff` writes its section in canonical order, as `c4 diff` does

`patch_diff` emitted its entries in sorted path order, so within a
directory a subdirectory came before its sibling files. A patch section is
canonical c4m text: "files before directories, then natural sort within
each group" (SPECIFICATION.md). Core and c4py both read the old order back
to the same state, but the changeset's bytes, and so its C4 ID, were not
`c4 diff`'s. The section is now sorted the way every listing is. The shared
changeset vector (c4 #175, `tests/vectors/changeset-vector/`, vendored byte
for byte) pins it: given `base.c4m` and `v2.c4m`, `patch_diff` writes
`changeset.c4m` byte for byte, and `chain.c4m` resolves to `v2.c4m`.
Measured before the change: the writer row failed (`deep/` before
`gone.txt` and `new.txt`); every other row passed.

### ⚠ A range's ID list goes with its range through merge, diff, patch and filter

data loss fixed: `merge`, `patch_diff` and `Manifest.filter` wrote no ID
list for any folded range they output, so the frames' C4 IDs were gone
from the result; they now carry the list of every range they output.
correctness fixed: `apply_patch`, the decoder's patch step and
`resolve_chain` kept the list of a range the patch removed; a removed
range now takes its list with it. The rule follows c4 core #159 (Go's
`rangeDataFor`, `c4m/rangedata.go`), ported as `range_data_for` in
`c4py.manifest`: an output carries the ID list of every range it holds,
from whichever input carried it, and no list for a range it does not
hold; a list no input carries stays absent. `patch_diff` writes the
lists of the ranges it adds or changes after its entries, as `c4 diff`
does; a removal line takes none. Ruled 2026-11-13 by the maintainer: "A
frame id list is a part of the c4m format, and is the same thing as a
subdirectory being referenced by it's ID on a parents directory line."
The lists are part of no description ID, so no C4 ID c4py computes
changes; the bytes, and so the byte ID, of a merged, diffed, patched or
filtered c4m holding a range do. Tested by the shared range-data vector,
vendored from c4 b82ec07.

### ⚠ Vectors re-vendored: a folded range's size counts its ID list

Ruled 2026-11-13 by the maintainer: "The size is the size of every frame
and the frame id list." A range's size is its frames' bytes plus
count × 90, its list's bytes (c4 core #161). **The C4 IDs of the
directories holding folded ranges, and of every directory above them,
change** wherever core folds: a range line's size feeds its directory's
listing and so its ID. A range's own C4 ID, its list's ID, does not
change (measured: the vector's plate line keeps its ID, only its size
moves from 24 to 384).
c4py does not fold sequences and has no expander, so no C4 ID c4py
computes for a given c4m changes: it reads a range's size as stated, and
its normaliser already derived a directory's size over a range child
from that stated size (446 for the vector's `sub/`, where Go's derived
175). The sequence-null-size vector is re-vendored from c4 b82ec07 (the
plate range is now 384, and its root ID changed with it), and the
sequence-size vector is new, with its decode side and the normaliser's
derivation tested. A range whose list is not inline stays one folded
entry; c4py never makes frames from it (c4 #160, no list, no expansion).
CI's c4 CLI pin stays at b17c393: the comparison test's output is the
same at b82ec07 (measured; c4py's scanner does not fold).

### ⚠ Range data kept on re-encode and through patch sections

data loss fixed: re-encoded c4m files kept no range data; they now keep it,
so their bytes and byte IDs match the input. Range data is a folded
sequence's member ID lists, the inline lines after the entries. The decoder
kept them; the encoder dropped them. It now writes them after the entries,
sorted by C4 ID, as the Go encoder's `Encoder.Encode` does. Patch sections
too: `apply_patch` returned a manifest with no range data, the decoder's
patch step ignored a patch section's, and `resolve_chain` rebuilt from
entries alone. Each now keeps the base's range data merged with the
patch's, the patch's winning on a shared key, as Go's `ApplyPatch`
(`c4m/operations.go`) does; `apply_patch` also takes the patch as a
`Manifest` for that. Root and description IDs do not change: range data
is not part of either. Found by the sequence-null-size vector (decided
2026-11-13 by the release lead; reversible).

### A folded sequence's size is null when a frame's size is; every vector round-trips byte for byte

Ruled 2026-11-13: "One cannot use size as an expectation if there are
nulls present, so we must say nothing." A folded sequence (a range like
`shot.[1001-1004].exr`) states the sum of its frames' sizes, and its size
is null if any frame's size is null (`-`, or a `0` beside a C4 ID other
than the empty content's), as a null child does a directory's (c4 core
PR #155; decided 2026-11-13 by the release lead; reversible). c4py does
not fold sequences, so no C4 ID it computes changes, and it already
decoded a range's `-` size as null. The shared vector is vendored from
c4 commit 7a6ba94 (#156) as `tests/vectors/sequence-null-size-vector/`,
and its decode half is tested. A new test decodes and canonically
re-encodes every vendored vector `.c4m` and compares SHA-512 digests
(hashlib) as each vector README's `## Round trip` table states (c4 #158
at 7cdc9f3, whose READMEs and canonical partner files are re-vendored): a
file re-encodes to itself or to its library-written canonical partner's
exact bytes, a chain resolves to the stated ID, and a damaged file is
refused; there is no other kind of row. Every vendored `.c4m` must be
named exactly once. CI's c4 CLI pin moves
from 5ee54b6 to b17c393.

### CI fails on a test skip the allow-list does not name

A port can diverge silently from a shared vector when the comparison test
skips (c4py's NBSP case was one). CI now runs pytest with `--junitxml`,
and `scripts/check_skips.py` (stdlib only) prints every skipped or xfailed
test with its reason and fails on one that `tests/allowed-skips.txt` does
not name for that platform (`sys.platform`, or `*`). A listed skip that did
not happen is reported, not failed on. Measured on every CI leg first: the
only skip was `test_matches_go_cli_byte_for_byte`, which needs the Go c4
CLI. CI now installs it (`go install …/cmd/c4@5ee54b6`, pinned), the test
finds it through `C4_BIN` or PATH, and the allow-list is empty. The test
now compares c4py's output with the flattened form, `c4 id <dir> | c4 patch
-` (c4 master's `c4 id` streams a chain), and checks that `c4 id -q <dir>`
equals c4py's description ID. Both match on every leg.

### ⚠ SafeName's printable test is pinned to Unicode 17.0.0

SafeName writes a character as itself (Tier 1) when it is printable. c4py
decided that with `unicodedata`, which follows the interpreter's Unicode
(13.0 on Python 3.10, 14.0 on 3.11, 15.1 on 3.13), and it counted every
`Zs` space separator as printable where Go's `unicode.IsPrint` admits only
U+0020. So U+0870 (assigned in Unicode 14.0) was escaped on Python 3.10 and
written plainly on 3.11+, and U+00A0 and the other non-ASCII spaces were
written plainly where every other implementation escapes them: one name,
two encodings, two C4 IDs. The test is now one table pinned to Unicode
17.0.0, `tests/vectors/safename-vector/printable-17.0.0.txt` (shared with
c4 core, PR #149), embedded as `c4py/_printable.py` by
`scripts/gen_printable.py` and searched by bisection; `unicodedata` is no
longer used (decided 2026-11-13 by the release lead; reversible). **C4 IDs
change** for a name holding a code point whose class differed between the
interpreter's Unicode and 17.0.0, and for a name holding a non-ASCII space
separator, on every Python. The shared safename vector's last held case
now passes.

### Encoding a description keeps its external base

`dumps` and `dump` never wrote a manifest's `base`, the first-line bare
C4 ID naming the block a description refines, although `loads` keeps it.
So `loads` then `dumps` turned a block on a base into a standalone
description of the block's own entries, silently (found 2026-11-13 in c4
core, PR #145). Canonical and pretty encoding now write a set base as the
first line. A manifest with no base encodes exactly as before, and the
description ID (`compute_c4id`) is unchanged. `patch_diff` already writes
its own first line (the old state's ID) and never writes either input's
base.

### ⚠ A size of 0 beside a C4 ID other than the empty content's is null

Only the empty content has size 0. A `0` on a file or directory line
beside any other C4 ID is therefore a null size written by a surface that
has no `-` (a mount), and is read as one (ruled 2026-11-08; c4 core PR
#68, landed 2026-11-13, decided by the release lead; reversible).
`Entry.has_null_size()` is now the one test: the decoder holds such a `0`
as -1, canonical form and both encoders write `-`, and the parent
directory's size goes null over it, so `0 f <ID>` and `- f <ID>` are one
description with one ID. A `0` with no C4 ID, a `0` with the empty
content's ID, and a symlink's `0` stay known zeros.
`c4py.entry.EMPTY_CONTENT_ID` names the ID of zero bytes.

This changes the C4 ID of any description holding the combination,
including one built through the API with `size=0` beside a real content
ID. No shipped scanner writes it. The shared `null-size-vector` fixture
is copied from c4 core.

### ⚠ Breaking: a c4m file that ends with a bare C4 ID is refused

A c4m file never ends with its own ID (ruled 2026-11-08). A bare C4 ID is
the first-line external base reference or a checkpoint before a patch
section; one at end of input is a format error. `load` and `loads` now
raise `c4py.TrailingIDError` (a `ValueError`) for it, naming the line and
the fix. Until now c4py accepted a correct trailing ID as a "closing
validator".

Files of this shape were written by c4 1.0.13 through 1.0.26 with
`c4 diff FILE.c4m DIR >> FILE.c4m`. To read one again, delete its final
line, or rewrite it with `c4 id FILE.c4m > fixed.c4m` (c4 1.1), which
checks the ID before dropping it. c4py has no normaliser of its own and
does not migrate them.

A wrong trailing ID is still reported as a patch ID mismatch, because the
ID is checked as a checkpoint before the end of the file is reached, as
in c4 core. A file holding only a first-line base reference is unchanged:
it reads as an empty manifest with that base.

`patch_diff(old, new)` now returns `""` when nothing changed, as
`c4 diff` prints nothing. Before, it returned the old ID alone, which,
appended to `old.c4m`, ended the file with a bare ID.

The shared `chain-vector` and `deepen-vector` fixtures are updated from
c4 core: `vector.c4m`, `bad-checkpoint.c4m`, `deepen.c4m` and `remove.c4m`
lose their final bare-ID line, and their root IDs are unchanged.
