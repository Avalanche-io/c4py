# Changelog

## Unreleased (1.1)

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
