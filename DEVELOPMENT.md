# c4py Development Guide

This document specifies the implementation requirements for c4py. All modules are implemented and passing tests. Test vectors from the Go reference implementation verify cross-language compatibility.

## Architecture

```
src/c4py/
  __init__.py      # Public API (complete — re-exports from modules)
  id.py            # C4 ID computation (IMPLEMENTED — 27/27 cross-language tests pass)
  store.py         # Content store (IMPLEMENTED — 13/13 tests pass, adaptive trie sharding)
  entry.py         # Entry dataclass (IMPLEMENTED)
  manifest.py      # Manifest type (IMPLEMENTED)
  decoder.py       # c4m parser (IMPLEMENTED)
  encoder.py       # c4m writer (IMPLEMENTED)
  naturalsort.py   # Natural sort (IMPLEMENTED)
  safename.py      # Filename encoding (IMPLEMENTED)
  scanner.py       # Directory scanner (IMPLEMENTED)
  diff.py          # Diff operations (IMPLEMENTED)
  validator.py     # Validation (IMPLEMENTED)
```

## CLI Alignment (c4 v1.0.0)

c4py maps to the c4 CLI (8 commands: id, cat, diff, patch, merge, log, split, version).

| c4py function | c4 CLI | Description |
|---------------|--------|-------------|
| `identify()` / `scan()` | `c4 id` | Identify content, scan directories |
| `store.get()` | `c4 cat` | Retrieve content by C4 ID |
| `patch_diff()` | `c4 diff` | Produce c4m patch between two states |
| `resolve_chain()` | `c4 patch` | Resolve patch chain to final state |
| `merge()` | `c4 merge` | Three-way merge with LWW conflict resolution |
| `log_chain()` | `c4 log` | Enumerate patches in a chain |
| `diff()` | — | High-level comparison (added/removed/modified) |

Key interop points:
- **Shared content store:** TreeStore adaptive trie, same config (`C4_STORE` / `~/.c4/config`).
- **`scan(store=...)` maps to `c4 id -s`:** Single-pass identify + store.
- **Canonical output matches `c4 id`:** `c4py.dumps(manifest)` produces byte-identical output.
- **Reconciliation:** c4py includes a full reconcile module (`reconcile.py`) and workspace module (`workspace.py`) for applying manifest state to directories, matching the Go `reconcile` package.

## Implementation Priority

### Phase 1: Core (must ship in v0.1.0)

1. **id.py** — DONE. C4 ID computation, parsing, tree IDs. 27/27 cross-language tests pass.

2. **store.py** — DONE. FSStore with adaptive trie sharding, open_store() config
   discovery, ContentNotFound error. 13/13 tests pass. Compatible with Go CLI layout.

3. **naturalsort.py** — Implement `natural_sort_key()`.
   - Split names into alternating text/numeric segments
   - Numeric: compare as integers, then by string length for ties
   - Text sorts before numeric in mixed comparisons
   - Reference: `/Users/joshua/ws/active/c4/oss/c4/c4m/naturalsort.go`
   - Tests: `tests/test_naturalsort.py`

4. **safename.py** — Implement all four functions.
   - Tier 1: printable UTF-8 passthrough (except `¤` U+00A4 and `\`)
   - Tier 2: `\0`, `\t`, `\n`, `\r`, `\\`
   - Tier 3: non-printable → braille U+2800-U+28FF between `¤` delimiters
   - Field escaping: space→`\ `, `"`→`\"`, `[`→`\[`, `]`→`\]`
   - Reference: `/Users/joshua/ws/active/c4/oss/c4/c4m/safename.go`

5. **entry.py** — Complete `format_mode()` and `canonical()`.
   - Mode string: 10 chars, position 0 = file type, 1-9 = rwx with special bits
   - Use Python's `stat` module constants for bit extraction
   - Null mode (0) → `-`
   - Reference: `/Users/joshua/ws/active/c4/oss/c4/c4m/entry.go`

6. **decoder.py** — Implement `loads()` and `load()`.
   - Parse entry lines: split fields, handle indentation
   - Auto-detect indent width from first indented line
   - Handle null values: `-` for mode, timestamp, size, c4id
   - Parse timestamps: accept RFC3339 (canonical) and common variations
   - Handle link operators: symlinks, hard links, flow links
   - Reject @ directives, CR characters, invalid UTF-8
   - Skip inline ID list lines (range data): len > 90, len % 90 == 0, all 90-char
     chunks are valid C4 IDs. These are NOT patch boundaries.
   - Bare C4 ID lines (exactly 90 chars) are patch boundaries
   - Reference: `/Users/joshua/ws/active/c4/oss/c4/c4m/decoder.go`
   - Reference: `/Users/joshua/ws/active/c4/oss/c4/c4m/chain.go`

7. **encoder.py** — Implement `dumps()` and `dump()`.
   - Canonical mode: no indent, single spaces, UTC, null as `-`
   - Pretty mode: indented, padded sizes, column-aligned C4 IDs
   - Pretty column: default 80, shift to next 10-col boundary if needed
   - Reference: `/Users/joshua/ws/active/c4/oss/c4/c4m/encoder.go`

8. **manifest.py** — Complete `sort_entries()` and `compute_c4id()`.
   - Sort: files before directories at each depth, then natural sort
   - C4 ID: format all entries canonical, hash the UTF-8 bytes
   - Reference: `/Users/joshua/ws/active/c4/oss/c4/c4m/manifest.go`

9. **scanner.py** — Implement `scan()`.
   - Walk directory with `os.walk()` or `pathlib`
   - Stat each file for mode, timestamp, size
   - Compute C4 ID for regular files (streaming)
   - When `store` is provided: tee read stream to store.put() (single pass,
     zero extra I/O). This matches `c4 id -s` in the CLI.
   - Record symlink targets
   - Build entry tree with correct depth
   - Sort and compute directory C4 IDs bottom-up
   - Reference: `/Users/joshua/ws/active/c4/oss/c4/c4m/builder.go`

10. **diff.py** — Implement all functions:
    - `diff(a, b)` — high-level comparison: match by path, categorize as
      added/removed/modified/same
    - `patch_diff(old, new)` — produce c4m patch text. First line is bare C4 ID
      of old (base state). Patch entry semantics: exact duplicate = removal,
      same path different content = modification, new path = addition.
      Maps to: `c4 diff before.c4m after.c4m`
    - `apply_patch(base, entries)` — apply patch entries to base manifest
    - `resolve_chain(manifest)` — resolve all patches to final state.
      Maps to: `c4 patch project.c4m`
    - `log_chain(manifest)` — enumerate patches with summary stats.
      Maps to: `c4 log project.c4m`
    - Reference: `/Users/joshua/ws/active/c4/oss/c4/c4m/operations.go`
    - Reference: `/Users/joshua/ws/active/c4/oss/c4/design/cli/phase1_cli_redesign.md`

11. **validator.py** — Implement `validate()`.
    - Check for duplicate paths
    - Check for path traversal (`../`, `./`)
    - Check indentation consistency
    - Check mode string format
    - Check timestamp format
    - Check C4 ID format (90 chars, c4 prefix, base58)
    - Check directory names end with /
    - Verify bare C4 ID patch boundaries match accumulated content
    - Reference: `/Users/joshua/ws/active/c4/oss/c4/c4m/validator.go`

### Phase 2: Polish (v0.1.0 nice-to-have)

- Sequence notation parsing (frame.[0001-0100].exr)
- Pretty-print with local timezone detection
- `c4 split` equivalent (extract patch range from chain)

### Phase 3: Future (v0.2.0+)

- Network client integration (absorb current c4-python code)
- c4git Python API
- Performance: optional C extension for SHA-512 (hashlib is already C-backed)

## Cross-Language Compatibility

**This is the most critical requirement.** c4py must produce byte-identical output to the Go reference implementation for:

1. C4 ID strings (given same input bytes)
2. Canonical c4m text (given same manifest)
3. Natural sort order (given same entry names)
4. Manifest C4 IDs (given same directory contents)

Test vectors are in `tests/vectors/known_ids.json`, generated from the Go implementation.

To generate additional test vectors:
```bash
cd /Users/joshua/ws/active/c4/oss/c4
go run /tmp/c4vectors.go
```

## Running Tests

```bash
cd /Users/joshua/ws/active/c4/oss/c4py
pip install -e ".[dev]"
pytest
pytest --cov=c4py
mypy src/
ruff check src/ tests/
```

## Code Style

- Python 3.10+ (use `X | Y` union syntax, not `Union[X, Y]`)
- Type annotations on all public functions
- mypy strict mode must pass
- ruff for linting
- No external dependencies (stdlib only)
- Docstrings on all public classes and functions
- Keep it minimal — match the C4 philosophy

## Key Design Decisions

- **Pure Python, zero dependencies.** hashlib.sha512 is C-backed and fast enough.
- **Streaming identification.** `identify()` reads in chunks, constant memory.
- **Immutable C4ID.** The digest is set once at construction.
- **Lenient parser, strict encoder.** Accept variations, always output canonical.
- **Cross-language tests are the spec.** If Go produces X for input Y, c4py must too.

## Reference Files

All paths are in the Go reference implementation at `/Users/joshua/ws/active/c4/oss/c4/`:

| Module | Go Reference |
|--------|-------------|
| id.py | `id.go` (C4ID, Parse, Identify) |
| store.py | `store/treestore.go`, `store/config.go` |
| entry.py | `c4m/entry.go` |
| manifest.py | `c4m/manifest.go` |
| decoder.py | `c4m/decoder.go`, `c4m/chain.go` (patch chain parsing) |
| encoder.py | `c4m/encoder.go` |
| naturalsort.py | `c4m/naturalsort.go` |
| safename.py | `c4m/safename.go` |
| scanner.py | `cmd/c4/internal/scan/` (progressive scanner with modes) |
| diff.py | `c4m/operations.go` (PatchDiff, ApplyPatch, Diff), `c4m/chain.go` (ResolvePatchChain), `c4m/merge.go` (Merge) |
| validator.py | `c4m/validator.go` |

**Key specs:**
- c4m format: `c4m/SPECIFICATION.md`
- c4m standard (ABNF grammar): `c4m/C4M-STANDARD.md`
- Unix recipes: `docs/c4m-unix-recipes.md`

**Note:** The c4 v1.0.0 release (branch `release/v1.0`) contains 6 packages:
`c4`, `c4m`, `store`, `reconcile`, `cmd/c4`, `cmd/c4/internal/scan`.
Non-essential packages (chunk, transform, db, hashlib, progscan) were stripped.
Only depend on APIs from the shipping packages.
