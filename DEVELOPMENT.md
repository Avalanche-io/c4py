# pyc4 Development Guide

This document specifies the implementation requirements for pyc4. The project is scaffolded — module skeletons with docstrings and type signatures exist, test vectors from the Go reference implementation are provided, and test files are ready.

## Architecture

```
src/pyc4/
  __init__.py      # Public API (complete — re-exports from modules)
  id.py            # C4 ID computation (IMPLEMENTED — passes cross-language tests)
  entry.py         # Entry dataclass (partial — formatting TODO)
  manifest.py      # Manifest type (skeleton)
  decoder.py       # c4m parser (skeleton)
  encoder.py       # c4m writer (skeleton)
  naturalsort.py   # Natural sort (skeleton)
  safename.py      # Filename encoding (skeleton)
  scanner.py       # Directory scanner (skeleton)
  diff.py          # Diff operations (skeleton)
  validator.py     # Validation (skeleton)
```

## Implementation Priority

### Phase 1: Core (must ship in v0.1.0)

1. **id.py** — DONE. C4 ID computation, parsing, tree IDs. Passes all cross-language tests.

2. **naturalsort.py** — Implement `natural_sort_key()`.
   - Split names into alternating text/numeric segments
   - Numeric: compare as integers, then by string length for ties
   - Text sorts before numeric in mixed comparisons
   - Reference: `/Users/joshua/ws/active/c4/oss/c4/c4m/naturalsort.go`
   - Tests: `tests/test_naturalsort.py`

3. **safename.py** — Implement all four functions.
   - Tier 1: printable UTF-8 passthrough (except `¤` U+00A4 and `\`)
   - Tier 2: `\0`, `\t`, `\n`, `\r`, `\\`
   - Tier 3: non-printable → braille U+2800-U+28FF between `¤` delimiters
   - Field escaping: space→`\ `, `"`→`\"`, `[`→`\[`, `]`→`\]`
   - Reference: `/Users/joshua/ws/active/c4/oss/c4/c4m/safename.go`

4. **entry.py** — Complete `format_mode()` and `canonical()`.
   - Mode string: 10 chars, position 0 = file type, 1-9 = rwx with special bits
   - Use Python's `stat` module constants for bit extraction
   - Null mode (0) → `-`
   - Reference: `/Users/joshua/ws/active/c4/oss/c4/c4m/entry.go`

5. **decoder.py** — Implement `loads()` and `load()`.
   - Parse entry lines: split fields, handle indentation
   - Auto-detect indent width from first indented line
   - Handle null values: `-` for mode, timestamp, size, c4id
   - Parse timestamps: accept RFC3339 (canonical) and common variations
   - Handle link operators: symlinks, hard links, flow links
   - Reject @ directives, CR characters, invalid UTF-8
   - Bare C4 ID lines are patch boundaries (future — can defer to v0.2.0)
   - Reference: `/Users/joshua/ws/active/c4/oss/c4/c4m/decoder.go`

6. **encoder.py** — Implement `dumps()` and `dump()`.
   - Canonical mode: no indent, single spaces, UTC, null as `-`
   - Pretty mode: indented, padded sizes, column-aligned C4 IDs
   - Pretty column: default 80, shift to next 10-col boundary if needed
   - Reference: `/Users/joshua/ws/active/c4/oss/c4/c4m/encoder.go`

7. **manifest.py** — Complete `sort_entries()` and `compute_c4id()`.
   - Sort: files before directories at each depth, then natural sort
   - C4 ID: format all entries canonical, hash the UTF-8 bytes
   - Reference: `/Users/joshua/ws/active/c4/oss/c4/c4m/manifest.go`

8. **scanner.py** — Implement `scan()`.
   - Walk directory with `os.walk()` or `pathlib`
   - Stat each file for mode, timestamp, size
   - Compute C4 ID for regular files (streaming)
   - Record symlink targets
   - Build entry tree with correct depth
   - Sort and compute directory C4 IDs bottom-up
   - Reference: `/Users/joshua/ws/active/c4/oss/c4/c4m/builder.go`

9. **diff.py** — Implement `diff()`.
   - Match entries by full path (reconstruct path from name + depth)
   - Same path + same C4 ID + same metadata → same
   - Same path + different C4 ID or metadata → modified
   - Only in a → removed
   - Only in b → added
   - Reference: `/Users/joshua/ws/active/c4/oss/c4/c4m/operations.go`

10. **validator.py** — Implement `validate()`.
    - Check for duplicate paths
    - Check for path traversal (`../`, `./`)
    - Check indentation consistency
    - Check mode string format
    - Check timestamp format
    - Check C4 ID format (90 chars, c4 prefix, base58)
    - Check directory names end with /
    - Reference: `/Users/joshua/ws/active/c4/oss/c4/c4m/validator.go`

### Phase 2: Polish (v0.1.0 nice-to-have)

- Sequence notation parsing (frame.[0001-0100].exr)
- Patch format support (bare C4 ID lines, apply_patch)
- Pretty-print with local timezone detection

### Phase 3: Future (v0.2.0+)

- c4d client integration (absorb current c4-python code)
- c4git Python API
- Performance: optional C extension for SHA-512 (hashlib is already C-backed)

## Cross-Language Compatibility

**This is the most critical requirement.** pyc4 must produce byte-identical output to the Go reference implementation for:

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
cd /Users/joshua/ws/active/c4/oss/pyc4
pip install -e ".[dev]"
pytest
pytest --cov=pyc4
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
- **Cross-language tests are the spec.** If Go produces X for input Y, pyc4 must too.

## Reference Files

All paths are in the Go reference implementation at `/Users/joshua/ws/active/c4/oss/c4/`:

| Module | Go Reference |
|--------|-------------|
| id.py | `id.go`, `tree.go` |
| entry.py | `c4m/entry.go` |
| manifest.py | `c4m/manifest.go` |
| decoder.py | `c4m/decoder.go` |
| encoder.py | `c4m/encoder.go` |
| naturalsort.py | `c4m/naturalsort.go` |
| safename.py | `c4m/safename.go` |
| scanner.py | `c4m/builder.go` |
| diff.py | `c4m/operations.go` |
| validator.py | `c4m/validator.go` |

The c4m SPECIFICATION is at `/Users/joshua/ws/active/c4/oss/c4/c4m/SPECIFICATION.md`.
