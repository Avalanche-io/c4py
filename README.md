# c4py

[![CI](https://github.com/Avalanche-io/c4py/actions/workflows/ci.yml/badge.svg)](https://github.com/Avalanche-io/c4py/actions/workflows/ci.yml)
[![Apache 2.0](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](./LICENSE)
[![Python 3.10+](https://img.shields.io/badge/python-3.10+-blue.svg)](https://www.python.org)

Pure Python implementation of [C4](https://github.com/Avalanche-io/c4) universal content identification (SMPTE ST 2114:2017).

```python
import c4py

# Verify a delivery matches the manifest
report = c4py.verify_tree("delivery-v3.c4m", "/deliveries/v3/")
for bad in report.corrupt:
    print(f"MISMATCH: {bad.path}")

# Identify any file — same content always produces the same ID
c4id = c4py.identify_file("render.1001.exr")

# Compare two snapshots of a project
old = c4py.load("delivery-v2.c4m")
new = c4py.load("delivery-v3.c4m")
diff = c4py.diff(old, new)
print(f"+{len(diff.added)} -{len(diff.removed)} ~{len(diff.modified)}")
```

## Install

```bash
pip install c4py
```

## What is C4?

C4 IDs are universally unique, unforgeable identifiers derived from content using SHA-512. They are standardized as [SMPTE ST 2114:2017](https://ieeexplore.ieee.org/document/7971777). Same content always produces the same 90-character ID, regardless of filename, location, or time.

```python
>>> import c4py
>>> c4py.identify_bytes(b"hello world")
C4ID('c41yP4cqy7jmaRDzC2bmcGNZkuQb3VdftMk6YH7ynQ2Qw4zktKsyA9fk52xghNQNAdkpF9iFmFkKh2bNVG4kDWhsok')
```

## C4M Format

A **c4m file** is a human-readable text file that describes a filesystem. It captures file names, sizes, permissions, timestamps, and C4 IDs in a format you can read, edit, diff, and email.

```
-rw-r--r-- 2025-06-15T12:00:00Z      3 README.md   c45JwjSStebb...
drwxr-xr-x 2025-06-15T12:00:00Z    136 src/        c45bqBopeQt9...
  -rw-r--r-- 2025-06-15T12:00:00Z    3 main.go     c43U5CUKjg7Z...
```

A 2 KB c4m file can describe an 8 TB project. Compare two c4m files to find exactly which frames changed across a delivery — in seconds, not hours.

## API

### Identification

```python
# Identify a file on disk
c4id = c4py.identify_file("render.1001.exr")

# Verify a file matches an expected ID
assert c4py.identify_file("render.1001.exr") == expected_id

# From file-like object (streaming, constant memory)
c4id = c4py.identify(file_obj)

# From bytes
c4id = c4py.identify_bytes(b"data")

# Parse a C4 ID string (also works as C4ID constructor)
c4id = c4py.parse("c45xZeXwMSpq...")
c4id = c4py.C4ID("c45xZeXwMSpq...")  # same thing

# C4 ID properties
str(c4id)       # 90-character string
bytes(c4id)     # 64-byte digest
c4id.hex()      # hex digest
bool(c4id)      # False for nil ID, True otherwise
```

### Tree IDs (Set Identity)

```python
# Compute a single ID for a set of IDs (order-independent)
tree_id = c4py.tree_id([id_a, id_b, id_c])
```

### C4M Files

```python
# Parse
manifest = c4py.load("project.c4m")
manifest = c4py.loads(text)

# Write
c4py.dump(manifest, file_obj)
text = c4py.dumps(manifest)
text = c4py.dumps(manifest, pretty=True)

# Scan directory
manifest = c4py.scan("/path/to/dir")

# Iterate entries
for entry in manifest:
    print(entry.name, entry.size, entry.c4id)
```

### Content Store

c4py reads and writes the same content store as the `c4` CLI and `c4sh` when `C4_STORE` or the `store` line in `~/.c4/config` names one local path: content stored by any of them is available to the others. c4py doesn't read the comma-separated list of stores that `c4` accepts.

```python
# Open a store (auto-discovers from C4_STORE env or ~/.c4/config)
store = c4py.open_store()

# Or specify a path
store = c4py.open_store("/data/c4store")

# Store and retrieve content
c4id = store.put(open("render.exr", "rb"))
assert store.has(c4id)
with store.get(c4id) as f:  # a binary file object
    content = f.read()

# Scan + store in one pass (zero extra I/O). File content only:
# unlike `c4 id -s`, directory listings aren't stored.
manifest = c4py.scan("/projects/HERO", store=store)
```

### Diff and Patch Chains

```python
# Compare two manifests
diff = c4py.diff(old_manifest, new_manifest)
diff.added      # entries only in new
diff.removed    # entries only in old
diff.modified   # same path, different content
diff.same       # identical entries

# Produce a c4m patch (like `c4 diff before.c4m after.c4m`)
patch_text = c4py.patch_diff(old_manifest, new_manifest)

# c4m files can contain version histories (base + patches).
# load() applies every patch as it reads, so a loaded chain is already its
# final state (like `c4 patch project.c4m`); resolve_chain returns it as is.
final = c4py.load("project.c4m")

# load() keeps no sections, so log_chain of a loaded file gives one entry,
# the final state. For a file's history use `c4 log project.c4m`.
for info in c4py.log_chain(final):
    print(f"{info.index}  {info.c4id}  +{info.added} -{info.removed} ~{info.modified}")
```

### Validation

```python
result = c4py.validate(manifest)
for issue in result.errors:
    print(issue)
```

## Works With

c4py is part of the [C4 ecosystem](https://github.com/Avalanche-io/c4):

- **[c4](https://github.com/Avalanche-io/c4)** — Go CLI for identification and content storage (`c4 id`, `c4 cat`, `c4 diff`)
- **[c4sh](https://github.com/Avalanche-io/c4sh)** — Shell integration that makes c4m files behave as directories
- **[c4git](https://github.com/Avalanche-io/c4git)** — Git clean/smudge filter for large media assets

All tools share the same content store and produce identical C4 IDs.

## Compatibility

c4py produces byte-identical output to the [Go reference implementation](https://github.com/Avalanche-io/c4) for the same tree, except that `scan()` skips names starting with `.` (files and folders), which `c4 id` includes, so a tree holding one has a different C4 ID. Cross-language test vectors check the rest.

Zero external dependencies. Pure Python. Works offline.

## Design Decisions

See the [FAQ](https://github.com/Avalanche-io/c4/blob/release/1.0.x/docs/faq.md) for design decisions including SHA-512 permanence, the c4m format, and content store scaling.

## License

Apache 2.0
