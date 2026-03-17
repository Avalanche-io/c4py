# pyc4

Pure Python implementation of [C4](https://github.com/Avalanche-io/c4) universal content identification (SMPTE ST 2114:2017).

```python
import pyc4

# Identify any file
c4id = pyc4.identify(open("movie.mov", "rb"))
print(c4id)  # c45xZeXwMSpq...

# Same content always produces the same ID
assert pyc4.identify(open("copy.mov", "rb")) == c4id

# Scan a directory into a c4m file
manifest = pyc4.scan("/projects/HERO/shots/")
pyc4.dump(manifest, open("HERO-shots.c4m", "w"))

# Compare two snapshots
old = pyc4.load("delivery-v2.c4m")
new = pyc4.load("delivery-v3.c4m")
diff = pyc4.diff(old, new)
print(f"Added: {len(diff.added)}, Modified: {len(diff.modified)}, Removed: {len(diff.removed)}")
```

## Install

```bash
pip install pyc4
```

## What is C4?

C4 IDs are universally unique, unforgeable identifiers derived from content using SHA-512. They are standardized as [SMPTE ST 2114:2017](https://ieeexplore.ieee.org/document/7971777). Same content always produces the same 90-character ID, regardless of filename, location, or time.

```python
>>> import pyc4
>>> pyc4.identify_bytes(b"hello world")
C4ID('c41yP4cqy7jmaRDzC2bmcGNZkuQb3VdftMk6YH7ynQ2Qw4zktKsyA9fk52xghNQNAdkpF9iFmFkKh2bNVG4kDWhsok')
```

## C4M Format

A **c4m file** is a human-readable text file that describes a filesystem. It captures file names, sizes, permissions, timestamps, and C4 IDs in a format you can read, edit, diff, and email.

```
-rw-r--r-- 2025-06-15T12:00:00Z      3 README.md   c45xZeXwMSpq...
drwxr-xr-x 2025-06-15T12:00:00Z      3 src/        -
  -rw-r--r-- 2025-06-15T12:00:00Z    3 main.go     c45KgBYEvEE7...
```

A 2 KB c4m file can describe an 8 TB project. Compare two c4m files to find exactly which frames changed across a delivery — in seconds, not hours.

## API

### Identification

```python
# From file-like object (streaming, constant memory)
c4id = pyc4.identify(file_obj)

# From bytes
c4id = pyc4.identify_bytes(b"data")

# Parse a C4 ID string
c4id = pyc4.parse("c45xZeXwMSpq...")

# C4 ID properties
str(c4id)       # 90-character string
bytes(c4id)     # 64-byte digest
c4id.hex()      # hex digest
```

### Tree IDs (Set Identity)

```python
# Compute a single ID for a set of IDs (order-independent)
tree_id = pyc4.tree_id([id_a, id_b, id_c])
```

### C4M Files

```python
# Parse
manifest = pyc4.load("project.c4m")
manifest = pyc4.loads(text)

# Write
pyc4.dump(manifest, file_obj)
text = pyc4.dumps(manifest)
text = pyc4.dumps(manifest, pretty=True)

# Scan directory
manifest = pyc4.scan("/path/to/dir")

# Iterate entries
for entry in manifest:
    print(entry.name, entry.size, entry.c4id)
```

### Diff

```python
diff = pyc4.diff(old_manifest, new_manifest)
diff.added      # entries only in new
diff.removed    # entries only in old
diff.modified   # same path, different content
diff.same       # identical entries
```

### Validation

```python
result = pyc4.validate(manifest)
for issue in result.errors:
    print(issue)
```

## Compatibility

pyc4 produces byte-identical output to the [Go reference implementation](https://github.com/Avalanche-io/c4). Cross-language test vectors ensure this.

Zero external dependencies. Pure Python. Works offline.

## License

MIT
