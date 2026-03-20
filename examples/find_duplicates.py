#!/usr/bin/env python3
"""Find duplicate files across a project.

Scan a directory and find files with identical content, regardless of
their name or location. Useful for cleaning up render farm output where
the same frame might exist in multiple directories.

    python find_duplicates.py /projects/HERO/

Two files with the same C4 ID are byte-for-byte identical — no false
positives, ever. SHA-512 makes collisions physically impossible.
"""

import sys
import c4py


def main():
    if len(sys.argv) != 2:
        print(f"Usage: {sys.argv[0]} <directory>")
        sys.exit(1)

    path = sys.argv[1]
    print(f"Scanning {path}...")

    manifest = c4py.scan(path, progress=lambda p, i, _: print(f"\r  {i} files scanned", end="", flush=True))
    print()

    dupes = manifest.duplicates()
    if not dupes:
        print("No duplicates found.")
        return

    total_waste = 0
    for c4id, paths in sorted(dupes.items(), key=lambda x: len(x[1]), reverse=True):
        entry = next(e for _, e in manifest.files() if e.c4id == c4id)
        waste = entry.size * (len(paths) - 1)
        total_waste += waste
        print(f"\n{len(paths)} copies ({entry.human_size()} each):")
        for p in paths:
            print(f"  {p}")

    from c4py.entry import _human_size
    print(f"\n{len(dupes)} sets of duplicates, {_human_size(total_waste)} wasted")


if __name__ == "__main__":
    main()
