#!/usr/bin/env python3
"""Verify a delivery matches its manifest.

A vendor sends you 2 TB of rendered frames and a c4m file. Before you
start compositing, you want to know: is everything here, and is any
of it corrupt?

    python verify_delivery.py delivery.c4m /mnt/vendor_delivery/

This replaces the manual process of running md5sum on every file and
comparing against a checksum list — except C4 IDs are computed from
SHA-512 (stronger than MD5), and the c4m file also captures permissions,
timestamps, and directory structure.
"""

import sys
import c4py


def main():
    if len(sys.argv) != 3:
        print(f"Usage: {sys.argv[0]} <manifest.c4m> <directory>")
        sys.exit(1)

    c4m_path = sys.argv[1]
    directory = sys.argv[2]

    print(f"Verifying {directory} against {c4m_path}...")

    report = c4py.verify_tree(
        c4m_path, directory,
        progress=lambda path, i, total: print(f"\r  checking {path}...", end="", flush=True),
    )
    print()  # clear the progress line

    if report.corrupt:
        print(f"\n{len(report.corrupt)} CORRUPT:")
        for c in report.corrupt:
            print(f"  {c.path}")
            print(f"    expected: {c.expected}")
            print(f"    actual:   {c.actual}")

    if report.missing:
        print(f"\n{len(report.missing)} MISSING:")
        for path in report.missing:
            print(f"  {path}")

    if report.extra:
        print(f"\n{len(report.extra)} EXTRA (not in manifest):")
        for path in report.extra:
            print(f"  {path}")

    ok = len(report.ok)
    total = ok + len(report.corrupt) + len(report.missing)
    print(f"\n{ok}/{total} files OK", end="")
    if report.extra:
        print(f", {len(report.extra)} extra", end="")
    print()

    sys.exit(0 if not report.corrupt and not report.missing else 1)


if __name__ == "__main__":
    main()
