#!/usr/bin/env python3
"""Verify a downloaded dataset matches its published manifest.

Datasets are large and downloads fail silently — a truncated JPEG, a
corrupt parquet shard, a missing split file. This script verifies
every byte against a published c4m manifest.

    python ml_dataset_verify.py imagenet-1k.c4m ~/datasets/imagenet-1k/

Publish a c4m alongside your dataset and anyone can verify their copy
is identical. No md5sums.txt with a flat list of hashes — the c4m
captures the full directory structure, sizes, timestamps, and
cryptographic IDs in one human-readable file.

The c4m file for a 1.2M image dataset is about 170 MB of text. The
dataset itself is 150 GB. That 170 MB file describes every byte of
the 150 GB with SHA-512 certainty.
"""

import sys
import time

import c4py


def main():
    if len(sys.argv) != 3:
        print(f"Usage: {sys.argv[0]} <dataset.c4m> <dataset_dir>")
        sys.exit(1)

    c4m_path, dataset_dir = sys.argv[1], sys.argv[2]

    manifest = c4py.load(c4m_path)
    print(f"Manifest: {manifest.summary()}")
    print(f"Verifying {dataset_dir}...\n")

    start = time.time()
    report = c4py.verify_tree(
        manifest, dataset_dir,
        progress=lambda p, i, total: print(
            f"\r  [{i}/{total}] {p[:60]:<60}", end="", flush=True
        ),
    )
    elapsed = time.time() - start
    print(f"\n\nVerified in {elapsed:.1f}s")

    if not report.corrupt and not report.missing:
        print(f"  All {len(report.ok)} files match the manifest.")
        if report.extra:
            print(f"  {len(report.extra)} extra files not in manifest (ok to ignore)")
        sys.exit(0)

    if report.corrupt:
        print(f"\n  {len(report.corrupt)} CORRUPT (re-download these):")
        for c in report.corrupt[:10]:
            print(f"    {c.path}")
        if len(report.corrupt) > 10:
            print(f"    ... and {len(report.corrupt) - 10} more")

    if report.missing:
        print(f"\n  {len(report.missing)} MISSING (download incomplete):")
        for m in report.missing[:10]:
            print(f"    {m}")
        if len(report.missing) > 10:
            print(f"    ... and {len(report.missing) - 10} more")

    sys.exit(1)


if __name__ == "__main__":
    main()
