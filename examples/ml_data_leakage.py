#!/usr/bin/env python3
"""Detect data leakage between train and test splits.

If the same image appears in both your training and test sets, your
evaluation metrics are lying to you. This is a well-known problem
(see: "Do ImageNet Classifiers Generalize to ImageNet?") and it's
shockingly common in custom datasets.

C4 IDs make exact-match detection trivial — if two files have the
same C4 ID, they're byte-for-byte identical.

    python ml_data_leakage.py ~/datasets/my_model/train/ ~/datasets/my_model/test/

This catches:
- Identical files with different names (renamed duplicates)
- Same image in different directory structures
- Copies that were accidentally included in both splits
"""

import sys

import c4py


def main():
    if len(sys.argv) != 3:
        print(f"Usage: {sys.argv[0]} <train_dir> <test_dir>")
        sys.exit(1)

    train_dir, test_dir = sys.argv[1], sys.argv[2]

    print(f"Scanning training set: {train_dir}")
    train = c4py.scan(
        train_dir,
        progress=lambda p, i, _: print(f"\r  {i} files", end="", flush=True),
    )
    print()

    print(f"Scanning test set: {test_dir}")
    test = c4py.scan(
        test_dir,
        progress=lambda p, i, _: print(f"\r  {i} files", end="", flush=True),
    )
    print()

    # Build C4 ID → path maps
    train_ids = {}
    for path, entry in train.files():
        if entry.c4id:
            train_ids.setdefault(entry.c4id, []).append(path)

    test_ids = {}
    for path, entry in test.files():
        if entry.c4id:
            test_ids.setdefault(entry.c4id, []).append(path)

    # Find overlap
    leaked = set(train_ids.keys()) & set(test_ids.keys())

    print(f"Training set: {train.file_count()} files, {len(train_ids)} unique")
    print(f"Test set:     {test.file_count()} files, {len(test_ids)} unique")

    if not leaked:
        print("\nNo data leakage detected. Train and test sets are disjoint.")
        sys.exit(0)

    print(f"\nDATA LEAKAGE: {len(leaked)} identical files in both splits\n")

    for c4id in sorted(leaked, key=lambda x: str(x)):
        train_paths = train_ids[c4id]
        test_paths = test_ids[c4id]
        print(f"  {c4id}")
        for p in train_paths:
            print(f"    train: {p}")
        for p in test_paths:
            print(f"    test:  {p}")
        print()

    pct = len(leaked) / len(test_ids) * 100
    print(f"{len(leaked)} leaked files = {pct:.1f}% of test set by unique content")
    sys.exit(1)


if __name__ == "__main__":
    main()
