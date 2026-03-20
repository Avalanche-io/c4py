#!/usr/bin/env python3
"""Create a portable bundle for sneakernet transfer.

When you can't rsync — the destination is air-gapped, on a different
network, or you just want to hand someone a USB drive — create a
self-contained bundle with everything they need.

    # Create the bundle
    python portable_bundle.py pack /projects/HERO/deliveries/v03/ bundle/

    # On the receiving end (they don't need c4 or c4py installed)
    cd bundle/
    sh extract.sh

    # Or if they have c4py:
    python portable_bundle.py unpack bundle/ /projects/received/
"""

import sys
import tempfile
from pathlib import Path

import c4py


def pack(source_dir, output_dir):
    store = c4py.open_store()
    print(f"Scanning {source_dir}...")

    # Scan and store content
    manifest = c4py.scan(
        source_dir, store=store,
        progress=lambda p, i, _: print(f"\r  {i} files", end="", flush=True),
    )
    print()

    # Save manifest to a temp location, then let pool() copy it into the bundle
    c4m_name = Path(source_dir).name + ".c4m"
    with tempfile.TemporaryDirectory() as tmp:
        c4m_path = Path(tmp) / c4m_name
        c4py.dump(manifest, str(c4m_path))

        # Create pool bundle (copies c4m + store objects into output_dir)
        result = c4py.pool(str(c4m_path), output_dir, store=store)
    print(f"Bundle created in {output_dir}/")
    print(f"  {result.copied} objects, {result.skipped} already present")
    print(f"  {manifest.summary()}")
    print(f"\nTo extract without c4py: cd {output_dir} && sh extract.sh")


def unpack(bundle_dir, dest_dir):
    print(f"Ingesting {bundle_dir}...")
    result = c4py.ingest(bundle_dir)
    print(f"  {result.copied} objects ingested, {result.skipped} already present")

    if result.manifests:
        store = c4py.open_store()
        for c4m_file in result.manifests:
            manifest = c4py.load(c4m_file)
            print(f"\nRestoring {c4m_file} → {dest_dir}")
            r = c4py.reconcile(manifest, dest_dir, store=store)
            print(f"  {r.created} files created")


def main():
    if len(sys.argv) < 2:
        print(f"Usage:")
        print(f"  {sys.argv[0]} pack <source_dir> <bundle_dir>")
        print(f"  {sys.argv[0]} unpack <bundle_dir> <dest_dir>")
        sys.exit(1)

    cmd = sys.argv[1]
    if cmd == "pack" and len(sys.argv) == 4:
        pack(sys.argv[2], sys.argv[3])
    elif cmd == "unpack" and len(sys.argv) == 4:
        unpack(sys.argv[2], sys.argv[3])
    else:
        print(f"Usage: {sys.argv[0]} pack|unpack <args>")
        sys.exit(1)


if __name__ == "__main__":
    main()
