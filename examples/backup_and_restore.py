#!/usr/bin/env python3
"""Content-addressed backup and restore.

Back up a directory to a content store + c4m manifest. Restore from
the manifest at any time. Deduplication is automatic — if a file
already exists in the store (same content, same C4 ID), it's not
stored again.

    # Backup
    python backup_and_restore.py backup /projects/HERO hero.c4m

    # Restore
    python backup_and_restore.py restore hero.c4m /projects/HERO_restored

    # Verify
    python backup_and_restore.py verify hero.c4m /projects/HERO

The store location comes from C4_STORE or ~/.c4/config. Set it up:
    export C4_STORE=~/.c4/store
"""

import sys
import c4py


def backup(source_dir, c4m_path):
    store = c4py.open_store()
    print(f"Backing up {source_dir} → {c4m_path}")
    print(f"Store: {store.root}")

    manifest = c4py.scan(
        source_dir, store=store,
        progress=lambda p, i, _: print(f"\r  {i} files stored", end="", flush=True),
    )
    print()

    c4py.dump(manifest, c4m_path)
    print(f"Backup complete: {manifest.summary()}")


def restore(c4m_path, dest_dir):
    store = c4py.open_store()
    manifest = c4py.load(c4m_path)
    print(f"Restoring {c4m_path} → {dest_dir}")
    print(f"  {manifest.summary()}")

    result = c4py.reconcile(
        manifest, dest_dir, store=store,
        progress=lambda op, path, i, total: print(f"\r  {op}: {path}", end="", flush=True),
    )
    print()
    print(f"Restored: {result.created} created, {result.skipped} skipped")
    if result.errors:
        print(f"Errors: {len(result.errors)}")
        for e in result.errors:
            print(f"  {e}")


def verify(c4m_path, directory):
    manifest = c4py.load(c4m_path)
    print(f"Verifying {directory} against {c4m_path}...")

    report = c4py.verify_tree(manifest, directory)
    ok = len(report.ok)
    total = ok + len(report.corrupt) + len(report.missing)

    if report.corrupt or report.missing:
        if report.corrupt:
            print(f"  {len(report.corrupt)} CORRUPT files")
        if report.missing:
            print(f"  {len(report.missing)} MISSING files")
        print(f"  {ok}/{total} OK")
        return False
    else:
        print(f"  All {ok} files verified OK")
        return True


def main():
    if len(sys.argv) < 2:
        print(f"Usage:")
        print(f"  {sys.argv[0]} backup <dir> <manifest.c4m>")
        print(f"  {sys.argv[0]} restore <manifest.c4m> <dir>")
        print(f"  {sys.argv[0]} verify <manifest.c4m> <dir>")
        sys.exit(1)

    cmd = sys.argv[1]
    if cmd == "backup" and len(sys.argv) == 4:
        backup(sys.argv[2], sys.argv[3])
    elif cmd == "restore" and len(sys.argv) == 4:
        restore(sys.argv[2], sys.argv[3])
    elif cmd == "verify" and len(sys.argv) == 4:
        ok = verify(sys.argv[2], sys.argv[3])
        sys.exit(0 if ok else 1)
    else:
        print(f"Unknown command: {cmd}")
        sys.exit(1)


if __name__ == "__main__":
    main()
