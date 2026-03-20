# c4py Examples

Real-world scripts for pipeline TDs. Each is self-contained — copy it
into your pipeline and adapt.

## Verify a delivery

Check that every file in a vendor delivery matches the manifest:

```bash
python verify_delivery.py delivery.c4m /mnt/vendor_delivery/
```

Replaces: running md5sum on every file and comparing against a checksum list.

## Find duplicate files

Scan a directory and find files with identical content, regardless of name:

```bash
python find_duplicates.py /projects/HERO/
```

Two files with the same C4 ID are byte-for-byte identical. SHA-512 makes
false positives physically impossible.

## Track changes over time

Take snapshots and see what changed between them:

```bash
python track_changes.py /projects/HERO/shots/   # first run: saves snapshot
# ... work happens ...
python track_changes.py /projects/HERO/shots/   # shows changes
```

## Compare deliveries (ShotGrid-style)

See which shots have new or changed frames between two delivery versions:

```bash
python shotgrid_delivery.py /deliveries/SHOW/v02/ /deliveries/SHOW/v03/
```

## Backup and restore

Content-addressed backup with automatic deduplication:

```bash
export C4_STORE=~/.c4/store
python backup_and_restore.py backup /projects/HERO hero.c4m
python backup_and_restore.py restore hero.c4m /projects/HERO_restored
python backup_and_restore.py verify hero.c4m /projects/HERO
```

## Monitor render farm output

Check which frames have been rendered vs. what's expected:

```bash
python render_farm_monitor.py expected.c4m /farm/output/HERO/shot_010/
```

Exit codes: 0 = complete, 1 = frames pending, 2 = corrupt frames found.
Designed to run from Deadline/Tractor/OpenCue event hooks.

## Portable bundle (sneakernet)

Bundle a delivery for USB/air-gap transfer. The bundle includes an
`extract.sh` that works without c4py or c4 installed:

```bash
python portable_bundle.py pack /projects/HERO/deliveries/v03/ bundle/
# hand someone the USB drive
python portable_bundle.py unpack bundle/ /projects/received/
# or without c4py: cd bundle && sh extract.sh
```
