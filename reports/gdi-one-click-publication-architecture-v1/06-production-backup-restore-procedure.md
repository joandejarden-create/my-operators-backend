# Production GDI registry / Spice snapshot backup + restore

**STOP:** Do not run production volume writes until founder GO.
This documents the durable procedure and a **local restore drill** that proves the method.

## What to back up (production)

Root = `GDI_PUBLISHED_INVENTORY_ROOT` on the Railway volume (or the GDI published-inventory subtree).

Required paths:

```
registry/current-registry-v1.json
snapshots/recKRJjcPnb4tVDDS/
history/recKRJjcPnb4tVDDS/
```

Optional but recommended:

```
registry/  (entire)
history/   (entire)
```

Do **not** modify ADP trees under the shared volume parent.

Destination (example):

```
reports/gdi-one-click-publication-architecture-v1/backup-volume-BEFORE_SPICE_ONE_CLICK_<UTC>/
```

Include a manifest JSON: `backup-manifest.json` with UTC time, deploy revision, hotel IDs, file counts, sha256 of `current-registry-v1.json`.

## Restore

1. Stop accepting publish traffic (or hold founder GO).
2. Copy `current-registry-v1.json` from backup over the live registry file (atomic replace).
3. Restore `snapshots/recKRJjcPnb4tVDDS/**` and `history/recKRJjcPnb4tVDDS/**` from backup.
4. Confirm share token files untouched.
5. Confirm Bethesda / Waterstone opportunity counts unchanged.
6. Confirm Spice share serves prior content.

## Local restore drill (executed this session — not production)

See `06b-local-backup-restore-drill.json`.
