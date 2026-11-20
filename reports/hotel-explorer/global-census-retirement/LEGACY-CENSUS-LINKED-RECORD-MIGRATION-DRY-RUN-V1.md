# Legacy Census linked-record migration dry-run v1

Generated: 2026-11-20T00:02:40.926Z
**applyAuthorized: false** — `AIRTABLE_LINKS_MIGRATED` remains **false**.

Map source: FULL_MATRIX
Mapped pairs: **108**
Unmatched (orphan risk sample): **142**
Duplicate HPC targets: **1**

## Referential integrity checklist

- [ ] No orphaned links after apply
- [ ] No duplicated relationships
- [ ] Contact / ownership mappings remapped via explicit ids
- [ ] GDI opportunities not duplicated by hotel-id rewrite
- [ ] ADP associations use HPC ids
- [ ] Evidence provenance retained (legacy id kept as historical attribute)

See JSON for full `legacyToHpcMap` and proposed apply steps.