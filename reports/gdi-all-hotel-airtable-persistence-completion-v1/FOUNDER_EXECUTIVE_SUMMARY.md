# Founder executive summary — all-hotel Airtable research persistence completion V1

**STOP FOR FOUNDER REVIEW — NO BULK PRODUCTION WRITES · NO SCHEMA CREATES**

## Verdict

The complete local research inventory across **19** active GDI hotels (**659** opportunities) is inventoried and classified into Research Inventory vs Qualified GDO vs Customer Publication.

**Durable Airtable storage for research-only opportunities requires a minimal new table** (`GDI Hotel Research Opportunities`). Forcing those rows into GDO would violate commercial qualification. Misusing Research Targets/Runs is rejected.

Until schema approval, FS `opportunities.json` remains the operational research SoT; this package provides exact dry-run manifests for every hotel.

## Aggregate counts

| Metric | Count |
|--------|-------|
| Hotels with research inventory | 19 |
| Local research opportunities | 659 |
| Live Airtable GDO rows (sum) | 674 |
| Research → pending schema persist | 659 |
| Qualified GDO CREATE candidates | 0 |
| Qualified GDO already present | 509 |
| Conflicts quarantined | 2 |
| Airtable-only GDOs | 163 |
| YOTEL legacy recreate blocks | 0 |

## Classification key

- **Already persisted (GDO):** unchanged qualified overlaps
- **Eligible to persist (GDO):** create candidates meeting commercial eligibility
- **Needs schema change:** research-only / watchlist inventory → proposed table
- **Deferred:** identity conflicts (e.g. org-name mismatches)
- **Research-only:** not GDO-eligible
- **Customer-published:** CURRENT packages only (unchanged by this work)

## YOTEL

Legacy 108 not recreated. Local 42 remain research baseline for inventory table after approval; only independently qualified subset may enter GDO.

## Git

Primary `main` @ `2fd417f5c` synchronized. Isolation worktree on focused completion branch. ADP untouched. Large Changes count = untracked research artifacts, not disposable source edits — WIP preserved.

## Next founder actions

1. Approve/reject proposed research inventory table schema
2. Authorize bounded migration batches from PRODUCTION_BATCH_MIGRATION_PLAN.md
3. Optionally authorize Rock Creek one-field Venue ID fix on separate allowlist
