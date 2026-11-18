# Founder executive summary — GDI research consolidation & Full E2E persistence

## Verdict

Unmerged WIP classified. Full E2E is wired to the existing research-inventory persistence adapter (dry-run / fail-closed). **No production Airtable writes. No GDO writes. No publication changes. No ADP changes.**

## Exact current counts

| Metric | Count |
|--------|------:|
| Unmerged commit files (`72df16986`) | 822 |
| Isolation staged files | 821 |
| Commit ≡ isolation content | 821 |
| Commit-only (unrelated nested WT gitlink) | 1 |
| Baseline Airtable research (`tblbkDj0vhQLfrssV`) | **659** |
| Clean HEAD filesystem research | **659** |
| Local Bethesda WIP on disk (skip-worktree cleared) | **+27 → 686 FS** |
| New Airtable creates prepared (not applied) | **27** |
| WIP run-report artifacts | 748 |
| Hotel execution/research-state files | 69 |
| Pursuits delta (quarantined) | 1 |

## What the 822 files are (not 822 opportunities)

Almost entirely Full E2E execution evidence for Radisson / Bethesda / YOTEL (+ one pursuits audit event) and association-events run reports. Isolation matches the commit byte-for-byte for 821 paths.

## Persistence integration

Canonical Full E2E entry now optionally calls `gdi-full-e2e-research-persistence-bridge-v1` → `upsertResearchInventoryRecord`.

Production unlock requires **all** of: `--authorize-research-persistence`, `--exact-write-set-authorization`, `--apply-research-persistence`, and `GDI_RESEARCH_INVENTORY_PRODUCTION_WRITES=1`. Paid-research flags never unlock writes.

## Stop for founder

1. Authorize (or reject) the **27 Bethesda** research creates in `NEW_OR_CHANGED_RESEARCH_MANIFEST.json`.
2. Separately authorize Rock Creek Venue ID one-field fix.
3. Separately activate automatic production research persistence.
4. Review quarantined pursuits `FOLLOW_UP_DUE` delta before any main merge of that file.
5. Bethesda +27 preserved at `wt-gdi-isolation-post-365/data/group-demand-intelligence/_wip-preserve-20261118/recLuxvwwxID7U2B8-opportunities.86.json` (primary restored to HEAD 59).
6. Keep isolation WT + `72df16986` until evidence archive decision.


