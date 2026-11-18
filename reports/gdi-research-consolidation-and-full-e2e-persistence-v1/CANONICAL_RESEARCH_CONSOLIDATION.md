# Canonical research consolidation

## Policy applied

- No blind cherry-pick of `72df16986`.
- No competing research stores created.
- No publication package / CURRENT pointer changes.
- No GDO mutation.
- Historical research-state / segment-state retained on feature tip + isolation.

## Local canonical FS finding

`data/.../recLuxvwwxID7U2B8/opportunities.json` had **skip-worktree** set and contained **86** opportunities vs HEAD **59** (+27).

Preservation steps (non-destructive):

1. Copied the 86-opportunity file to isolation preserve path:
   `wt-gdi-isolation-post-365/data/group-demand-intelligence/_wip-preserve-20261118/recLuxvwwxID7U2B8-opportunities.86.json`
2. Restored primary path to HEAD (59) so CI/backfill gates stay deterministic.
3. Catalogued the +27 creates in `NEW_OR_CHANGED_RESEARCH_MANIFEST.json` for founder Airtable authorization.

Primary working tree is clean for this path again; unique +27 remain recoverable from isolation preserve + manifest.

## Quarantine

| Artifact | Action |
|----------|--------|
| 748 run reports | Off-main evidence; keep on tip/isolation |
| 42 segment-state files | Preserve off-main until archive policy |
| 27 opp research-state files | Preserve off-main; not opportunity inventory rows |
| pursuits FOLLOW_UP_DUE delta | Quarantine — hotel-private; do not auto-merge |
| +27 Bethesda opportunities (WT) | Manifested for founder Airtable auth; FS left dirty/local |

## Recoverability

- Commit `72df16986` retains full tree.
- Isolation index retains 821 staged paths.
- Bethesda +27 remain on disk in canonical path.

