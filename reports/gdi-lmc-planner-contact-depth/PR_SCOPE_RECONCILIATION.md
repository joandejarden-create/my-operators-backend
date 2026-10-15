# PR #99 scope reconciliation → replacement PR

**Date:** 2026-10-16  
**Lineage:** GitHub PR #99 (`gdi/lmc-planner-contact-depth`, tip `289852e6`) was **not** mergeable onto current `origin/main` (`01e3f11c`). This replacement branch `gdi/lmc-planner-contact-depth-scoped` carries only the contact-depth delivery delta.

## Why not merge #99 as-is

| Signal | PR #99 (reported) | Reality vs current main |
|--------|-------------------|-------------------------|
| Files / churn | ~103 files, ~50k+/1.2k lines, 8 commits | Tip is an older release stack: PE persist, multi-segment planner, scaling, priority-action, Railway deploy flags, plus contact-depth |
| Concurrent main | — | Qualification integrity (#101–#103, `b571d945`) + AI Admin More menu (#104–#105, `8148eed9` / `01e3f11c`) already on main |
| Two-dot hazard | — | Tip **deletes** main modules (e.g. `hotel-fit-narrative-consistency-v1`, `opportunity-change-ledger-v1`, `paid-multisegment-research-v1`, PE adapter) and would overwrite qualification-hardened `dealality-gdi-ui.js` / DTO / contact-display |

Merging #99 wholesale would undo concurrent fixes and ship unrelated research dumps.

## File-manifest classification (PR #99 tip vs main)

### Required shared contact-depth code (taken into scoped PR)

- `lib/.../buyer-contact-enrichment-contract-v1.js` — `NOT_ATTEMPTED`, `EXHAUSTED_ORG_FALLBACK`, `CONTACT_DEPTH_OUTCOME`, org-route must not suppress named research
- `lib/.../buyer-contact-enrichment-v1.js` — shared pass, Surfe opt-in, freshness/reuse, exhausted fallback
- `lib/.../weekly-refresh-orchestrator-v1.js` — weekly executes same public contact-depth path; Surfe still gated
- `scripts/gdi-research-to-page-workflow-v1.mjs` — contact-depth executes by default; `--skip-buyer-enrichment-execute` debug only; `--authorize-surfe` still required for paid
- `scripts/gdi-weekly-refresh-coordinator-v1.mjs` — weekly entrypoint
- `scripts/gdi-lmc-planner-contact-depth-v1.mjs` — batch/readback helper (no new paid runs in this release task)
- Proper Case (surgical): `opportunity-list-dto.js` `toProperCaseDisplayTitle`, UI + CSS `text-transform: none` on `.brand-card__meta` only

### Necessary tests

- `scripts/test-gdi-buyer-contact-enrichment-v1.mjs` (updated)
- `scripts/test-gdi-contact-depth-stage-wiring-v1.mjs`
- `scripts/test-gdi-tile-header-proper-case.mjs`

### Necessary documentation (kept)

- `reports/gdi-lmc-planner-contact-depth/FOUNDER_REPORT.md`
- `account-contact-depth-register.json`, `before-after-contact-register.json`, `canonical-readback-proof.json`
- `scorecard/*`
- This reconciliation + post-deploy closeout artifacts

### Already on main (not re-merged from #99)

- AI Admin portal table + More-menu portal layering (`public/js/admin-ai-demand-table.js`, CSS, Playwright proof)
- Use-case qualification / publication integrity / filter-before-sanitize
- Compact tile contacts + static hotel-fit score tiles
- Stronger `contact-display-surface-v1.js` firstname/org mapping (kept from main; tip would regress it)

### Unrelated / excluded from scoped PR

- PE canonical persist packages, venue partnership apply reports
- Priority Action Window parity tree
- Hotel-relevance backfill for other hotels
- Segment research plan CSVs for this batch (planner expansion, not contact-depth code)
- Scaling-companies / multi-segment planner feature commits on the #99 tip history
- Railway `--service` deploy fix commit (already superseded by supported deploy workflow on main)

### Raw research / customer snapshots / diagnostic dumps (excluded)

- `reports/gdi-lmc-planner-contact-depth/results/gdi_lmc_*.json` (~20–27 KB each provider/research blobs)
- Any credentials, share tokens, or full Surfe response bodies

Local evidence under other worktrees/`gdi-release-main` is **preserved** (not deleted).

## Concurrent fix preservation check

After scoped merge onto main, confirm present:

1. AI Admin More menu portals out of sticky stacking (`admin-ai-demand-table.js`).
2. Qualification integrity eligibility + filter-before-sanitize + visibility-order tests.
3. Contact-depth contract symbols + coordinator/weekly execute path.
4. Proper Case tile headers without global org-name lowercasing (acronyms preserved).

## Paid / Surfe

This release task authorizes **no** new Surfe spend and **no** unattended publication. Remaining ~$4.50 of the prior $11 envelope is unchanged by this merge/deploy/verify work.
