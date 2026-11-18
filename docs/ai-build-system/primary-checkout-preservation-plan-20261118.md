# Primary checkout preservation plan (2026-11-18)

**Read-only classification.** No reset/stash/clean/overwrite performed.  
**Primary path:** `C:\Dev\dealality\my-operators-backend`  
**Primary branch:** `feat/gdi-bethesda-exa-priority8-live-benchmark-v1` @ `c3cd2ec66` (pre-#380 tip)  
**New main baseline:** `ed0d91aa8` (PR #380 merged)  
**Dirty path count:** 39

## Legend

| Disposition | Meaning |
|---|---|
| `PRESERVE_COMMIT_GDI` | Unique GDI source/report — commit onto a GDI task branch before syncing primary |
| `PRESERVE_BACKUP_THEN_TAKE_MAIN` | Differs from main; back up working tree copy, then accept main (usually #380) |
| `CLEAR_SAFE_IDENTICAL` | Working tree already matches main — safe to `git checkout --` / discard dirty bit after verify |
| `PRESERVE_UNTRACKED_GDI` | New GDI scripts/reports — add+commit on GDI branch |
| `SUPERSEDED_IDENTICAL_UNTRACKED` | Untracked but byte-identical to main — keep until verified, then delete untracked copy |
| `PRESERVE_TMP_EVIDENCE` | Merge/trial artifacts — copy to external backup if still needed, else retain in primary until review |
| `NESTED_ARTIFACT` | Nested worktree/dir artifact — do not delete without inventory |

## Exact path plan

| Status | Path | Bucket | vs main | Disposition |
|---|---|---|---|---|
| M | `lib/group-demand-intelligence/workflow/gdi-canonical-full-e2e-entry-v1.js` | GDI source | +8/−3 unique | **PRESERVE_COMMIT_GDI** |
| M | `lib/group-demand-intelligence/workflow/original-full-e2e-research-chain-v1.js` | GDI source | +23/−0 unique | **PRESERVE_COMMIT_GDI** |
| M | `lib/hotel-intelligence/radar-internal-preview-hotels-v1.js` | HE/Radar | working tree **missing** 36 lines present on main (#380) | **PRESERVE_BACKUP_THEN_TAKE_MAIN** (do not keep stale WIP over #380 without diff review) |
| M | `package.json` | Shared | mixed (GDI script entries vs #380 HE scripts) | **PRESERVE_COMMIT_GDI** for GDI script keys after three-way review with main |
| M | `public/brand-presence-mapping.js` | HE/Radar | identical to main | **CLEAR_SAFE_IDENTICAL** |
| M | `public/deal-capture-radar-with-ranked-list.html` | HE/Radar | identical to main | **CLEAR_SAFE_IDENTICAL** |
| M | `reports/gdi-bethesda-automated-research-persistence-pilot-v1/DEDUPLICATION_AND_SHARED_INTELLIGENCE.md` | GDI reports | identical to main | **CLEAR_SAFE_IDENTICAL** |
| M | `reports/gdi-bethesda-automated-research-persistence-pilot-v1/FAILURE_RECOVERY_TESTS.md` | GDI reports | identical to main | **CLEAR_SAFE_IDENTICAL** |
| M | `reports/gdi-bethesda-automated-research-persistence-pilot-v1/FOUNDER_EXECUTIVE_SUMMARY.md` | GDI reports | identical to main | **CLEAR_SAFE_IDENTICAL** |
| M | `reports/gdi-bethesda-automated-research-persistence-pilot-v1/FULL_E2E_EXECUTION_TRACE.md` | GDI reports | identical to main | **CLEAR_SAFE_IDENTICAL** |
| M | `reports/gdi-bethesda-automated-research-persistence-pilot-v1/PILOT_RESULT.json` | GDI reports | +1/−1 | **PRESERVE_COMMIT_GDI** |
| M | `reports/gdi-bethesda-automated-research-persistence-pilot-v1/PRODUCTION_PILOT_AUTHORIZATION.md` | GDI reports | identical to main | **CLEAR_SAFE_IDENTICAL** |
| M | `reports/gdi-bethesda-automated-research-persistence-pilot-v1/RESEARCH_PERSISTENCE_RESULTS.md` | GDI reports | identical to main | **CLEAR_SAFE_IDENTICAL** |
| M | `reports/gdi-bethesda-automated-research-persistence-pilot-v1/TEST_RESULTS.md` | GDI reports | identical to main | **CLEAR_SAFE_IDENTICAL** |
| M | `reports/gdi-bethesda-automated-research-persistence-pilot-v1/WEEKLY_CHANGE_VALIDATION.md` | GDI reports | identical to main | **CLEAR_SAFE_IDENTICAL** |
| M | `reports/gdi-bethesda-research-consolidation-and-persistence-activation-v1/EXACT_27_RECORD_PRODUCTION_MANIFEST.json` | GDI reports | +1/−1 | **PRESERVE_COMMIT_GDI** |
| M | `reports/gdi-research-consolidation-and-full-e2e-persistence-v1/BRIDGE_TEST_SNAPSHOT.json` | GDI reports | +2/−0 | **PRESERVE_COMMIT_GDI** (also in prior external backup set) |
| M | `scripts/test-radar-internal-preview-hotels-v1.mjs` | HE/Radar | working tree missing 13 lines on main | **PRESERVE_BACKUP_THEN_TAKE_MAIN** |
| ?? | `my-operators-backend-ava-v4-reconcile/` | Nested artifact | not on main | **NESTED_ARTIFACT** — inventory before any delete |
| ?? | `reports/gdi-association-events-runs/` | GDI reports | untracked | **PRESERVE_UNTRACKED_GDI** |
| ?? | `reports/gdi-bethesda-exa-priority8-live-benchmark-v1/` | GDI reports | untracked | **PRESERVE_UNTRACKED_GDI** |
| ?? | `reports/gdi-scaling-companies-hotel-runner-v1/` | GDI reports | untracked | **PRESERVE_UNTRACKED_GDI** |
| ?? | `scripts/gdi-bethesda-exa-priority8-live-benchmark-v1.mjs` | GDI source | untracked | **PRESERVE_UNTRACKED_GDI** |
| ?? | `scripts/test-gdi-bethesda-exa-priority8-live-benchmark-v1.mjs` | GDI source | untracked | **PRESERVE_UNTRACKED_GDI** |
| ?? | `scripts/test-radar-hpc-cluster-inclusion-v1.mjs` | HE/Radar | byte-identical to main (fc) | **SUPERSEDED_IDENTICAL_UNTRACKED** |
| ?? | `tmp/consolidation-worktree-inventory.csv` | tmp | untracked | **PRESERVE_TMP_EVIDENCE** |
| ?? | `tmp/gitleaks-bin/` | tmp | tool cache | **PRESERVE_TMP_EVIDENCE** (or reinstall; also in `C:\Backups\Dealality\pr380-recovery-20261118`) |
| ?? | `tmp/local-8080-restart.log.err` | tmp | log | **PRESERVE_TMP_EVIDENCE** |
| ?? | `tmp/merge1.err` | tmp | merge evidence | **PRESERVE_TMP_EVIDENCE** |
| ?? | `tmp/merge2.err` | tmp | merge evidence | **PRESERVE_TMP_EVIDENCE** |
| ?? | `tmp/merge3.err` | tmp | merge evidence | **PRESERVE_TMP_EVIDENCE** |
| ?? | `tmp/radar-3way-ours.js` | HE trial | untracked | **PRESERVE_TMP_EVIDENCE** |
| ?? | `tmp/radar-3way-result.js` | HE trial | untracked | **PRESERVE_TMP_EVIDENCE** |
| ?? | `tmp/radar-after-both.js` | HE trial | untracked | **PRESERVE_TMP_EVIDENCE** |
| ?? | `tmp/radar-after-primary.js` | HE trial | untracked | **PRESERVE_TMP_EVIDENCE** |
| ?? | `tmp/radar-base.js` | HE trial | untracked | **PRESERVE_TMP_EVIDENCE** |
| ?? | `tmp/radar-he-dirty.js` | HE trial | untracked | **PRESERVE_TMP_EVIDENCE** |
| ?? | `tmp/radar-merge-trial2.js` | HE trial | untracked | **PRESERVE_TMP_EVIDENCE** |
| ?? | `tmp/radar-primary-dirty.js` | HE trial | untracked | **PRESERVE_TMP_EVIDENCE** |

## Recommended execution order (future founder-approved phase — NOT this PR)

1. External backup of entire primary dirty set (timestamped, outside worktrees) — verify hashes.
2. Commit **PRESERVE_COMMIT_GDI** + **PRESERVE_UNTRACKED_GDI** onto `feat/gdi-bethesda-exa-priority8-live-benchmark-v1` (or fresh GDI branch from main + cherry-pick/port).
3. Backup then restore **PRESERVE_BACKUP_THEN_TAKE_MAIN** files from `origin/main`.
4. Clear **CLEAR_SAFE_IDENTICAL** / **SUPERSEDED_IDENTICAL_UNTRACKED** only after hash verify.
5. Relocate **PRESERVE_TMP_EVIDENCE** / **NESTED_ARTIFACT** per founder.
6. Only then: switch primary to `main` and ff-only to `origin/main`.

## ADP

No ADP product source paths appear in the 39 dirty entries. ADP evidence remains in other worktrees/history; do not treat as absent globally.

## Blockers for primary → main sync

Until steps 1–5 complete, primary **must not** be branch-switched or hard-synced. Preflight will correctly block merges that overlap dirty HE files with upstream main.
