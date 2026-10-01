# System Recovery Integrity — follow-up

**Branch:** `cursor/local-system-startup-recovery`  
**Date:** 2026-10-01

## Remaining root cause (test failures)

`lib/market-alerts-contact/orchestrator.js` was restored from an **incomplete Write+partial StrReplace** history:

1. Missing `preferDirectEnrich` / named-direct path in `planContactEnrichment` (mixed older Write).
2. Incomplete batch planner that **ignored caller `budget`** and always re-planned with `createProviderRunBudget()` from env defaults — so cap tests never saw `CONTACT_PROVIDER_RUN_CAP_REACHED` / `providerBudget`.

Recovered canonical orchestrator body from transcript Write (preferDirect + budget), then fixed batch wrapper to honor `budget`.

## Test failures fixed

| Assert | Status |
|--------|--------|
| named prefers direct enrich | PASS |
| named skips search | PASS |
| batch stops on provider cap | PASS |
| total calls within cap | PASS |

`npm run test:market-alerts-contact-intelligence-v1` → **PASS**

## Files changed (this follow-up)

- `lib/market-alerts-contact/orchestrator.js` — recovered + budget honor fix

## Git-tracked runtime modules

All critical boot packages previously restored are **TRACKED_OK** on this branch (from commit `d272564` + this follow-up commit). No critical untracked runtime modules under those trees.

## Temporary recovery scripts recommendation

| Script | Classification |
|--------|----------------|
| `scripts/audit-startup-import-graph.mjs` | **Keep permanent** — CI/preflight gate |
| `scripts/loop-audit-restore-startup.mjs` | **Keep permanent** (ops recovery) |
| `scripts/restore-market-alerts-contact-from-transcript.mjs` | **Keep permanent** (ops recovery) |
| `scripts/_restore-path-fragment-from-history.mjs` | **Keep permanent** (ops recovery) |
| `scripts/_restore-named-from-transcripts.mjs` | **Keep permanent** (ops recovery) |
| `scripts/_restore-basenames-from-history.mjs` | **Keep permanent** (ops recovery) |
| `scripts/_bulk-restore-contact-intelligence.mjs` | **Keep permanent** (ops recovery) |
| `scripts/_scan-transcripts-*.mjs`, `_find-*`, `_debug-*`, `_inspect-*`, `_list-*`, `_probe-*`, `_recover-*`, `_rebuild-*`, `_extract-*`, `_loop-restore-server.mjs`, `_restore-missing-*`, `_restore-paths-*`, `_restore-surfe-*`, `_restore-server-deps.mjs` | **Temporary artifacts** — safe to delete later; do not delete in this pass |

## Verification

- npm start: PASS  
- /health: PASS  
- Market Alerts / Brand Explorer / Hotel Intelligence / Contact Intelligence routes: PASS  
- Surfe / production writes: 0  

## Status

**READY FOR CHATGPT QA — SYSTEM RECOVERY INTEGRITY PASS**
