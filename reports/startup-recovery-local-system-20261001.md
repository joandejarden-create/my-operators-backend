# Local System Startup Recovery Report

**Branch:** `cursor/local-system-startup-recovery`  
**Date:** 2026-10-01  
**Repo:** `C:\Dev\deal-capture-proxy`  
**Prior branch base:** `deploy/gdi-pe-v1-7-customer-closure`

## Verdict

**READY FOR CHATGPT QA — LOCAL SYSTEM STARTUP RESTORED**

`npm start` succeeds; process stays listening on `:8080`. `/health` returns `{"ok":true}`. Brand Explorer, Market Alerts list, Contact Intelligence meta, and Hotel Intelligence research templates all respond without import crashes. **Zero production Airtable writes** performed by this recovery (census map snapshot rebuild used existing read/build path already invoked at boot).

---

## Root cause (systemic)

Many local library modules required by `server.js` were **never committed** (untracked) and were **missing from the working tree** after a clean/upload sync. Imports still pointed at the canonical paths. This is the same failure class as the earlier Market Alerts `identify-stakeholders` miss — not obsolete routes.

Classification for `contact-target.js`: **C. Accidental omission / never committed** (present again as restored untracked file; contents recovered from agent transcript + Cursor local history).

---

## Startup blockers found and fixed

| # | Missing / broken path | Root cause | Recovery |
|---|----------------------|------------|----------|
| 1 | `lib/hotel-intelligence/contact-intelligence/contact-target.js` (+ sibling CI modules) | Never committed; omitted from tree | Restored from transcripts / Cursor History |
| 2 | `lib/market-alerts-contact/*` + `market-alerts-consumer-fluff.js` + `lib/surfe/client.js` | Same | Transcript + History |
| 3 | `lib/hotel-intelligence/research/native-loop/*` | Same | History path-fragment restore |
| 4 | `lib/hotel-intelligence/ownership/*` | Same | History |
| 5 | `lib/context-dev/*`, `lib/pdl/*`, `lib/inegi-denue/*`, `lib/fullenrich/*` | Same | History / transcripts |
| 6 | `lib/hotel-intelligence/research/providers/parallel/*` | Same | Transcripts |
| 7 | `lib/hotel-intelligence/research/native-langchain/**` | Same | Transcripts |
| 8 | `lib/hotel-census/census-map-snapshot.js`, `map-hotel-dto.js`, `adp-gdi-canonical-identity.js` | Same | Transcripts |
| 9 | `api/decision-outcomes.js` import `../hotel-census/...` | **Stale/wrong relative path** | Corrected to `../lib/hotel-census/adp-gdi-canonical-identity.js` |
| 10 | `ownership-research-strategy/source-tier.js` export name skew (`classifySourceTier` vs `classifySourceTiers`) | Mixed restored versions | Added alias export |

Static ESM import-graph audit after recovery: **`missingCount: 0`** (`scripts/audit-startup-import-graph.mjs server.js`).

---

## Files restored / changed (startup-critical)

### Corrected import (code change)
- `api/decision-outcomes.js` — fix ADP canonical identity dynamic import path
- `lib/hotel-intelligence/contact-intelligence/ownership-research-strategy/source-tier.js` — alias `classifySourceTiers`

### Restored packages (representative)
- `lib/market-alerts-contact/**`
- `lib/market-alerts-consumer-fluff.js`
- `lib/market-alerts-hotel-development-announce.js`
- `lib/surfe/client.js`
- `lib/hotel-intelligence/contact-intelligence/**` (including `contact-target.js`, waterfall, ownership research, etc.)
- `lib/hotel-intelligence/ownership/**`
- `lib/hotel-intelligence/research/native-loop/**`
- `lib/hotel-intelligence/research/providers/parallel/**`
- `lib/hotel-intelligence/research/native-langchain/**`
- `lib/context-dev/**`, `lib/pdl/**`, `lib/inegi-denue/**`, `lib/fullenrich/**`
- `lib/hotel-census/census-map-snapshot.js`, `map-hotel-dto.js`, `adp-gdi-canonical-identity.js`
- Recovery tooling: `scripts/audit-startup-import-graph.mjs`, `scripts/loop-audit-restore-startup.mjs`, `scripts/_restore-*.mjs`, `scripts/restore-market-alerts-contact-from-transcript.mjs`

### Not committed (out of scope / unrelated dirty tree)
- GDI HTML/data/token registry edits, rail-explore submodule noise, ADP evidence JSON, etc.

---

## Runtime verification

| Check | Result |
|-------|--------|
| `npm start` | **PASS** — `Server running at http://localhost:8080` |
| Process stays up | **PASS** — Listen on 8080 after census boot |
| `GET /health` | **PASS** — HTTP 200 `{"ok":true}` |
| `GET /` | **PASS** — HTTP 200 service JSON |
| Brand Explorer API `GET /api/brand-explorer/brands?limit=1` | **PASS** — HTTP 200 |
| `brand-explorer.html` | **PASS** — HTTP 200 |
| Market Alerts `GET /api/market-alerts?limit=1` | **PASS** — HTTP 200 (0 items in 7d window — data/filter warning only) |
| Market Alerts contacts route | **PASS** — HTTP 403 auth (`user_not_found`), **not** module miss |
| Contact Intelligence `GET /api/contact-intelligence/meta` | **PASS** — HTTP 200 |
| Hotel Intelligence `GET /api/hotel-intelligence/research/templates` | **PASS** — HTTP 200 |
| Census map snapshot boot | Non-blocking after first build — `boot build ok records=15692` |

### Non-startup warnings (acceptable)
- `[deal-readiness-field-tabs] Missing tab mapping for required field: Primary Market Region`
- Market Alerts empty 7d list / Published At filter messaging
- ADP published read source = filesystem (expected local default)

---

## Tests

| Test | Result |
|------|--------|
| `npm run test:market-alerts-v1_3` | **PASS** |
| `npm run test:contact-intelligence-v1` | **PASS** |
| `npm run test:brand-explorer-route-state` | **PASS** (after restoring `public/js/brand-explorer-route-state.js`) |
| `npm run test:market-alerts-contact-intelligence-v1` | **PARTIAL** — most asserts OK; fails on named-direct enrich / batch `providerBudget` (orchestrator StrReplace history incomplete). **Does not block boot.** |
| Import graph audit | **PASS** — 0 missing |

---

## Production safety

- Surfe live enrichment: **0** (not invoked)
- Production Airtable **writes**: **0** intentional recovery writes
- No deploy / publish / merge to production

---

## Recommended follow-ups (non-blocking)

1. Commit remaining omitted libs that are still untracked so the next clean checkout does not regress.
2. Finish Market Alerts contact orchestrator StrReplace gaps so `test:market-alerts-contact-intelligence-v1` goes fully green.
3. Add a CI gate: `node scripts/audit-startup-import-graph.mjs server.js` fails on `missingCount > 0`.
