# Phase 1 closeout — founder decision report (PR #364)

**STOP — do not merge/deploy without founder approval.**

Branch: `cursor/adp-hotel-explorer-audit-1ed2`  
Worktree: `C:\Dev\dealality\my-operators-backend-adp-audit`  
Primary GDI / port 8080: untouched

## Decision answers

| # | Question | Answer |
|---|----------|--------|
| 1 | Safe to merge into main? | **Conditionally yes for HE code**, after resolving expected `package.json` (and possibly ranked-list) merge with newer GDI scripts on `origin/main`. Not a force-merge. Customer-surface production claim remains **blocked** by pre-existing ADP failures (not HE regressions). |
| 2 | Preserve GDI behavior? | **Yes** — no GDI checkout edits; isolated `test:gdi-airtable-purpose-integrity-audit-v1` PASS. Main has 5 newer GDI commits (#366–#370) not in this branch — integrate carefully. |
| 3 | Canonical app route validated? | **Embed path PASS**; full `/app#/opportunity-radar` shell = **BLOCKED_AUTH** (Memberstack sign-in gate). Screenshot: `screenshots/phase1-closeout-app-signin-gate.png`. |
| 4 | Bethesda auth safe? | **Yes for production** (`NODE_ENV=production` disables DEV_AUTH_BYPASS). Local tighten: bypass admin must send `X-Dealality-Owner-App: 1` or internal-preview returns **403**. Public brand-presence: Bethesda **absent** (15692). Query alone insufficient. No Bethesda share pack. |
| 5 | Pre-existing customer-surface issues? | **33 failures** remain (territory rates, BPP, evidence links, displacement parity) — Phase 1.1 register: not new HE regressions; still block customer-surface production claims. |
| 6 | Phase 1.3B preserved? | **Yes** — untracked under `data/hotel-identity/`, `lib/hotel-identity/`, `phase-1.3a/`, `phase-1.3b/`, related scripts. Not in PR #364. |

## Validation summary

| Check | Result |
|-------|--------|
| `test:adp-phase1-radar-he-gates` | PASS |
| Permanence Layer A | PASS |
| Layer B | BLOCKED (pre-existing durable evidence) |
| GDI purpose integrity (isolated) | PASS |
| Customer surface contract | FAIL 33 (pre-existing) |
| Public Bethesda absent | PASS |
| Internal-preview no header | 403 |
| Internal-preview + owner-app header | 200 + Bethesda |
| Embed HE open Bethesda | PASS (screenshot) |
| `/app` signed-in click-path | BLOCKED_AUTH |

## Integration vs `origin/main`

- This branch tip was `dfc93df33` + local HE commits.  
- `origin/main` is **5 commits ahead** (GDI Airtable / research table work).  
- Likely touchpoints: **`package.json`** (changed in both). Ranked-list HE wiring is branch-only vs main.

## PR scope

Hotel Explorer + progressive enrichment + 1.1 reliability already on PR + Phase 1.2/1.3 Bethesda bridge + closeout docs.  
**Out:** census identity ledger / shadow / migration (1.3a/1.3b).
