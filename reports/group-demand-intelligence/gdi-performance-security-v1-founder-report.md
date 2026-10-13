# GDI PERFORMANCE + SECURITY V1 — FOUNDER REPORT

As of: 2026-09-23  
Branch: `deploy/adp-final-trust-closure-20260910`  
Code SHA: _(commit pending)_  
Deploy ID: `2dd6d643` (perf cache) · peek follow-up `aa7ed57f`

---

## A. ROOT CAUSE

INITIAL LOAD PRIMARY BOTTLENECK:  
**Airtable full-hotel opportunity list (~5s P50) on every open**, plus sequential share `resolve → opportunities`.

VIEW DETAILS PRIMARY BOTTLENECK:  
**Detail endpoint re-listed the entire hotel from Airtable (~5s)** with no click acknowledgement — dead UI until response.

SECONDARY BOTTLENECKS:
- Commercial progression sequential N+1 (decision/event per subject)
- Serial resolve→list bootstrap
- No client detail shell / skeleton

---

## B. PERFORMANCE BEFORE / AFTER

### INITIAL LOAD (opportunities API as proxy for useful content)

| Metric | Before | After (warm) |
| --- | ---: | ---: |
| P50 | 5147ms | **336ms** |
| P95 | 5306ms | **392ms** |

Cold first hit still ~5–9s (Airtable). Warm/reuse within 45s TTL is fast.

### VIEW DETAILS (detail API)

| Metric | Before | After (warm) |
| --- | ---: | ---: |
| P50 | 4881ms | **171ms** (`X-GDI-Detail-Source: cache`) |
| P95 | 5159ms | **178ms** |

---

## C. REQUEST EFFICIENCY

| | Before | After |
| --- | --- | --- |
| Initial request count | 2 serial (resolve+list) | 2 **parallel** when token peek works |
| View Details requests | 1 (full reload) | 1 (cache/single-row preferred) |
| Airtable calls (warm detail after list) | 1 full list again | **0** (memory cache) |
| Transfer size list | ~62KB | ~62KB (unchanged; not primary) |
| Duplicate full-hotel reloads | Yes on every detail | **Eliminated within TTL** |

---

## D. USER EXPERIENCE

CLICK ACKNOWLEDGEMENT: **<100ms** (drawer shell + “Loading details…”)  
DETAIL VISIBLE (warm API): **~171ms P50 / ~178ms P95**  
FIRST USEFUL GDI CONTENT (warm list): **~336ms P50**  
Cold first open: still multi-second (Airtable) — **watch**

TARGET MET (warm View Details + click ack): **YES**  
TARGET MET (cold initial ≤3.5s): **NO** — Airtable cold path remains

---

## E. CONTEXT7

DOCUMENTATION AREAS REVIEWED: Express (resolved), Airtable Web API (resolved); live query snippets intermittently failed  
CHANGES SUPPORTED: parallel I/O, private short-TTL cache (not shared HTTP cache), single-record over full scan, route timing logs

---

## F. STRIX

Strix CLI targets missing — used bounded production probe instead.

CRITICAL: **0** FAIL  
HIGH: **0** FAIL  
MEDIUM fixed: **2** (CSV injection, URL scheme)  
OPEN: cold Airtable latency (performance watch, not security)

FIXED: 2 · OPEN: 0 security fails

---

## G. SECURITY CONTROLS

| Control | Result |
| --- | --- |
| AUTH BYPASS | PASS (probe) |
| IDOR | PASS |
| CROSS-HOTEL ACCESS | PASS |
| SHARE TOKEN TAMPERING | PASS |
| SHARE EXPIRY | PASS (existing durability suite) |
| WRITE AUTHORIZATION | PASS (existing lifecycle tests) |
| XSS | PASS (esc + safe URL) |
| CSV INJECTION | PASS (fixed) |
| URL SAFETY | PASS (fixed) |
| SSRF | PASS (no read-path URL fetch) |
| SECRET LEAKAGE | PASS (no tokens in perf logs) |

---

## H. DATA INTEGRITY

OPPORTUNITY IDS / SHARE TOKENS / VALIDATIONS / ACTIONS / OUTCOMES: **0 / 0 / 0 / 0 / 0**

---

## I. EXISTING SHARES

Bethesda / Waterstone / Renaissance / NOW NOW / Cambridge: **PASS** (page/resolve/opportunities 200; tokens unchanged)

---

## J. CODE / TESTS

FILES CHANGED:
- `lib/group-demand-intelligence/read-cache.js` (new)
- `lib/group-demand-intelligence/opportunity-persistence.js`
- `lib/decision-outcomes/gdi-commercial-progression.js`
- `lib/group-demand-intelligence/live-commercial-quality-v1.js`
- `api/group-demand-intelligence.js`
- `public/js/group-demand-intelligence/{share-app,app,dealality-gdi-ui}.js`
- reports + `scripts/test-gdi-performance-security-v1.mjs` + `scripts/gdi-security-v1-probe.mjs`

NEW TESTS: 6 unit gates + 7 live security probes  
PASS: all run · FAIL: 0  
COMMIT SHA: _(filled on commit)_  
DEPLOY ID: `2dd6d643` / `aa7ed57f`

HOTEL/CITY/EVENT/PERSON-SPECIFIC: **NO / NO / NO / NO**

---

## K. DECISION

1. Why initial load slow? **Airtable full-hotel list (~5s) + serial resolve→list**  
2. Why View Details slow? **Same full-hotel reload with dead UI**  
3. Highest-impact fix? **45s hotel-scoped read cache + immediate detail shell**  
4. View Details perceptibly immediate? **Yes on warm (shell + ~170ms)**  
5. Initial load improved? **Dramatically when warm; cold Airtable still slow**  
6. External deps still dominate cold? **Yes — Airtable**  
7. Airtable scaling concern? **Yes for cold opens / multi-hotel concurrency — watch**  
8. Caching boundaries safe? **Yes — hotel-keyed, TTL, invalidated on writes**  
9. Strix material vulns? **No confirmed critical/high; 2 medium fixed**  
10. Auth + share authorization sound? **Yes**  
11. Perf weaken security? **No**  
12. Ready for broader weekly usage? **Yes with cold-load watch**

---

## L. FINAL VERDICT

**PASSES WITH WATCH ITEMS — SAFE FOR CONTROLLED SCALE**

Watch: cold Airtable first-open latency (~5s). Warm browse/detail and share security are in good shape.
