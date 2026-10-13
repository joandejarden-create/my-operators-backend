# Bethesda GDI Weekly Refresh — Founder Report

**RUN ID:** `gdi_weekly_bethesda_20260921`  
**RUN DATE:** 2026-09-21  
**STACK:** Discovery Hygiene V3 + V11/V12 (WHO deferred this week — 0 NEW TRUE)  
**PRE-COMMIT BASE:** `7312754ea8823bb4f5c28491f0096805030a0ffd`

---

## A. RUN

| Field | Value |
|---|---|
| HOTEL | Bethesda Marriott |
| hotelId | `recLuxvwwxID7U2B8` |
| RUN ID | `gdi_weekly_bethesda_20260921` |
| RUN DATE | 2026-09-21 |
| PRIOR CANONICAL OPPORTUNITIES | **38** |
| CURRENT RAW CANDIDATES | **2** |
| CURRENT TRUE_ACTIONABLE | **0** |

---

## B. WEEKLY DELTA

| State | Count |
|---|---:|
| NEW | **0** |
| UPDATED | **0** |
| REACTIVATED | **0** |
| UNCHANGED | **38** |
| CLOSED/DOWNGRADED | **0** |

---

## C. NEW THIS WEEK

*(none — no V3 TRUE_ACTIONABLE unmatched cycles)*

---

## D. UPDATED THIS WEEK

*(none)*

---

## E. REACTIVATED

*(none)*

---

## F. TOP SALES PRIORITIES

No new weekly TRUE leads. Sales should continue on existing HIGH/MEDIUM opportunities already in the Bethesda bag (unchanged this week). Next weekly run should increase discovery density (`--max-queries` / `--allow-parallel`) if denser NEW volume is required.

---

## G. UI

| Item | Result |
|---|---|
| NEW PILL | PASS (wired; no rows this week) |
| UPDATED PILL | PASS |
| REACTIVATED PILL | PASS |
| OPTIONAL ICON (dot) | PASS |
| NEW THIS WEEK FILTER | PASS |
| UPDATED FILTER | PASS |
| REACTIVATED FILTER | PASS |
| HEADER NEW COUNT | PASS (hides at zero) |
| AUTH GDI UI | PASS |
| BETHESDA SHARE UI | PASS |

---

## H. DATA INTEGRITY

| Check | Count |
|---|---:|
| DUPLICATE OPPORTUNITIES CREATED | **0** |
| FIRST-SEEN RESET | **0** |
| VALIDATIONS LOST | **0** |
| ACTIONS LOST | **0** |
| OUTCOMES LOST | **0** |
| DECISION LINKS LOST | **0** |

---

## I. WHO / PROVIDERS

| Metric | Value |
|---|---|
| NEW WHO | 0 |
| SURFE CALLS | 0 |
| SURFE NEW EMAIL | 0 |
| SURFE NEW PHONE | 0 |
| PDL CALLS | 0 |
| UNCHANGED-OPP PROVIDER CALLS | **0** |

WHO intentionally deferred: zero NEW TRUE_ACTIONABLE to research.

---

## J. PERSISTENCE / GENERALIZATION

**CODE FILES:**

- `lib/group-demand-intelligence/weekly-delta.js`
- `lib/group-demand-intelligence/discovery-hygiene-v2.js` (Bethesda DMV geo contract)
- `lib/group-demand-intelligence/discovery-recall-v1.js` (Bethesda recall profile)
- `scripts/gdi-weekly-refresh.mjs`
- `scripts/test-gdi-weekly-delta.mjs`
- `public/js/group-demand-intelligence/dealality-gdi-ui.js`
- `public/js/group-demand-intelligence/app.js`
- `public/js/group-demand-intelligence/share-app.js`
- `public/css/group-demand-intelligence.css`
- `package.json`

**DELTA TESTS:** 15/15 PASS  
**UI TESTS:** covered in weekly-delta suite (pill/filter/header/share+auth wiring)  

| Hardcode audit | |
|---|---|
| HOTEL-SPECIFIC LOGIC | NO |
| EVENT-SPECIFIC LOGIC | NO |
| PERSON-SPECIFIC LOGIC | NO |
| DOMAIN-SPECIFIC RULE | NO |

Bethesda appears only in hotel config / geo-recall onboarding data.

---

## K. DECISION

1. Did Bethesda produce genuinely NEW opportunities this week? **NO** (0 NEW TRUE)  
2. Were NEW vs UPDATED vs REACTIVATED classified reliably? **YES** (tests + run)  
3. Did canonical matching prevent duplicate leads? **YES** (0 dupes)  
4. Are the new opportunities commercially actionable? **N/A** (none new)  
5. Is the NEW pill visually clear? **YES** (design wired)  
6. Does the New This Week filter work correctly? **YES**  
7. Can a hotel user immediately understand what changed since last week? **YES** (header summary + filters; this week = no change banner at zero)  
8. Were Bethesda's existing validations/actions/outcomes preserved? **YES**  
9. Was provider spend limited to genuinely new/materially changed contacts? **YES** (0 calls)  
10. Is the weekly-delta implementation reusable for every GDI hotel? **YES**  
11. Is Bethesda's weekly update safe for founder live-promote review? **YES** (staged annotations only; no Airtable promote)

---

## L. FINAL VERDICT

**BETHESDA WEEKLY REFRESH PASSES WITH WATCH ITEMS**

Watch items:

1. Native discovery yield this week was thin (2 candidates / 0 TRUE) — increase query budget or enable parallel on the next weekly cycle.  
2. WHO not run (correct given 0 NEW TRUE).  
3. Do **not** live-promote as a “new leads” week — promote the **weekly-delta capability** + staged annotations for founder review.

Do not start Wave 3. Do not run another hotel.
