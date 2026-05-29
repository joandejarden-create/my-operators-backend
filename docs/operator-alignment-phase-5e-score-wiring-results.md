# Operator Alignment Phase 5E — Score Wiring Results

**Date:** 2026-05-25  
**Sample deal:** `recIeGRZP21udmTnt` (Aeropuerto Cancún Select-Service Hotel)  
**Audit report:** `reports/operator-alignment-scoring-phase5e-recIeGRZP21udmTnt.json`  
**Baseline (post–5B backfill, pre–5E wiring):** `reports/operator-alignment-scoring-after-backfill-recIeGRZP21udmTnt.json`

---

## Summary

Phase 5E wires `scoreOperatorMatchForDeal` to structured deal fields (SI + Deals) with legacy fallback only when structured inputs are absent. **Scoring weights unchanged.** BAS, OCS, Airtable schema, and PDF layout unchanged.

Franchise **brand agreement** and **third-party managed** **operating model** are scored on separate dimensions — no longer treated as a single “Franchise Only vs management” conflict.

---

## Score distribution — before vs after

| Metric | Before 5E (legacy fields) | After 5E (structured wiring) |
|--------|---------------------------|------------------------------|
| Average score | **64.1** | **71.8** |
| Min / Max | 54.0 / 69.4 | 65.6 / 75.6 |
| Strong (80+) | 0 | 0 |
| Moderate (65–79) | 5 | **10** |
| Conditional (50–64) | 5 | 0 |

**Interpretation:** Scores rose because structure and geography factors now read populated deal fields. Scores did **not** reach Strong (80+) because **service offerings** still show limited overlap with operator `Offered Services` (operator-side enrichment still needed — Phase 5C). This is intentional, not weight inflation.

---

## Factors changed

| Factor | Before | After | Field source |
|--------|--------|-------|----------------|
| dealStructureAssignment | **20** (all) | **72** (typical) | `structured` — Preferred Management Structure + Operating Model; legacy MP `Franchise Only` ignored when SI structured |
| serviceOfferings | **30** (typical) | **~44** | `structured` — Must-Have Operator Services; legacy free-text must-haves not primary |
| geographyMarkets | 100 | **88** | `structured` — Market Presence Requirement + Active Countries/Markets when on operator |
| systemsReporting | 70–90 | **90** (typical) | `structured` — Owner Reporting Expectations vs Owner Reporting Level |
| chainScale | 100 | 100 | unchanged |
| feeCommercial | 75 | 75 | unchanged |
| assetProjectStageFit | ~59.5 | ~60 | Pre-opening + Opening Timeline wired |
| brandPortfolioRelevance | scored | often **excluded** | null when operator brands empty (not weak 25) |

---

## Franchise + third-party managed — conflict resolved?

| Input | Value | Used for scoring |
|-------|-------|------------------|
| Brand Agreement Structure | Franchise | Annotated in structure rationale; **not** compared as “Franchise Only” penalty |
| Operating Model | Third-party managed | Drives management-structure match targets |
| Preferred Management Structure | Franchise with third-party operator; Full third-party management | Primary structure overlap |
| MP Preferred Deal Structure | Franchise Only (legacy) | **Fallback only** when structured fields absent |

---

## Missing-data behavior (5E)

| Situation | Behavior |
|-----------|----------|
| Optional deal field absent | Factor excluded from weighted denominator (`score: null`) |
| Material deal requirement, operator field absent | `missingDataClass: needs_validation`, factor excluded; rationale on card |
| Legacy-only deal | Falls back to MP/SI legacy with `fieldSource: legacy_mp` / `legacy_si` |

Examples: empty operator `managementStructuresSupported` → structure factor excluded with validation message (not score 20). Empty operator services with structured deal must-haves → service factor excluded (not score 30).

---

## Narrative differentiation

| Change | Location |
|--------|----------|
| Factor-specific `rationale` on breakdown | `lib/operator-alignment-scoring-factors.js` |
| Signals prefer `rationale` over generic templates | `lib/operator-alignment-company-utils.js` |
| Removed unconditional default bullets when ≥2 signals exist | `public/js/operator-alignment-snapshot.js` |

Cards should show more varied bullets (e.g. Mexico presence, third-party path, pre-opening). Full de-templating remains **Phase 5F** for `humanizeCompanyAlignmentSignal`.

---

## New code paths

| File | Role |
|------|------|
| `lib/operator-alignment-deal-normalize.js` | `normalizeOperatorAlignmentDealInputs` |
| `lib/operator-alignment-scoring-factors.js` | Structure, service, geography, stage, reporting factor scorers |
| `api/my-deals.js` | `scoreOperatorMatchForDeal` consumes normalized deal + factor helpers |
| `scripts/audit-operator-alignment-scoring.mjs` | Field source + missing-data audit output |
| `scripts/validate-operator-alignment-phase-5e.mjs` | Regression checks |

---

## What still requires operator-side enrichment (5C)

- `Offered Services` aligned to deal Must-Have Operator Services (raises service factor above ~44)
- `Active Countries` / `Active Markets` on all demo operators (geography can reach 100 with city match)
- `Management Structures Supported` explicit multis
- `Pre-Opening Support Capability` / `New-Build Opening Experience`

---

## Airtable Option Validation (2026-05-25)

Live schema export: `reports/operator-alignment-live-airtable-options.json` (+ CSV).

| Result | Detail |
|--------|--------|
| Phase 5B structured fields | **38 / 38 Exact** match vs planned options in code |
| Legacy fields | `Must-Haves From Brand/Operator`, `Services Required From Operator`, `Preferred Deal Structure` — Extra legacy options (expected); scoring uses structured fields first |
| chainScalesSupported | Synced planned list to live (`Independent` not `Independent / Boutique`) |
| Post-validation scoring audit | `reports/operator-alignment-scoring-phase5e-options-validated-recIeGRZP21udmTnt.json` — same range 65.6–75.6; structured sources intact |

See `docs/operator-alignment-airtable-options-audit.md` for full field table and backfill safety rules.

**Follow-up:** Phase 5C operator backfill raised Cancún sample scores to ~74–86 avg 80.7 — see `docs/operator-alignment-phase-5c-operator-backfill-results.md`.

---

## Confirmations

- No `OPERATOR_MATCH_WEIGHTS` changes  
- No Brand Alignment Snapshot changes  
- No Operator Capability Snapshot changes  
- No Airtable schema changes in this phase  
- No PDF layout changes  
- No advisory “recommend operator” language added  

---

## Commands

```bash
node scripts/validate-operator-alignment-phase-5e.mjs
node scripts/audit-operator-alignment-scoring.mjs recIeGRZP21udmTnt
```
