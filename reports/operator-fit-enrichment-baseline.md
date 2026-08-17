# Operator Fit — Enrichment Readiness Baseline

**Date:** 2026-08-03  
**Branch:** `app-shell-left-nav`  
**Commit:** `3c88c0b`  
**Mode:** Founder QA + data enrichment readiness (read-only; no production wiring)

---

## Feature-flag state

| Flag | Default | Current intent |
| ---- | ------- | -------------- |
| `OPERATOR_FIT_ENGINE_V2` | `0` (off) | **Must remain off** for owners / My Deals |
| `OPERATOR_FIT_ENGINE_V2_SHADOW` | unset / off | Optional shadow logging only |

No production enablement in this phase.

---

## Tests run

| Suite | Result |
| ----- | ------ |
| `npm run test:operator-fit-v2` | **Pass** |
| `npm run operator-fit-v2-shadow-comparison` | **Pass** — generic #1 legacy 2→v2 0; sparse>69 legacy 4→v2 0 |
| `npm run test:operator-fit-readiness` | **Pass** |
| `npm run operator-fit-data-readiness` | **Pass** (read-only live classify) |
| `npm run operator-fit-taxonomy-validation` | **Pass** — 0 taxonomy issues on empty/sparse fields |
| `npm run operator-fit-evidence-validation` | **Pass** — 24/24 operators have evidence issues |
| `npm run operator-fit-real-deal-shadow` | **Pass** — Deal A/B/C redacted reports written |
| `node scripts/validate-operator-alignment-phase-5e.mjs` | **Pass** |
| `node scripts/validate-operator-alignment-companies.mjs` | **Pass** |
| `node scripts/test-operator-alignment-snapshot-page.mjs` | **2 pre-existing FAIL** (My Deals OAS contract strings) — **not fixed** |

---

## Active operator universe

| Metric | Value |
| ------ | ----: |
| Active operators | **24** |
| Ranking Ready (urban representative) | **0** |
| Conditionally Rankable | **3** |
| Research Required | **21** |
| Out of Current Scope | **0** |

### Completeness (enrichment catalog, live)

| Bucket | Avg |
| ------ | --: |
| Eligibility coverage | 55.2% |
| Differentiation coverage | 34.7% |
| Evidence coverage | **0%** |
| Overall project-applicable coverage (eval) | ~74%* |

\*Overall coverage can look high while critical Ranking Ready fields (structured geography, structures, evidence) remain missing — Ranking Ready is gated on critical fields, not overall % alone.

### Highest-impact gaps (active universe)

| Field | Present | Missing |
| ----- | ------: | ------: |
| Structured Active Countries | ~2 | ~22 |
| Operating structures | 3 | 21 |
| Evidence sources (independent / detailed) | 0 | 24 |
| Brand approvals (verified) | 0 | 24 |
| Comparables (structured) | 0 | 24 |
| Conversion experience | 0 | 24 |

**Note:** Free-text “markets / regions” prose is **not** treated as Ranking Ready geography (founder enrichment QA).

---

## Current Top-5 behavior

- Production Top-5 pool requires **Ranking Ready** + ≥50% project coverage + no critical eligibility conflict.
- With **0 Ranking Ready** operators, production Top-5 is empty for synthetic and real-deal shadows.
- Diagnostic rankings may still list Conditionally Rankable / Research Required candidates for internal review — **do not show owner-facing Top-5**.
- Legacy OAS still returns inflated scores driven by completeness / table-stakes patterns (unchanged).

---

## Files expected to change (this phase)

**Add / update:**

- `docs/architecture/decisions/operator-fit-enrichment-founder-decisions.md`
- `docs/data/operator-fit-minimum-viable-operator-profile.md`
- `docs/data/operator-fit-airtable-enrichment-architecture.md`
- `docs/reviews/operator-fit-founder-review-package.md`
- `lib/operator-fit/readiness.js` (+ brand-managed confirmation hardening in fit layers)
- `scripts/operator-fit-*.mjs`, `scripts/test-operator-fit-readiness.mjs`
- `reports/operator-fit-*` (baseline, readiness, shadow, field impact, schema dry-run)
- `public/internal/operator-fit-data-readiness.html` (+ API under internal runbook auth)
- `package.json` script entries
- Minimal `server.js` route registration for internal page/API

**Protected (must not modify logic):**

- `scoreOperatorMatchForDeal` / OAS weight config / factor helpers
- Brand Match v2 (`api/match-score-server.js`, brand weight config)
- Owner intake bindings / deal setup field routing
- Target List schema (brand-only)
- Airtable schema (no create/rename/delete/apply)
- Operator Deal Requests as shortlist storage (forbidden)

---

## Explicit non-goals

Airtable writes, schema apply, My Deals wiring, flag default-on, shortlist persistence, pathway matrix, owner-adjustable weights, AI-invented factual claims, OAS deprecation.
