# Operator Fit Engine — Staged Build Plan

**Date:** 2026-08-03  
**Status:** Plan only — implementation not started  
**Depends on:** audits in `docs/audits/*`, architecture in `docs/architecture/operator-fit-engine-proposed-architecture.md`

No calendar estimates — complexity only: Small / Medium / Large / Very Large.

---

## Phase 0: Audit and decisions

| | |
| -- | -- |
| **Objective** | Freeze current-state truth; founder decisions; scoring principles |
| **Dependencies** | This audit package |
| **Airtable** | None (read-only already done) |
| **Backend** | None |
| **Frontend** | None |
| **Tests** | Re-run OAS validators |
| **Risks** | Deciding to “tweak weights only” without eligibility/evidence layers |
| **Acceptance** | Exec summary signed off; decisions list resolved or parked |
| **Must not change** | Live scoring formulas, field names, intake mappings |
| **Complexity** | Small |

---

## Phase 1: Data foundation

| | |
| -- | -- |
| **Objective** | Normalize taxonomies; populate structured scoring fields; evidence + brand–operator + comparables shapes |
| **Dependencies** | Phase 0 decisions; options registries |
| **Airtable** | Backfill Active Countries/Markets/Structures/Offered Services carefully; add evidence/source fields; enrich Brand Relationships; ensure Case Study required columns; **no renames** of preserved structure values without migration doc |
| **Backend** | Read-path completeness reports; dry-run backfills only first |
| **Frontend** | None required |
| **Tests** | Completeness audit script; option validators |
| **Risks** | Inflating Offered Services with table-stakes everywhere (worsens differentiation) |
| **Acceptance** | Target completeness for eligibility fields on Active operators; conversion experience field usable; confidence metadata on Master |
| **Must not change** | Owner intake field bindings; Explorer gold baselines without explicit task |
| **Complexity** | Large |

---

## Phase 2: Eligibility and deterministic matching

| | |
| -- | -- |
| **Objective** | Hard eligibility; project-specific deterministic Operator–Project fit; unknown≠no; layer scores + tests; stop positive points for generic capabilities |
| **Dependencies** | Phase 1 minimum viable population |
| **Airtable** | Eligibility-supporting fields only |
| **Backend** | New fit module alongside (not silent replace) `scoreOperatorMatchForDeal`; config object for weights; gates; mandate-conditioned emphasis |
| **Frontend** | Breakdown consumes layer keys |
| **Tests** | Pure unit tests per layer; synthetic scenarios from audit sim; regression vs frozen OAS snapshots |
| **Risks** | Dual engines confuse UI; must feature-flag |
| **Acceptance** | Generic-claims fixture no longer auto-ranks #1 on niche deals; sparse-data cannot outscore on empty denominator alone; gates documented |
| **Must not change** | Brand Match v2 gates/weights unless separate task; OAS flag-off default until accepted |
| **Complexity** | Very Large |

---

## Phase 3: Operator results experience

| | |
| -- | -- |
| **Objective** | Top-five alignment cards; layer breakdown; why/concerns/validation |
| **Dependencies** | Phase 2 API |
| **Airtable** | None required |
| **Backend** | Pathways or ranked operator-fit DTO |
| **Frontend** | Reuse OAS/BAS cards + `match-score-breakdown-ui`; My Deals Operator Strategy upgrade; honest copy (no false “best operator”) |
| **Tests** | UI contract tests; print PDF smoke |
| **Risks** | Overclaiming confidence |
| **Acceptance** | Loading/empty/error/success states; footnote; top 5 + breakdown |
| **Must not change** | Brand Target List behavior |
| **Complexity** | Large |

---

## Phase 4: Comparison and pathways

| | |
| -- | -- |
| **Objective** | Side-by-side compare; brand×operator×structure pathways; shortlist persistence; brand-managed vs third-party |
| **Dependencies** | Phase 3; Brand Match v2; compatibility data from Phase 1 |
| **Airtable** | Operator shortlist / pathway results table |
| **Backend** | Pathway composer API |
| **Frontend** | deal-compare patterns; pathway cards |
| **Tests** | Pathway composition fixtures |
| **Risks** | Combinatorial explosion — cap brands×operators |
| **Acceptance** | Compare ≤3 operators; pathway list distinguishes layer scores |
| **Must not change** | Deal Compare brand-proposal semantics |
| **Complexity** | Very Large |

---

## Phase 5: Evidence and performance enrichment

| | |
| -- | -- |
| **Objective** | Comparable performance records; evidence confidence; source validation; outreach collection |
| **Dependencies** | PI governance; Case Studies |
| **Airtable** | Performance child fields; source links; validation status |
| **Backend** | Performance layer; confidence ordinals |
| **Frontend** | Evidence panel; outreach questionnaire |
| **Tests** | Confidence never treats marketing as verified |
| **Risks** | Thin CALA comps; overfit to sparse numbers |
| **Acceptance** | Verified vs operator-reported visually distinct |
| **Must not change** | Company Validated rules in PI governance |
| **Complexity** | Very Large |

---

## Phase 6: Learning loop

| | |
| -- | -- |
| **Objective** | Capture decisions/outcomes; compare predicted vs selected; terms; satisfaction; stabilization |
| **Dependencies** | Phases 3–5; Operator Deal Requests |
| **Airtable** | Outcome fields on requests / new Outcomes table |
| **Backend** | Analytics exports; optional weight learning **offline only** |
| **Frontend** | Internal dashboards first |
| **Tests** | Write validation on outcomes |
| **Risks** | Feedback leakage into live scores without review |
| **Acceptance** | End-to-end decision record for pilot deals |
| **Must not change** | Auto-updating production weights without founder approval |
| **Complexity** | Large |

---

## Cross-phase rules

- Dry-run all Airtable applies.  
- Central field maps only.  
- High-impact changes need rollback flags.  
- Run `npm run dealality:pr-check-suggest` on implementation PRs.  
- Do not execute implementation from this audit chat automatically.
