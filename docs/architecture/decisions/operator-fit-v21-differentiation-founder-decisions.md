# ADR — Operator Fit v2.1 Targeted Differentiation Founder Decisions

**Status:** Accepted  
**Date:** 2026-08-09  
**Scope:** Internal/shadow methodology only (`OPERATOR_FIT_DIFFERENTIATION_V21=0` default)

---

## 1.1 Preserve top-level weights

Geography 22 · Segment 14 · Asset/Development 20 · Project Complexity 12 · Brand Experience 10 · Ownership/Governance 10 · Regional Resources 6 · Commercial Differentiator 6. **No change.**

## 1.2 Preserve 70 / 15 / 15

Operator–Project 70% · Operating Structure 15% · Brand–Operator Compatibility 15%. **No change.**

## 1.3 Unknown is not execution risk

`unknown_validation` (and equivalent missing-information states) must **not** create a numerical execution-risk penalty in v2.1. Unknown still contributes zero in applicable factors, stays in the denominator, lowers coverage, may constrain Evidence Strength, and creates Validate Next.

Only **confirmed_risk** (verified adverse evidence) may create a full numerical risk penalty. Potential concerns remain non-numerical in v2.1.

## 1.4 Tie materiality

Owner-facing: Displayed Alignment difference **&lt; 1.0** ⇒ not meaningfully separated. No owner ordinal #1/#2 / winner claim. Internal `candidateId` sort may remain for stability only.

## 1.5 Candidate-set tiers

Ranking Ready ≠ Leading Candidate.

- **Leading Candidates** — production Ranking Ready, not Research Stage, no hard eligibility failure, Eligible/Preferred (not merely conditional), Good or Strong Alignment  
- **Potential Fits — Validation Needed** — Potential Alignment and/or Eligible With Conditions with meaningful alignment  
- **Additional Candidates Requiring Validation** — Limited Alignment and/or insufficient readiness  
- **Under Evaluation** — Research Stage only  

Do not force five operators in any tier.

## 1.6 Performance Evidence remains separate

No RevPAR / GOP / EBITDA / flow-through / ramp-up / forecast accuracy / owner returns in Operator Alignment v2.1.

## Versioning

- v2 remains reproducible (default evaluation path).  
- v2.1 is versioned (`operator-fit-v2.1.0-diff`) behind `OPERATOR_FIT_DIFFERENTIATION_V21`.  
- Shortlist history is not rewritten.  
- Owner pilot remains disabled; My Deals unwired.
