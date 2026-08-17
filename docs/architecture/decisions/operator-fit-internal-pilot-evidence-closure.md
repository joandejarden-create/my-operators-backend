# ADR — Operator Fit Internal Pilot Evidence Closure

**Status:** Accepted  
**Date:** 2026-08-04  
**Phase:** Evidence closure — no owner pilot enablement

---

## 1.1 Scoring methodology frozen

Do **not** change core Operator Alignment factors, weights, evidence ceilings, readiness threshold, generic-capability treatment, missing-data treatment, or Market Presence eligibility rules unless a **reproducible** internal-pilot defect shows a materially wrong result.

Advisor preference alone is not grounds to retune.

Defect path: document → reproduce → show affected deals/operators → recommend change → **founder approval before implement**.

## 1.2 Brand relationship versus project approval

**Brand Relationship** = evidence the operator currently/recently operates or has a documented relationship with a brand/parent (conservative evidence classes).

**Project Approval** = confirmation the operator is approved/acceptable for **this** hotel opportunity.

Project Approval must **not** be inferred from portfolio evidence. It may remain `To Be Confirmed` during Fit. Lack of project-specific approval must **not** auto-disqualify an otherwise credible operator before outreach unless a known hard operator restriction exists — emit a validation requirement instead.

## 1.3 Brand relationship treatment

- Verified current relationships → may contribute positive compatibility evidence  
- Historical → lower confidence  
- Announced → qualified  
- Unknown → no positive points  
- Known incompatibility → may affect eligibility/compatibility  

`Verified Current Relationship` ≠ `Approved for This Project`.

## 1.4 Internal shortlist persistence

Approve Airtable table **`Operator Fit - Shortlist`** for internal pilot. File store remains fallback/test fixture. Do **not** use ODR. Do **not** expose shortlist to normal owners yet.

## 1.5 Human advisor testing

Owner-pilot recommendation must include **≥5 completed live advisor scorecards** (not templates alone).

## 1.6 Owner terminology

Do not finalize owner-facing copy from engineering terms alone. Test and recommend language for Alignment, Evidence Confidence, Data Coverage, Ranking Ready, Research Stage, Eligible With Conditions, Validation Required, Additional Candidate Requiring Research. Internal concepts may stay more technical than owner copy.

## Non-goals

Owner pilot enablement · My Deals wiring · auto ODR · pathway matrix · OAS deprecation · owner-controlled weights · scoring retune without founder approval.
