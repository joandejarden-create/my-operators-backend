# Operator Intelligence — Airtable Architecture Recommendation

**Status:** Recommendation only — **do not apply**  
**Date:** 2026-08-03

---

## Recommended architecture

**Hybrid:** keep Operator Setup Master + child tables for published profile fields; add claim/evidence spine for research auditability.

| Object | Role | MVP for calibration persistence | Before scaling research | Future project-specific |
| ------ | ---- | ------------------------------- | ----------------------- | ----------------------- |
| Existing Operator Setup fields | Published destination for Class 1/2 facts | Populate Active Countries, Structures, experience | Continue | — |
| Operator Claims (new) | Structured claims with scopes | Optional local JSON first | **Required** | — |
| Evidence Sources (extend PI Source Library) | Source records reusable across claims | Link PI sources | **Required** | — |
| Operator Comparables (extend Case Studies) | Structured comps | Extend Case Studies | Normalize | — |
| Brand–Operator Relationships | Approval / BM / geography | Extend existing child | Required for branded ranking | Project-specific approval = Class 3 |
| Market Presence (new or Platform multi) | Presence types | Populate Platform first | Dedicated rows if multi-type per country | — |
| Project Responses | Outreach economics/capacity | — | — | **Required later** |

---

## Required capabilities

One claim ↔ many sources · one source ↔ many claims · historical · conflicts · verification · publication status · refresh dates · geographic/brand/property scope · comps · evidence reuse · auditability · auto-publish · exception review.

---

## Separation

1. **Calibration persistence (now):** local `data/operator-intelligence/calibration-cohort/**` — sufficient until founder approves Airtable write wave.  
2. **Scaling research:** Claims + Sources (+ Comparables + Brand Relationships + Market Presence).  
3. **Project-specific:** fees, capacity, BM availability for a deal, competitive conflicts — Class 3 outreach tables; **not** ODR-as-shortlist.
