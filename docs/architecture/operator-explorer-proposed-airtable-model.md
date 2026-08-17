# Operator Explorer — Proposed Airtable Model

**Date:** 2026-08-09  
**Status:** Recommendation only — minimize new schema  
**No tables/fields created in this phase**

---

## Recommended structure

```text
Operator Setup - Master  (canonical operator)
        │
        ├── Operator Setup - Profile / Platform / Commercial / Governance  (published summaries)
        ├── Operator Setup - Case Studies  (selected owner stories)
        ├── Operator Setup - * presentation children  (Explorer tabs)
        │
        ├── Operator Intelligence - Market Presence  (EXISTS — geo SoT)
        ├── Operator Intelligence - Claims  (EXISTS — claim spine)
        ├── Operator Intelligence - Assignments  (PROPOSED)
        ├── Operator Intelligence - Brand Relationships  (PROPOSED typed graph)
        └── Partner Intelligence - Source Library  (EXISTS — shared)

Operator Fit - Shortlist  (EXISTS — workflow only)
Operator Deal Requests  (EXISTS — outreach only)
Company Profile / Companies  (platform/outreach — linked later if needed, not SoT)
```

---

## Existing to keep as-is (core)

| Object | Why |
| ------ | --- |
| Operator Setup - Master + scoring children | Published Explorer/Fit summaries |
| Claims | Generalized evidence/claim backbone — extend, don't replace |
| Market Presence | Typed geo — extend grain carefully |
| PI Source Library | Shared research core |
| Case Studies | Owner stories |
| Brand Relationships (presentation) | Explorer Brand tab UX |
| Shortlist / ODR | Workflow |

---

## Proposed new tables

### 1) Operator Intelligence - Assignments

| | |
| - | - |
| Why required | Case Studies cannot be inventory SoT; need derivation + Fit comps |
| Why existing insufficient | Polluted selects; missing keys/dates/brand links/currentness/evidence |
| Relationship | N assignments → 1 Master; optional link to Case Study; optional Census hotel |
| Expected volume | Tens–hundreds per researched operator over time |
| Scoring use | Geo/segment/development/comps |
| Explorer use | Selected Assignments + portfolio summaries |
| Research use | Primary structured discovery target |

### 2) Operator Intelligence - Brand Relationships (typed)

| | |
| - | - |
| Why required | Presentation rows ≠ approval/experience graph |
| Why existing insufficient | Profile.brands has no current/historical/evidence/approval scope |
| Relationship | Operator × Brand (± parent); evidence links |
| Expected volume | Low tens per operator |
| Scoring use | Brand depth / eligibility for branded deals |
| Explorer use | Brand Relationships section (facts feeding presentation) |
| Research use | Prevent approval over-inference |

---

## Proposed additive fields (minimum)

| Field | On | Why |
| ----- | -- | --- |
| Record Purpose (Production / Research / Test Fixture) | Master | Dummy isolation without status spaghetti |
| Candidate Type (cleaned vocab) | Master or Profile | Distinguish Third-Party vs Brand-Managed vs Integrated — **design only; do not create yet** |
| Claims → PI Source Library link | Claims | Stronger than URL-only |
| Optional Presence City / Property Count | Market Presence | Footprint depth |
| Optional Assignment link on Case Study | Case Studies | Story ↔ inventory |

Normalize Claims free-text enums to selects when additive options are approved.

### Brand-Managed capability (on proposed typed Brand Relationships)

| Field concept | Why |
| ------------- | --- |
| Relationship Type incl. `Brand Managed Capability` | Scoped BM offering without implying project approval |
| Brand scope + Geography scope + Segment scope | Prevents global “brand = manager” assumption |
| Offered to third-party owners? | Yes / Selective / No / Unknown |
| Management entity alias | e.g. MxM, Hilton Management Services — same Operator Master |

Do **not** create a separate Management Offerings table in Phase 1 unless typed Brand Relationships proves insufficient.

---

## Explicitly not proposed now

- Project-specific fees/capacity tables  
- Merging Company Profile into Master  
- Storing Fit scores on Master  
- Third Active Countries representation  
- Brand Explorer image/FDD tables for operators  
