# Operator Fit — Validation Lifecycle

**Date:** 2026-08-04  
**Purpose:** Connect Operator Fit ranking to diligence and outreach without treating Fit as a final selection.

---

## Lifecycle phases

```text
Before shortlist → Before outreach → During outreach → Before proposal comparison → Before final recommendation
```

| Phase | What must be true | Typical items |
| ----- | ----------------- | ------------- |
| **Before shortlist** | Qualifying geography / structures / readiness for the lane | Market Presence type; management structures; not fabricating approval |
| **Before outreach** | Credible reason to contact | Presence confirmation plan; comparable relevance; capacity/interest hypothesis |
| **During outreach** | Ask operator / brand | Project-specific brand approval; fees; interest; regional team; conflicts |
| **Before proposal comparison** | Commercial package complete | Fee proposal; services scope; reporting package; exclusivity |
| **Before final recommendation** | Dealality/AO can defend selection | Approval geography; residual unknowns closed or accepted; decision history intact |

## Classification used in engine

`listRankingChangeValidations()` tags each item with:

- `phase` — one of the five above  
- `criticality` — `required_before_outreach` · `required_before_final` · `useful_not_critical`  
- `direction` — could improve / reduce / unknown (no invented answers)

## Shortlist vs Target List vs ODR

| Object | Role |
| ------ | ---- |
| Target List | Brand exploration |
| **Operator Shortlist** | Curated operator decisions + frozen Fit snapshot |
| Operator Deal Request | Outreach / RFP — created later, not on shortlist |

## Rule

Operator Fit informs shortlist; **outreach is a separate deliberate step**. Never auto-create ODR from shortlist in this pilot.
