# HE-INTEL-001 — Ownership research objective (founder correction)

**Contract:** `HE_INTEL_001_OWNERSHIP_RESEARCH_OBJECTIVE_V1`  
**Applies to:** P3A/P3B live benchmarking + Hotel Explorer ownership presentation  
**Architecture:** Extends Research Engine V1 — no competing framework

## Primary goals

1. Actual ultimate hotel owner, investor, family, fund, REIT, or controlling ownership organization  
2. Named people who make or represent ownership decisions  
3. Verified contact information and credible professional pathways  
4. Ownership structure / legal entities connecting the hotel to actual owners  

Legal entities are **intermediary evidence**, not the final measure of success.

## Distinctions (required)

| Role | Notes |
| --- | --- |
| Legal titleholder | PropCo / SPE — partial if alone |
| Economic / beneficial owner | Prefer when evidenced |
| Controlling investor / group | Family, fund, REIT, etc. |
| Asset manager / ownership representative | Pathway when ultimate UNRESOLVED |
| Hotel management / operator | **Not an owner** |
| Brand / franchise | **Not an owner** |

## Scoring

- **FULL_ACTIONABLE** — ultimate owner + ownership decision-maker + verified contact or credible pathway + structure chain  
- **PARTIAL_LEGAL_ENTITY_ONLY** — titleholder/LLC only (not full success)  
- **PARTIAL_REPRESENTATIVE_PATHWAY** — ultimate UNRESOLVED but verified representative/pathway  
- **UNRESOLVED** — mark explicitly; never invent names, emails, phones, percentages  

Never invent beneficial owners or contact details.

## Hotel Explorer drawer

Must ultimately answer:

1. Who owns this hotel?  
2. Who should I speak with about it?  
3. How can I reach them?  
4. How do we know?  

Projection: `projectHotelExplorerContactViewModel` → `ownershipSummary.drawerAnswers`.

## Module

`lib/hotel-intelligence/research-engine-v1/ownership-research-objective.js`

```bash
npm run test:he-intel-001-ownership-research-objective-v1
```
