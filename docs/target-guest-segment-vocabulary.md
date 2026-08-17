# Target Guest Segment — shared vocabulary

> **Authority for Brand Basics + Deal Strategic Intent guest-segment selects.**  
> Also mirrored in `reports/brand-vs-deal-select-options-cleanup-checklist.md` §10.

**Last updated:** 2026-07-24

## Fields

| Side | Table | Field | Type |
|------|--------|--------|------|
| Brand | `Brand Setup - Brand Basics` | **Target Guest Segments** | multipleSelects |
| Deal | `Strategic Intent - Operational - Key Challenges` | **Target Guest Segment** | multipleSelects |

Brand nuance (not a select): **Guest Psychographics Description**.  
Deal legacy text (optional, not on form): **Target Guest Segment Other Text**.

## KEEP (identical strings; both multi-select)

1. Corporate / Business  
2. Leisure  
3. Bleisure  
4. Family  
5. Solo Traveler  
6. Wellness Seeker  
7. Group / MICE  
8. Contract / Extended Stay  
9. Government / Military  
10. International Inbound  
11. Staycation / Local  
12. Digital Nomad  
13. Luxury / Discerning  
14. Experience-Oriented  

Code: `scripts/lib/target-guest-segment-vocabulary.mjs`  
UX: Brand Setup + Deal Setup / New Deal Setup use the same 14 options as multi-select (no `Other`).

## Remap (before deleting Meta options)

| Old | New |
|-----|-----|
| Business | Corporate / Business |
| Group / Events | Group / MICE |
| Bleisure (Business + Leisure) | Bleisure |
| Family Leisure | Family |
| Convention / Meetings | Group / MICE |
| Tour Groups | Group / MICE |
| Free-text Deal Meta blobs | drop / map into KEEP (or legacy Other Text) |

## Rules

- Do not invent select options outside KEEP.  
- Prefer Meta ensure scripts / typecast-safe writes that only use KEEP.  
- After cleanup, avoid `typecast: true` on these fields so free-text cannot re-pollute Deal Meta.  
- Meta field PATCH is blocked on this base — KEEP was seeded via typecast; obsolete options need **manual UI delete**: `reports/target-guest-segment-manual-meta-prune.md`.  
- Deal Meta may still list unused `Other` — safe to delete so Brand and Deal Meta match exactly.
