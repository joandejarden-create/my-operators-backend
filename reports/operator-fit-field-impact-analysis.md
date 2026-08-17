# Operator Fit — Field Impact Analysis

**Date:** 2026-08-03  
**Source:** Live Active operators (n=24) via `npm run operator-fit-data-readiness`  
**Mode:** Read-only assessment  
**Project types tested:** urban branded, leisure/resort, conversion/select-service, mixed-use (synthetic + 3 redacted deals)

---

## Impact table

| Field | Missing Operators | Eligibility Impact | Alignment Impact | Confidence Impact | Differentiation Value | Sourcing Difficulty | Priority |
| ----- | ----------------: | ------------------ | ---------------- | ----------------- | --------------------- | ------------------- | -------- |
| Structured Active Countries | ~22 | Yes | Yes | Indirect | High | Medium | **Critical** |
| Operating structures supported | 21 | Yes | Yes | Indirect | High | Medium | **Critical** |
| Evidence source for material claims | 24 | No | Indirect | Yes | Medium | Hard | **Critical** |
| Brand approval / relationship verified | 24 | Yes (branded) | Yes | Yes | High | Hard | High |
| Comparable operator assignments | 24 | No | Yes | Yes | Very high | Hard | High |
| Conversion / reflag experience | 24 | No | Yes (conversion deals) | Indirect | Very high | Medium | High |
| Owner reporting / governance | 22 | No | Yes (institutional) | Indirect | Medium | Medium | Medium |
| Regional resources / capacity | 24 | No | Soft | Indirect | Medium | Hard | Medium |
| Brands currently operated | 0–3 | No | Yes | Indirect | High | Medium | Medium (maintain) |
| Chain scales | 0 | Yes | Yes | Indirect | High | Easy | Maintain |
| Active status | 0 | Yes | No | No | None | Easy | Maintain |
| Generic offered-services checklist | 22 | No | **No** (table stakes) | No | None | Easy | **Deprioritize** |
| Fee / commercial economics | 24 | No | No (baseline) | No | Low (later) | Hard | Low (Level E) |

“Coverage only” fields (do not unlock Ranking Ready alone): generic offered services, narrative Explorer copy, Master `Source Type` metadata without claim-level evidence.

---

## Smallest set with largest credibility gain

Complete these **four** enrichment tracks before any owner-facing Top-5:

1. **Structured Active Countries** (+ Market Presence Type) for the Active 24  
2. **Management Structures Supported** for the Active 24  
3. **≥1 claim-level evidence source** (URL/type/date/class) for operators that will be ranked  
4. **≥1 structured comparable** (or verified conversion/new-build experience) for pipeline-relevant operators  

Together these unlock Conditionally Rankable → Ranking Ready for a shortlist of CALA-relevant operators without chasing every existing Airtable checkbox.

---

## Real-deal sensitivity

| Missing field | Deals affected | Effect |
| ------------- | -------------- | ------ |
| evidenceSource | A, B, C | Blocks Ranking Ready universe (0 production pool) |
| operatingStructures | A, B, C | Inflates “With Conditions”; weak structure factor |
| Structured geography | A, B (esp.) | Prose markets mislead humans; engine correctly withholds Ranking Ready |
| brandApprovals | Branded urban (A) | Cannot confirm BM / approved operators |
| conversionExperience | Conversion archetypes | Specialists cannot differentiate |
| mixed-use comps | Deal C | Top candidate remains Limited confidence |

---

## Threshold note (50%)

The 50% project-coverage floor is **not** the binding constraint today — critical field gaps are. Raising the floor would further empty Top-5 without improving data quality. **Recommend keep 50%** until ≥5 operators meet critical Ranking Ready fields on representative projects; then reassess with evidence.

---

## What not to enrich first

- Generic Offered Services checklists  
- Fee fields for baseline ranking  
- Narrative Explorer-only polish without structured geo/structures/evidence  
- Completing every Commercial checkbox for inactive/out-of-scope operators
