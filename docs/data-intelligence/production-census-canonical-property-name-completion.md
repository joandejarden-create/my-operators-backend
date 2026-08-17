# Canonical Property Name Completion

# Production Census — Canonical Property Name Completion

**Status:** `production_census_canonical_property_name_completion_partial_steward_remaining`  
**Generated:** 2026-08-07T17:06:09.900Z  
**Write target:** Deal Capture Platform → Hotel Property Census (`tbl9aY5ijiuIzzWam`)  
**Airtable writes:** no (controlled / proposal)

## Counts

| Metric | Count |
|--------|------:|
| Records scanned | 1224 |
| Complete clean | 1045 |
| Blank canonical | 3 |
| Dirty canonical | 0 |
| Safe autofill proposals | 0 |
| Safe cleanup proposals | 0 |
| Skipped identical | 0 |
| Steward cases | 176 |
| Duplicate risks blocked | 0 |

## Fields written

- Canonical Property Name

## Before / after examples

_None in this run._

## Guards

- Brand Setup / Brand Explorer: untouched
- VIC / old Census: blocked
- Owner / operator / dates / Recent Momentum / validation fields: blocked


## Rules

- High confidence only
- Blank autofill from clean Property Name when Brand + City + Country + Source URL present
- Exact membership suffix cleanup only (Radisson Individuals / Preferred Hotels & Resorts)
- Never overwrite materially different populated values
- Duplicate risk → steward
- Hotel Property Census only

## Commands

```bash
npm run census:autopilot -- --region CALA --scope active-brand-setup --mode controlled \
  --strategy fastest-safe --queue key_field_completion --run-until-complete --batch-size 250
```
