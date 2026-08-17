# Production Census — Core Identity Quality Gate

**Status:** `production_census_core_identity_quality_gate_partial_steward_remaining`  
**Generated:** 2026-08-07T17:05:38.467Z  
**Write target:** Deal Capture Platform → Hotel Property Census (`tbl9aY5ijiuIzzWam`)  
**Airtable writes:** no (controlled)

## Counts

| Metric | Count |
|--------|------:|
| Records scanned | 1224 |
| Quality pass | 1207 |
| Safe High autofix proposals | 0 |
| Unknown city | 6 |
| Descriptor city | 0 |
| City/state mixed unresolved | 2 |
| Case/accent normalize candidates | 0 |
| City/state split candidates | 0 |
| Canonical blank | 3 |
| Canonical conflict | 6 |
| Canonical fixes proposed | 0 |
| Steward review | 9 |
| Duplicate risk | 0 |
| Blocked dirty core identity | 8 |
| Coordinate blocked (dirty identity) | 17 |

## Readiness

- Census Ready: 223
- Needs Steward Review: 181
- Needs Enrichment: 818
- Blocked: 2

## Fields written (High proposals)

- City
- State / Region
- Canonical Property Name

## Before / after examples

_None_

## Guards

- No weak city inference from hotel name / country / coordinates
- No Mapbox geocode for dirty identity
- Brand Setup / Brand Explorer / VIC / owner-operator-dates blocked
