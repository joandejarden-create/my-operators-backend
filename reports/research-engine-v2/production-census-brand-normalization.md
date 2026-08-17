# Production Census Brand Normalization

**Status:** `production_census_brand_normalization_partial_steward_remaining`
**Queue:** `brand_normalization`
**Write target:** Hotel Property Census (`tbl9aY5ijiuIzzWam`)
**Airtable writes:** no (controlled)
**Dictionary Active brands:** 14

## Audit counters

| Class | Count |
| --- | ---: |
| Scanned | 1224 |
| brand_valid | 228 |
| brand_blank | 0 |
| brand_misspelled | 0 |
| brand_alias_normalizable | 0 |
| brand_parent_mismatch | 0 |
| brand_source_mismatch | 0 |
| brand_property_name_mismatch | 0 |
| soft_brand_collection_conflict | 0 |
| brand_unknown_not_in_dictionary | 823 |
| steward_review_required | 173 |
| High proposals | 0 |
| Updates applied | 0 |

## Clean Core

- Before: —
- After: —

## Examples before / after

_None_

## Safety

- Hotel Property Census only
- Brand Setup read-only; Brand Explorer untouched
- No address / coords / phone / rooms
- No weak brand inference from hotel name or parent alone
- Source-conflicting overwrites stewarded
