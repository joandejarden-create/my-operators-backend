# Production Census Schema v1.1.4 — Rooms / Keys Provenance Apply

**Status:** `production_census_schema_v114_rooms_keys_provenance_applied_ready_for_controlled_autopilot`  
**Generated:** 2026-08-05T19:56:18.219Z  
**Apply executed:** true  
**Base:** Deal Capture Platform (`appCCU…foLk`)  
**Table:** Hotel Property Census (`tbl9aY5ijiuIzzWam`)

## 1. Executive summary

Schema-only: Rooms / Keys provenance fields + Hold on Rooms Confidence. No Autopilot apply. No Brand Explorer / Brand Setup writes. No owner/operator/date population.

| Metric | Value |
| --- | ---: |
| Field count | 108 → 111 |
| Fields created | 3 |
| Hold on Rooms Confidence | yes |
| Census records | 666 |
| Existing cell value drift | 0 |
| New provenance populated | 0 |
| Validation pass | true |

## 2. Fields created

- **Rooms Source Type** (`singleSelect` · `fld1REDWd9zlQBswU`) — options: official_property_page, official_brand_directory, official_hotel_website, official_press_release, official_development_page, trusted_secondary_source, steward_review
- **Rooms Reviewed Date** (`date` · `fldOzFFDqynSm7OaH`)
- **Rooms Notes** (`multilineText` · `fld8OIYN43wPYgPDM`)

## 3. Rooms Confidence — Hold option

Meta API `PATCH .../fields` rejects `options.choices` updates (422). Hold was added via **typecast seed then restore** on blank record `rec02wJ8dk7HtjPjx`; final Rooms Confidence values unchanged (drift 0).

Choices now: Exact, High, Medium, Low, Insufficient, Unknown, **Hold**.

## 4. Validation gates

- Hotel Property Census = 666
- Rooms / Keys, Rooms Confidence, Rooms Source URL intact
- New provenance fields blank
- Brand Explorer / Brand Setup untouched
- `npm run test:census-autopilot` PASS
- `npm run dealality:batch-learning-audit` → `dealality_batch_learning_system_ready`

## 5. Rename recommendation (not applied)

Optional later rename `Rooms Confidence` / `Rooms Source URL` → `Rooms / Keys*` for naming parity — report only.

## 6. Recommended next step

Run Autopilot controlled:

```bash
npm run census:autopilot -- --region CALA --scope active-brand-setup --mode controlled --strategy fastest-safe --run-until-complete --batch-size 250
```

## Apply command (reference)

```bash
ALLOW_PRODUCTION_CENSUS_SCHEMA_V114=1 \
CONFIRM_SCHEMA_ONLY_NO_RECORD_WRITES=1 \
CONFIRM_NO_BRAND_EXPLORER_WRITES=1 \
npm run research-engine-v2:production-census-schema-v114-rooms-keys-provenance -- --apply \
  --confirm-add-rooms-keys-provenance-fields-only \
  --confirm-no-record-writes \
  --confirm-no-brand-explorer-writes \
  --confirm-no-brand-setup-writes \
  --confirm-no-field-deletes \
  --confirm-no-field-renames \
  --confirm-no-field-population \
  --confirm-add-hold-to-rooms-confidence
```
