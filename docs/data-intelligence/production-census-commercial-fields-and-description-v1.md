# Commercial Fields + Hotel Description v1

**Status:** `production_census_commercial_fields_and_description_v1_partial_secondary_source_decision_needed`
**Objective:** `commercial-fields-and-description-v1`
**Census mode:** `field-completion-only`
**Secondary sources enabled:** false
**Write target:** Hotel Property Census (`tbl9aY5ijiuIzzWam`)
**Airtable writes:** true

## Schema notes

- Description write field: `Hotel Description - AI Summary` (public-style, Census-field generated)
- Source Text reserved for official extracted text (not invented)
- Schema gaps (not written): Public Description, Internal Description, Description Status, Description Source Type, Description Reviewed Date, Description Notes

## Summary

- Records scanned: 321
- Records updated: 303
- Records inserted: 0
- Market writes: 44
- Submarket writes: 0
- Descriptions generated/written: 303
- Descriptions held: 18
- Phone written: 0
- Phone central reservation rejected/classified: 0
- Phone source missing: 307
- Rooms written: 0
- Rooms false positives rejected: 0
- Rooms source missing: 316
- Secondary source needed (phone/rooms): 623

## Choice MX043

- Record ID: recmWNlAbpgMinyLt
- Before: {"Canonical Property Name":"Quality Inn Chihuahua","Market":"Chihuahua","Submarket":null,"Address":null,"Phone":null,"Rooms / Keys":null,"Hotel Description - AI Summary":null,"State / Region":"Chihuahua"}
- After: {"Canonical Property Name":"Quality Inn Chihuahua","Market":"Chihuahua","Submarket":null,"Address":null,"Phone":null,"Rooms / Keys":null,"Hotel Description - AI Summary":"Quality Inn Chihuahua is a Quality Inn hotel in Chihuahua, Mexico, within the Chihuahua market.","State / Region":"Chihuahua"}
- Patch keys: Hotel Description - AI Summary, Last Reviewed Date, Enrichment Status

## Backlogs

- Market mapping backlog: 276
- Submarket mapping backlog: 321
- Secondary decision pack: `reports/research-engine-v2/hotel-census-secondary-source-decision-pack.md`

## Continue

```bash
ENABLE_SECONDARY_HOTEL_DATA_SOURCES=0 \
npm run census:autopilot -- --region CALA --scope official-parent-inventory --mode mission \
  --objective commercial-fields-and-description-v1 \
  --census-mode field-completion-only \
  --strategy highest-yield-safe --run-until-complete --max-passes 8 --batch-size 100 \
  --confirm-safe-writes --confirm-write-to-production-census \
  --confirm-no-brand-explorer-writes --confirm-no-owner-operator \
  --confirm-no-date-writes --confirm-no-recent-momentum \
  --confirm-no-company-validation --confirm-webhound-not-production-source \
  --enable-production-writes
```
