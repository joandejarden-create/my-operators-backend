# Universal Hotel Record Resolver v1

**Status:** `production_census_universal_record_resolver_v1_partial_source_remaining`
**Objective:** `universal-record-resolver-v1`
**Census mode:** `field-completion-only`
**Secondary sources enabled:** false
**Webhound as Census SoT:** false
**Write target:** Hotel Property Census (`tbl9aY5ijiuIzzWam`)
**Airtable writes:** true

## Summary

- Incomplete records scanned: 80
- Records resolved: 0
- Records partially resolved: 80
- Records unresolved: 0
- Records updated: 10
- Records inserted: 0

## Field writes

| Field | Count |
| --- | ---: |
| Canonical Property Name | 10 |
| Hotel URL | 0 |
| State / Region | 0 |
| Market | 0 |
| Submarket | 0 |
| Address | 0 |
| Phone | 0 |
| Rooms | 0 |
| Coordinates | 0 |

## Choice MX043

- Record ID: recmWNlAbpgMinyLt
- Before: {"Canonical Property Name":"Quality Inn Chihuahua","Property Name":"Quality Inn Chihuahua","Address":null,"Phone":null,"Rooms / Keys":null,"Market":"Chihuahua","Submarket":null,"Latitude":null,"Official":"https://www.choicehotels.com/chihuahua/chihuahua/quality-inn-hotels/mx043"}
- After: {"Canonical Property Name":"Quality Inn Chihuahua","Property Name":"Quality Inn Chihuahua","Address":null,"Phone":null,"Rooms / Keys":null,"Market":"Chihuahua","Submarket":null,"Latitude":null,"Official":"https://www.choicehotels.com/chihuahua/chihuahua/quality-inn-hotels/mx043"}
- Patch keys: Continent, Sub-Continent, Last Reviewed Date, Enrichment Status
- Blockers: not_in_choice_cala_regional; bot_blocked; choice_property_page_unavailable; submarket_not_high; timeout; submarket_not_high; mapbox_waiting_for_high_address

## Secondary / external

- Secondary opportunities: 0
- Secondary writes: 0
- External source decision report: `reports/research-engine-v2/hotel-census-external-source-options.md`

## Top unresolved (sample)

- recabSgALHHvys0In (Choice): continent, sub_continent, market, submarket, address, phone, rooms, coordinates
- recoKIP6S1Gh5fSXb (Choice): continent, sub_continent, market, submarket, address, phone, rooms, coordinates
- recsBUnZKJuP3bW5M (Choice): continent, sub_continent, market, submarket, address, phone, rooms, coordinates
- rec0qmO7Xj7uyjWLZ (Choice): continent, sub_continent, submarket, address, phone, rooms, coordinates
- rec29iAAbH99948Hi (Choice): continent, sub_continent, submarket, address, phone, rooms, coordinates
- rec5su7y7H7ZugdIO (Choice): continent, sub_continent, submarket, address, phone, rooms, coordinates
- rec61aOhNCTEqGAkE (Choice): continent, sub_continent, submarket, address, phone, rooms, coordinates
- recLore4MMly9ziBB (Choice): continent, sub_continent, submarket, address, phone, rooms, coordinates
- recSoXjB3IFyLIikx (Choice): continent, sub_continent, submarket, address, phone, rooms, coordinates
- recFePITe3R03edAY (Choice): continent, sub_continent, address, phone, rooms, coordinates
- rec35J5pknfdYHECD (Choice): state_region, continent, sub_continent, market, submarket, address, phone, rooms, coordinates
- recCBwyTopFYX8wir (Choice): state_region, continent, sub_continent, market, submarket, address, phone, rooms, coordinates
- recNx55qHaDlu1SGJ (Choice): state_region, continent, sub_continent, market, submarket, address, phone, rooms, coordinates
- recYlGs9tPhxf9rvu (Choice): state_region, continent, sub_continent, market, submarket, address, phone, rooms, coordinates
- recsQ1Zd95zEQHXZe (Choice): state_region, continent, sub_continent, market, submarket, address, phone, rooms, coordinates

## Command to continue

```bash
ALLOW_CENSUS_AUTOPILOT_APPLY=1 \
CONFIRM_WRITE_TO_PRODUCTION_CENSUS=1 \
CONFIRM_NO_BRAND_EXPLORER_WRITES=1 \
CONFIRM_NO_OWNER_OPERATOR_WRITES=1 \
ENABLE_SECONDARY_HOTEL_DATA_SOURCES=0 \
npm run census:autopilot -- --region CALA --scope official-parent-inventory --mode mission \
  --objective universal-record-resolver-v1 \
  --census-mode field-completion-only \
  --strategy highest-yield-safe --run-until-complete --max-passes 10 --batch-size 100 \
  --confirm-safe-writes --confirm-write-to-production-census \
  --confirm-no-brand-explorer-writes --confirm-no-owner-operator \
  --confirm-no-date-writes --confirm-no-recent-momentum \
  --confirm-no-company-validation --confirm-webhound-not-production-source \
  --enable-production-writes
```
