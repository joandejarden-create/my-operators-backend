# Production Census — Rooms Secondary Source Wave 2 v1

**Status:** `production_census_rooms_secondary_source_wave_2_v1_partial_source_remaining`
**Objective:** `rooms-secondary-source-wave-2-v1`
**Generated:** 2026-08-08T00:24:24.373Z
**Table:** Hotel Property Census (`tbl9aY5ijiuIzzWam`)
**Airtable writes:** true
**Inserts:** 0

## Schema

- Rooms Evidence Tier: **already_exists**
- Field exists: yes

## Rooms coverage

| Metric | Before | After |
| --- | ---: | ---: |
| With Rooms | 191 | 191 |
| Coverage % | 15.6 | 15.6 |
| Official High Rooms | 118 | 118 |
| Official High % | 61.78 | 61.78 |
| Secondary Source Rooms | 73 | 73 |
| Secondary Source % | 38.22 | 38.22 |
| Steward-Verified Rooms | 0 | 0 |
| Rooms Missing | 1033 | 1033 |
| Rooms Conflict (Hold) | 0 | 0 |

## Written this run

- Records updated: **0**
- Rooms values written: **0**
- Evidence tier backfills: **0**
- Official HTML rooms: **0**
- Colombia fuzzy rooms: **0**
- Conflicts held: **0**
- Phone written: **0**
- Phone blocked by policy: **817**

### By country


### By source type

- `trusted_secondary_source`: 73
- `(blank)`: 113
- `official_property_page`: 5

### By evidence tier

- `Tier 5 Tourism Board / Destination Authority`: 73
- `(blank)`: 118

## Country source discovery

### Mexico

- Missing rooms: 745
- Next: build_deterministic_property_level_adapter_or_steward_pack
- Live adapters: 0; discovery-only: 1; blocked/aggregate: 2

### Dominican Republic

- Missing rooms: 158
- Next: build_deterministic_property_level_adapter_or_steward_pack
- Live adapters: 0; discovery-only: 2; blocked/aggregate: 1

### Panama

- Missing rooms: 43
- Next: build_deterministic_property_level_adapter_or_steward_pack
- Live adapters: 0; discovery-only: 1; blocked/aggregate: 1

### Costa Rica

- Missing rooms: 46
- Next: build_deterministic_property_level_adapter_or_steward_pack
- Live adapters: 0; discovery-only: 1; blocked/aggregate: 1

### Colombia

- Missing rooms: 41
- Next: continue_adapter_enrichment_with_strict_match
- Live adapters: 1; discovery-only: 0; blocked/aggregate: 0

## Colombia remaining fuzzy / steward

- Steward candidates held: **41**
- Written via fuzzy: **0**

## Next backlog

- Mexico: build SECTUR RNT property-level rooms adapter (consulta portal is not bulk API); DATATUR remains aggregate-only
- Dominican Republic: MITUR RNT listing lacks habitaciones columns — need property detail scrape steward or licensed dataset
- Panama / Costa Rica: no property-level open lodging rooms dataset identified — official parent pages bot-blocked
- Colombia steward pack: 41 remaining ambiguous/low-sim RNT matches — do not force writes
- Official parent HTML: Marriott/IHG/Hilton/Choice commonly 403/Akamai from Autopilot runtime — need unblocked fetch path or DAM/factsheet cache
- Phone secondary still not approved
- Target coverage 95–100% (now 15.6%) requires Mexico-scale secondary adapter or unblocked official pages

## Continue command

```bash
ALLOW_CENSUS_AUTOPILOT_APPLY=1 CONFIRM_WRITE_TO_PRODUCTION_CENSUS=1 \
CONFIRM_NO_BRAND_EXPLORER_WRITES=1 CONFIRM_NO_OWNER_OPERATOR_WRITES=1 \
ENABLE_SECONDARY_HOTEL_DATA_SOURCES=1 ENABLE_SECONDARY_ROOMS_SOURCES=1 ENABLE_SECONDARY_PHONE_SOURCES=0 \
npm run census:autopilot -- --region CALA --scope official-parent-inventory --mode mission \
  --objective rooms-secondary-source-wave-2-v1 --census-mode field-completion-only \
  --strategy highest-yield-safe --run-until-complete --batch-size 100 \
  --confirm-safe-writes --confirm-write-to-production-census \
  --confirm-no-brand-explorer-writes --confirm-no-owner-operator \
  --confirm-no-date-writes --confirm-no-recent-momentum \
  --confirm-no-company-validation --confirm-webhound-not-production-source \
  --enable-production-writes
```
