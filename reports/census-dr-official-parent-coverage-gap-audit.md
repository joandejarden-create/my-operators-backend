# DR Official Parent Coverage Gap Audit

**Country:** Dominican Republic
**Airtable writes:** no
**Generated:** 2026-08-07T14:09:33.652Z

## DR Hotel Property Census

- Total DR rows: **174**
- DR OSM lineage: **114**

### Top brands in census

| Brand | Count |
| --- | ---: |
| Independent | 14 |
| RIU | 11 |
| Bahía Príncipe | 9 |
| Barceló | 8 |
| Hodelpa | 8 |
| Iberostar | 7 |
| Autograph Collection | 5 |
| Registry Collection | 5 |
| Wyndham | 5 |
| Dreams (Hyatt Inclusive Collection) | 4 |
| Holiday Inn | 4 |
| Be Live | 3 |
| Catalonia | 3 |
| Courtyard by Marriott | 3 |
| Excellence Resorts | 3 |
| Four Points by Sheraton | 3 |
| Iberostar Selection | 3 |
| Iberostar Waves | 3 |
| InterContinental | 3 |
| Majestic Resorts | 3 |
| Meliá | 3 |
| Occidental | 3 |
| AC Hotels by Marriott | 2 |
| Amhsa Marina Hotels | 2 |
| Ascend Hotel Collection | 2 |
| Breathless (Hyatt Inclusive Collection) | 2 |
| Curio Collection by Hilton | 2 |
| Embassy Suites by Hilton | 2 |
| Hampton by Hilton | 2 |
| Hard Rock Hotels | 2 |
| Karisma Hotels | 2 |
| Lopesan | 2 |
| Marriott Hotels | 2 |
| Preferred Hotels & Resorts | 2 |
| Renaissance Hotels | 2 |
| Small Luxury Hotels of the World | 2 |
| St. Regis | 2 |
| The Luxury Collection | 2 |
| Trademark Collection by Wyndham | 2 |
| Aloft Hotels | 1 |

## Adapter parents (official discovery)

| Parent | Official | Exact | Missing High | Missing Steward | Status |
| --- | ---: | ---: | ---: | ---: | --- |
| Marriott | 25 | 23 | 0 | 2 | production_census_coverage_reconciliation_v1_partial_missing_remaining |
| Hilton | 10 | 10 | 0 | 0 | production_census_coverage_reconciliation_v1_complete |
| IHG | 12 | 12 | 0 | 0 | production_census_coverage_reconciliation_v1_complete |
| Choice | 3 | 3 | 0 | 0 | production_census_coverage_reconciliation_v1_complete |
| Accor | 0 | 0 | 0 | 0 | production_census_coverage_reconciliation_v1_blocked |
| Wyndham | 10 | 10 | 0 | 0 | production_census_coverage_reconciliation_v1_complete |
| Preferred | 2 | 2 | 0 | 0 | production_census_coverage_reconciliation_v1_complete |

### Totals (adapter parents only)

- Official inventory rows discovered: **62**
- Exact matches: **60**
- Missing High: **0**
- Missing Steward: **2**

### Missing High (sample)

- (none)

### Missing Steward (sample)

- **The Ocean Club, a Luxury Collection Resort, Costa Norte** (Marriott / The Luxury Collection, ?) — https://www.marriott.com/en-us/hotels/poplc-the-ocean-club-a-luxury-collection-resort-costa-norte/overview
- **Donoma Las Terrenas Resort & Villas, Autograph Collection** (Marriott / Autograph Collection, ?) — https://www.marriott.com/en-us/hotels/azsak-donoma-las-terrenas-resort-and-villas-autograph-collection/overview

## Resort brands without official adapters (census presence only)

| Brand group | In DR census | Note |
| --- | ---: | --- |
| RIU | 11 | No Autopilot official discovery adapter — coverage gap cannot be auto-reconciled yet |
| Barceló / Occidental | 11 | No Autopilot official discovery adapter — coverage gap cannot be auto-reconciled yet |
| Bahía Príncipe | 9 | No Autopilot official discovery adapter — coverage gap cannot be auto-reconciled yet |
| Meliá | 3 | No Autopilot official discovery adapter — coverage gap cannot be auto-reconciled yet |
| Catalonia | 4 | No Autopilot official discovery adapter — coverage gap cannot be auto-reconciled yet |
| Be Live | 3 | No Autopilot official discovery adapter — coverage gap cannot be auto-reconciled yet |
| Hyatt Inclusive / Dreams / Breathless / Secrets | 9 | No Autopilot official discovery adapter — coverage gap cannot be auto-reconciled yet |
| Iberostar | 14 | No Autopilot official discovery adapter — coverage gap cannot be auto-reconciled yet |
| Hard Rock | 2 | No Autopilot official discovery adapter — coverage gap cannot be auto-reconciled yet |
| Lopesan | 2 | No Autopilot official discovery adapter — coverage gap cannot be auto-reconciled yet |
| Sirenis | 1 | No Autopilot official discovery adapter — coverage gap cannot be auto-reconciled yet |
| Hodelpa | 8 | No Autopilot official discovery adapter — coverage gap cannot be auto-reconciled yet |

## Next actions

- No High-confidence missing properties from adapter parents for DR
- Build/extend official discovery adapters for RIU / Barceló / Bahía / Meliá / Hyatt Inclusive before treating those brands as coverage-complete
- Do not use Google Travel 2,641 as a census target — hotels+rentals mix
