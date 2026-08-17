# Census intake format remediation

**Status:** `applied`
**Version:** census-intake-format-remediation-v1
**Apply executed:** true
**Airtable writes:** true

## Counts

| Metric | Count |
| --- | ---: |
| Records found | 21 |
| Patches | 20 |
| Deletes (hostels) | 1 |
| No-op | 0 |

## Patch sample

| Name | Before Family | After Family | Before BF | After BF | Clear State |
| --- | --- | --- | --- | --- | --- |
| Hotel BQ;Vent W&P Santo Domingo | independent_open_sources | (clear) | null | null | true |
| Hotel Casa Coco | independent_open_sources | (clear) | null | null | true |
| Hotal San Francisco de Asis | independent_open_sources | (clear) | null | null | true |
| Hotel Europa | independent_open_sources | (clear) | null | null | true |
| The Ocean Club, a Luxury Collection Resort | The Luxury Collection | Marriott | The Luxury Collection | Marriott | true |
| Radisson Blu Resort & Residence, Punta Cana | Radisson Blu by Choice | Choice | Radisson Blu by Choice | Choice | true |
| Embassy Suites by Hilton | Hilton Hotels & Resorts | Hilton | Hilton Hotels & Resorts | Hilton | true |
| Santo Domingo Bed & Breakfast | independent_open_sources | (clear) | null | null | true |
| Villas CODEVI | independent_open_sources | (clear) | null | null | true |
| Radisson Hotel Santo Domingo | Radisson by Choice | Choice | Radisson by Choice | Choice | true |
| Bungalows of Las Galeras | independent_open_sources | (clear) | null | null | true |
| Jarabacua River Club & Resort | independent_open_sources | (clear) | null | null | true |
| Bella Vista | independent_open_sources | (clear) | null | null | true |
| Four Points by Sheraton | Four Points by Sheraton | Marriott | Four Points by Sheraton | Marriott | true |
| Casa Barbara Las Terrenas | independent_open_sources | (clear) | null | null | true |
| Hotel & Apartments Buchen | independent_open_sources | (clear) | null | null | true |
| Hotel Tropicana Deluxe | independent_open_sources | (clear) | null | null | true |
| El Pelicano Apart-Hotel | independent_open_sources | (clear) | null | null | true |
| Hotel Villa La Plantacion | independent_open_sources | (clear) | null | null | true |
| Hotel Anselmo | independent_open_sources | (clear) | null | null | true |

## Deletes

- Hostal Ganesh (`osm_do_way_529318923`) — hostel/hostal out of scope
- Hotal San Francisco de Asis (`osm_do_way_630842152`) — hostal typo / hostel site; removed in follow-up

## Forward fix

Intake mapping now:
- **Family / Source Family** + **Brand Family** = parent only (`Marriott`, `Hilton`, `Choice`, …)
- Independents: leave Family/Brand Family blank (no `independent_open_sources`)
- **State / Region**: omit instead of writing `Unknown`
- Hostels/hostals (incl. `hotal` typo): reject at gate; never insert
- Hostal Ganesh (`osm_do_way_529318923`) — hostel/hostal out of scope
