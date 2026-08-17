# Level 2 Source Extraction Mission v1

**Status:** `production_census_level_2_source_extraction_v1_partial_source_remaining`
**Objective:** `level-2-source-extraction-v1`
**Write target:** Hotel Property Census (`tbl9aY5ijiuIzzWam`)
**Airtable writes:** yes
**Brand Setup writes:** false
**Brand Explorer writes:** false
**Updates:** 112
**Runtime ms:** 1111009

## Level 2 before → after

| Metric | Before | After |
| --- | ---: | ---: |
| Address complete | 181 | 201 |
| Address Confidence High | 195 | 215 |
| Address Source URL complete | 195 | 215 |
| Lat/Long complete | 262 | 262 |
| Phone complete | 21 | 109 |
| Rooms complete | 5 | 5 |
| Complete Census v1 | 0 | 0 |

## By parent

- **Hilton**: scanned=88 high=88 blocked=0 insufficient=0
- **Marriott**: scanned=202 high=0 blocked=120 insufficient=82
- **IHG**: scanned=37 high=0 blocked=0 insufficient=37
- **Accor**: scanned=17 high=0 blocked=0 insufficient=17
- **Wyndham**: scanned=58 high=0 blocked=0 insufficient=58
- **Other**: scanned=388 high=11 blocked=0 insufficient=377

## Operations

- High proposals: 99
- Records updated: 112
- Fields written: Phone, Last Reviewed Date, Address, Address Confidence, Address Source URL, Market, Latitude, Longitude, Coordinate Source Type, Coordinate Confidence, Geocode Provider, Geocode Method, Geocode Reviewed Date
- Fetch attempted/ok/blocked: 120/0/120
- Steward conflicts: 0
- Chained cala-census-completion: yes
- Chained status: production_census_cala_completion_v1_partial_source_remaining

## Examples

- **DoubleTree by Hilton Celaya** (Hilton): `{"Address":null,"Phone":null,"Rooms / Keys":null}` → `{"Phone":"+524612490930","Last Reviewed Date":"2026-08-07"}`
- **Homewood Suites by Hilton Queretaro** (Hilton): `{"Address":"Gasa de Inc. a Carr. QRO-SLP  681","Phone":null,"Rooms / Keys":null}` → `{"Phone":"+524423683030","Last Reviewed Date":"2026-08-07"}`
- **Hilton Mexico City Santa Fe** (Hilton): `{"Address":"Antonio Dovali Jaime No. 70","Phone":null,"Rooms / Keys":null}` → `{"Phone":"+525559859000","Last Reviewed Date":"2026-08-07"}`
- **Hilton Garden Inn Uruapan** (Hilton): `{"Address":null,"Phone":null,"Rooms / Keys":null}` → `{"Phone":"+524523921100","Last Reviewed Date":"2026-08-07"}`
- **Hilton Queretaro** (Hilton): `{"Address":null,"Phone":null,"Rooms / Keys":null}` → `{"Phone":"+524421891640","Last Reviewed Date":"2026-08-07"}`
- **Tropicana Los Cabos, Tapestry Collection by Hilton** (Hilton): `{"Address":"Blvd. Antonio Mijares 30","Phone":null,"Rooms / Keys":null}` → `{"Phone":"+526241421580","Last Reviewed Date":"2026-08-07"}`
- **Hilton Garden Inn Guadalajara Airport** (Hilton): `{"Address":"Carr. Guadalajara Chapala Km 17.5","Phone":null,"Rooms / Keys":null}` → `{"Phone":"+523396894200","Last Reviewed Date":"2026-08-07"}`
- **Hilton Garden Inn Aguascalientes** (Hilton): `{"Address":"Blvd. Luis Donaldo Colosio 404","Phone":null,"Rooms / Keys":null}` → `{"Phone":"+524494781700","Last Reviewed Date":"2026-08-07"}`

## Safety

- Hotel Property Census only
- Brand Setup / Brand Explorer untouched
- No owner/operator/date / Recent Momentum / Company Validated
- No Mapbox-as-address / Google / OTA phone
- No weak inference; steward conflicts held

## Next recommended action

Official Level 2 sources remain partial (bot-blocked pages / missing street JSON-LD / directory gaps). Do not invent; expand parent adapters or steward blocked families.
