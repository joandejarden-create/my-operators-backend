# Parent Company / Brand Family Normalization

**Status:** `production_census_parent_company_normalization_partial_steward_remaining`
**Queue:** `parent_company_normalization`
**Canonical field:** Brand Family
**Write target:** Hotel Property Census (`tbl9aY5ijiuIzzWam`)
**Airtable writes:** no
**Brand Setup writes:** false
**Brand Explorer writes:** false

## Schema

- **Brand Family** — canonical parent rollup field (Autopilot SoT)
- **Family / Source Family** — source lineage (not overwritten by this queue)
- No `Parent Company` column on Hotel Property Census

## Canonical parents

- Marriott International
- Hilton
- IHG
- Choice Hotels International
- Accor
- Wyndham Hotels & Resorts
- Preferred Hotels & Resorts
- BWH Hotels
- Small Luxury Hotels of the World
- Bunkhouse

## Counters

| Metric | Count |
| --- | ---: |
| Records scanned | 1224 |
| Parent valid | 1036 |
| Parent blank | 0 |
| Alias normalizable | 0 |
| Brand/parent mismatch | 0 |
| Source/parent mismatch | 0 |
| Parent unknown | 18 |
| High parent fixes proposed | 0 |
| Steward cases | 170 |
| Records written | 0 |

## Clean Core before → after

| Metric | Before | After |
| --- | ---: | ---: |
| Clean Core | — | — |
| Parent valid | 1036 | — |
| Parent blank | 0 | — |

## Fields written

(none)

## Examples before → after


## Unresolved steward cases

- **Club Regina Los Cabos managed by Accor**: parent=`Accor` expected=`Accor` — steward_review_required; human_review_required
- **Elena de Cobre, a Member of Design Hotels™**: parent=`Accor` expected=`Marriott International` — steward_review_required; human_review_required
- **Mezzanine, an SLH Hotel**: parent=`SLH` expected=`Hilton` — steward_review_required; human_review_required
- **American Trade Hotel, an SLH Hotel**: parent=`SLH` expected=`Hilton` — steward_review_required; human_review_required
- **Rio Perdido, an SLH Hotel**: parent=`SLH` expected=`Hilton` — steward_review_required; human_review_required
- **La Purificadora, Puebla, a Member of Design Hotels™**: parent=`Accor` expected=`Marriott International` — steward_review_required; human_review_required
- **Hotel Riu Palace Bavaro**: parent=`RIU` expected=`—` — steward_review_required; human_review_required
- **Be Live Grand Bavaro**: parent=`Be Live` expected=`—` — steward_review_required; human_review_required
- **Bahía Príncipe Bouganville**: parent=`Bahía Príncipe` expected=`—` — steward_review_required; human_review_required
- **Catalonia Royal La Romana**: parent=`Catalonia` expected=`—` — steward_review_required; human_review_required
- **Terrestre, a Member of Design Hotels™**: parent=`Accor` expected=`Marriott International` — steward_review_required; human_review_required
- **Hotel Riu Naiboa**: parent=`RIU` expected=`—` — steward_review_required; human_review_required
- **Baja Club Hotel, La Paz, Baja California Sur, a Member of Design Hotels™**: parent=`Accor` expected=`Marriott International` — steward_review_required; human_review_required
- **Marival Distinct All-Suites & World Spa  Handwritten Collection**: parent=`Accor` expected=`Accor` — steward_review_required; human_review_required
- **Gran Bahia Principe Cayacoa**: parent=`Bahía Príncipe` expected=`—` — steward_review_required; human_review_required
- **Grand Sirenis Punta Cana Resort**: parent=`Sirenis Hotels & Resorts` expected=`—` — steward_review_required; human_review_required
- **Rosas & Xocolate, Mérida, a Member of Design Hotels™**: parent=`Accor` expected=`Marriott International` — steward_review_required; human_review_required
- **2026 Best Vacation Destinations**: parent=`IHG` expected=`IHG` — steward_review_required; human_review_required
- **Choice property MX197**: parent=`Choice Hotels International` expected=`Choice Hotels International` — steward_review_required; human_review_required
- **Majestic Colonial Hotel**: parent=`Majestic Resorts` expected=`—` — steward_review_required; human_review_required
- **Wake BioHotel, a Member of Design Hotels™**: parent=`Accor` expected=`Marriott International` — steward_review_required; human_review_required
- **Hard Rock Hotel**: parent=`Hard Rock Hotels` expected=`—` — steward_review_required; human_review_required
- **Hotel Barceló Santo Domingo**: parent=`Barceló` expected=`—` — steward_review_required; human_review_required
- **Hotel BQ;Vent W&P Santo Domingo**: parent=`(blank)` expected=`—` — steward_review_required; human_review_required
- **CONDESA df, Mexico City, a Member of Design Hotels™**: parent=`Accor` expected=`Marriott International` — steward_review_required; human_review_required

## Safety

- Hotel Property Census only
- No Brand Setup / Brand Explorer writes
- No address / coords / phone / rooms / owner / operator / date writes
- No weak parent inference; source conflicts stewarded
