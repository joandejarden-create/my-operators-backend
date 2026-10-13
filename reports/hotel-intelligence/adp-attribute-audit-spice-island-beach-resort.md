# ADP Attribute Audit — Spice Island Beach Resort

Generated: 2026-10-03T14:58:44.280Z
HPC Hotel ID: `recKRJjcPnb4tVDDS`
ADP Property ID: `adp_spice_island_beach_resort`
Mode: DRY-RUN

## Summary

| Metric | Value |
|---|---|
| Active attributes proposed | 46 |
| Used in ADP | 42 |
| Not used in ADP | 4 |
| Categories | Identity, Positioning, Location, Commercial, Meeting / Group, Amenity, Demand Node, Seasonality, Need Period |
| Missing critical | Total Meeting Space Sq Ft, Meeting Room Count |
| Profile completeness | 5/7 |
| Airtable creates | 0 |
| Airtable updates | 46 |
| Airtable deactivates | 0 |

## Attributes used by ADP

| Attribute | Value | Category | ADP Use Type | Source Type | Confidence |
|---|---|---|---|---|---|
| Hotel Name | Spice Island Beach Resort | Identity | Prompt Context; Query Generation; Exclusion Logic | HPC | HIGH |
| Brand | Small Luxury Hotels of the World | Positioning | Prompt Context; Query Generation; Recommendation Context | HPC | HIGH |
| Property Identity Key | p55_gplace_ChIJtaIWROAgOIwRhr20ml2iNYQ | Identity | Exclusion Logic; Reporting Only | HPC | HIGH |
| Address | Grand Anse Beach | Location | Query Generation; Prompt Context; Exclusion Logic | HPC | HIGH |
| City | St George's | Location | Query Generation; Prompt Context; Exclusion Logic | HPC | HIGH |
| State | Grenada | Location | Query Generation; Prompt Context; Exclusion Logic | HPC | HIGH |
| Market | Grenada | Location | Query Generation; Prompt Context; Exclusion Logic | HPC | HIGH |
| Submarket | Grand Anse Beach | Location | Query Generation; Prompt Context; Exclusion Logic | HPC | HIGH |
| Rooms | 64 | Commercial | Prompt Context; Recommendation Context | Hotel Commercial Profile | HIGH |
| Official Events URL | https://www.spiceislandbeachresort.com/ | Meeting / Group | Reporting Only; Prompt Context | Hotel Commercial Profile | HIGH |
| Official Property URL | https://www.spiceislandbeachresort.com/ | Identity | Reporting Only; Prompt Context | HPC | HIGH |
| luxury | true | Amenity | Query Generation; Scoring; Prompt Context | Hotel Commercial Profile | MEDIUM |
| beachfront | true | Amenity | Query Generation; Scoring; Prompt Context | Hotel Commercial Profile | MEDIUM |
| oceanfront | true | Amenity | Query Generation; Scoring; Prompt Context | Hotel Commercial Profile | MEDIUM |
| island_resort | true | Amenity | Query Generation; Scoring; Prompt Context | Hotel Commercial Profile | MEDIUM |
| resort | true | Amenity | Query Generation; Scoring; Prompt Context | Hotel Commercial Profile | MEDIUM |
| all_inclusive_option | true | Amenity | Query Generation; Scoring; Prompt Context | Hotel Commercial Profile | MEDIUM |
| suite_inventory | true | Amenity | Query Generation; Scoring; Prompt Context | Hotel Commercial Profile | MEDIUM |
| swim_out | true | Amenity | Query Generation; Scoring; Prompt Context | Hotel Commercial Profile | MEDIUM |
| spa | true | Amenity | Query Generation; Scoring; Prompt Context | Hotel Commercial Profile | MEDIUM |
| full_service_spa | true | Amenity | Query Generation; Scoring; Prompt Context | Hotel Commercial Profile | MEDIUM |
| outdoor_pool | true | Amenity | Query Generation; Scoring; Prompt Context | Hotel Commercial Profile | MEDIUM |
| tennis_courts | true | Amenity | Query Generation; Scoring; Prompt Context | Hotel Commercial Profile | MEDIUM |
| watersports | true | Amenity | Query Generation; Scoring; Prompt Context | Hotel Commercial Profile | MEDIUM |
| kayaking | true | Amenity | Query Generation; Scoring; Prompt Context | Hotel Commercial Profile | MEDIUM |
| fine_dining | true | Amenity | Query Generation; Scoring; Prompt Context | Hotel Commercial Profile | MEDIUM |
| wedding_venue | true | Amenity | Query Generation; Scoring; Prompt Context | Hotel Commercial Profile | MEDIUM |
| honeymoon_destination | true | Amenity | Query Generation; Scoring; Prompt Context | Hotel Commercial Profile | MEDIUM |
| family_friendly | true | Amenity | Query Generation; Scoring; Prompt Context | Hotel Commercial Profile | MEDIUM |
| soft_brand | true | Amenity | Query Generation; Scoring; Prompt Context | Hotel Commercial Profile | MEDIUM |
| boutique_feel | true | Amenity | Query Generation; Scoring; Prompt Context | Hotel Commercial Profile | MEDIUM |
| full_service | true | Amenity | Query Generation; Scoring; Prompt Context | Hotel Commercial Profile | MEDIUM |
| five_diamond | true | Amenity | Query Generation; Scoring; Prompt Context | Hotel Commercial Profile | MEDIUM |
| meeting_space | true | Amenity | Query Generation; Scoring; Prompt Context | Hotel Commercial Profile | MEDIUM |
| event_space_outdoor | true | Amenity | Query Generation; Scoring; Prompt Context | Hotel Commercial Profile | MEDIUM |
| Demand Node: Port Louis Marina / yachting | Port Louis Marina / yachting | Demand Node | Query Generation; Demand Family Selection; Prompt Context | Public Research | MEDIUM |
| Demand Node: Grand Anse Beach | Grand Anse Beach | Demand Node | Query Generation; Demand Family Selection; Prompt Context | Public Research | HIGH |
| Demand Node: Grenada tourism / travel-trade | Grenada tourism / travel-trade | Demand Node | Query Generation; Demand Family Selection; Prompt Context | Public Research | HIGH |
| Demand Node: St. George's | St. George's | Demand Node | Query Generation; Demand Family Selection; Prompt Context | Public Research | HIGH |
| Demand Node: Wedding / honeymoon destination ecosystem | Wedding / honeymoon destination ecosystem | Demand Node | Query Generation; Demand Family Selection; Prompt Context | Public Research | HIGH |
| Demand Node: Maurice Bishop International Airport (GND) | Maurice Bishop International Airport (GND) | Demand Node | Query Generation; Demand Family Selection; Prompt Context | Public Research | HIGH |
| Event Space: Grand Anse Beach / outdoor wedding venue | {"sqFt":null,"type":"Outdoor","capacities":{"theaterOrEvent":null}} | Meeting / Group | Prompt Context; Recommendation Context | Hotel Event Space | MEDIUM |

## Attributes present but not used by ADP

| Attribute | Value | Notes |
|---|---|---|
| Country | Grenada | Available in Hotel Intelligence; not currently wired into ADP consumption paths. |
| Caribbean high season (approx Dec–Apr) | Public Caribbean leisure high season commonly cited by regio | Available in Hotel Intelligence; not currently wired into ADP consumption paths. Public seasonality available; ADP does not currently inject into prompts. |
| Atlantic hurricane season travel caution (Jun–Nov) | NOAA Atlantic hurricane season window — public travel-patter | Available in Hotel Intelligence; not currently wired into ADP consumption paths. Public seasonality available; ADP does not currently inject into prompts. |
| Need Period | NOT_PROVIDED | Present in Hotel Intelligence; ADP does not yet consume seasonality/need periods in live prompts. Hotel-supplied need periods not provided. Public/market seasonality is separate. Not an ADP production blocker; ADP does not consume need periods today. |

## How ADP uses key attributes

- **Hotel Name** = `Spice Island Beach Resort` → Prompt Context / Query Generation / Exclusion Logic
- **Brand** = `Small Luxury Hotels of the World` → Prompt Context / Query Generation / Recommendation Context
- **Property Identity Key** = `p55_gplace_ChIJtaIWROAgOIwRhr20ml2iNYQ` → Exclusion Logic / Reporting Only
- **Address** = `Grand Anse Beach` → Query Generation / Prompt Context / Exclusion Logic
- **City** = `St George's` → Query Generation / Prompt Context / Exclusion Logic
- **State** = `Grenada` → Query Generation / Prompt Context / Exclusion Logic
- **Market** = `Grenada` → Query Generation / Prompt Context / Exclusion Logic
- **Submarket** = `Grand Anse Beach` → Query Generation / Prompt Context / Exclusion Logic
- **Rooms** = `64` → Prompt Context / Recommendation Context
- **Official Events URL** = `https://www.spiceislandbeachresort.com/` → Reporting Only / Prompt Context
- **Official Property URL** = `https://www.spiceislandbeachresort.com/` → Reporting Only / Prompt Context
- **luxury** = `true` → Query Generation / Scoring / Prompt Context
- **beachfront** = `true` → Query Generation / Scoring / Prompt Context
- **oceanfront** = `true` → Query Generation / Scoring / Prompt Context
- **island_resort** = `true` → Query Generation / Scoring / Prompt Context
- **resort** = `true` → Query Generation / Scoring / Prompt Context
- **all_inclusive_option** = `true` → Query Generation / Scoring / Prompt Context
- **suite_inventory** = `true` → Query Generation / Scoring / Prompt Context
- **swim_out** = `true` → Query Generation / Scoring / Prompt Context
- **spa** = `true` → Query Generation / Scoring / Prompt Context
- **full_service_spa** = `true` → Query Generation / Scoring / Prompt Context
- **outdoor_pool** = `true` → Query Generation / Scoring / Prompt Context
- **tennis_courts** = `true` → Query Generation / Scoring / Prompt Context
- **watersports** = `true` → Query Generation / Scoring / Prompt Context
- **kayaking** = `true` → Query Generation / Scoring / Prompt Context
