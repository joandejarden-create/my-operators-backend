# Production Census — Marriott Property URL + Unknown City Backfill

**Status:** `production_census_marriott_property_url_city_backfill_partial_remaining`  
**Generated:** 2026-08-06T20:21:06.848Z  
**Write target:** Deal Capture Platform → Hotel Property Census (`tbl9aY5ijiuIzzWam`)  
**Airtable writes:** yes  
**Inserts:** 0  

## Before → After

| Metric | Before | After |
|--------|-------:|------:|
| Clean Core (all Census) | 789 | 853 |
| Below Clean Core | 118 | 54 |
| Marriott Unknown city | 64 | 3 |
| Coordinate blocked (dirty identity) | 95 | 38 |

## Applied

- Records fixed: 77
- Fields written: Source URL, City, Canonical Property Name
- Property URLs found: 64
- Source URLs replaced: 64
- Cities written: 61
- Canonical written: 0
- Stewarded: 3

## Blocked source patterns

- marriott_country_hotel_sitemap_as_source_url
- akamai_blocked_property_page_json_ld
- hotel_name_only_city_inference_forbidden

## Examples

- `rec02wJ8dk7HtjPjx` Elena de Cobre, a Member of Design Hotels™: City `Unknown` → `(url only)`; Source → property URL
- `rec0H65Zl2xa0St9L` City Express by Marriott Ciudad Obregón: City `Unknown` → `Ciudad Obregón`; Source → property URL
- `rec0dxxcwt79FO1nZ` City Express by Marriott Nogales: City `Unknown` → `Nogales`; Source → property URL
- `rec30eORcGKDSQKhJ` Terrestre, a Member of Design Hotels™: City `Unknown` → `Puerto Escondido`; Source → property URL
- `rec3HSGv9Rt3Nwtmd` Courtyard by Marriott Panama Metromall: City `Unknown` → `Panama City`; Source → property URL
- `rec3TjoFBXiolr2lz` City Express by Marriott San Luis Potosí Zona Industrial: City `Unknown` → `San Luis Potosí`; Source → property URL
- `rec4327YEwJT59CAv` Casa Nizuc, a Tribute Portfolio Resort: City `Unknown` → `Cancún`; Source → property URL
- `rec4UM5tor3ZMOvpp` City Express by Marriott Tuxpan: City `Unknown` → `Tuxpan`; Source → property URL
- `rec6Zn0N2vUDkRvk5` City Express by Marriott Tapachula: City `Unknown` → `Tapachula`; Source → property URL
- `rec7Z9d1HeCnfobLW` The St. Regis Punta Mita Resort: City `Unknown` → `Punta Mita`; Source → property URL
- `rec9fNNwBiJXgLuFS` City Express by Marriott Rosarito: City `Unknown` → `Rosarito`; Source → property URL
- `recAWR4e268Jk7xGw` Costa Rica Marriott Hotel Hacienda Belen: City `Unknown` → `Belén`; Source → property URL

## Next recommended action

Steward remaining 3 Marriott Unknown cities (Design Hotels without High slug/IATA city). Keep address/Mapbox paused.
