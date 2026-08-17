# Level 2 Adapter Wave 2

**Status:** `production_census_level_2_adapter_wave_2_partial_source_remaining`
**Objective:** `level-2-adapter-wave-2`
**Extractor:** `census-level-2-parent-extractors-v2-wave-2`
**Write target:** Hotel Property Census (`tbl9aY5ijiuIzzWam`)
**Airtable writes:** no
**Brand Setup writes:** false
**Brand Explorer writes:** false
**Updates:** 0
**Runtime ms:** 99797

## Level 2 before → after

| Metric | Before | After |
| --- | ---: | ---: |
| Address complete | 375 | 375 |
| Address Confidence High | 324 | 324 |
| Address Source URL complete | 325 | 325 |
| Lat/Long complete | 379 | 379 |
| Mapbox eligible | 58 | 58 |
| Coordinates written (chained) | — | — |
| Phone complete | 350 | 350 |
| Rooms complete | 116 | 116 |
| Complete Census v1 | 14 | 14 |

## Adapter yield

| Adapter | High writes / notes |
| --- | --- |
| Choice High writes | 0 |
| Marriott High writes | 0 |
| Rooms High writes | 0 |
| Bot-blocked sources | 0 |
| Source-insufficient | 11 |
| Steward conflicts | 0 |
| Fields written | (none) |
| Records updated | 0 |

## By parent

- **Marriott**: scanned=228 high=0 blocked=0 insufficient=11

## Operations

- High proposals: 0
- Records updated: 0
- Fields written: (none)
- Fetch attempted/ok/blocked: 0/0/0
- Steward conflicts: 0
- Chained cala-census-completion: no
- Chained status: —

## Wave 2 adapter notes

- Choice: property ID prefixes (all CALA), exact URL match, name+city+country match; property-level Address Source URL
- Marriott: non-Akamai sitemap/MARSHA metadata only; no invented address/phone/rooms when metadata insufficient
- Rooms: official HTML extractor + JSON-LD; High only; no OTA/inference
- Soft Clean Core autofill (Canonical / Source Family / Data Confidence Tier) when that is the only gate

## Examples


## Safety

- Hotel Property Census only
- Brand Setup / Brand Explorer untouched
- No owner/operator/date / Recent Momentum / Company Validated
- No Mapbox-as-address / Google / OTA phone
- No weak inference; steward conflicts held

## Next recommended action

Official Level 2 sources remain partial (bot-blocked pages / missing street JSON-LD / directory gaps). Do not invent; expand parent adapters or steward blocked families.
