# Remaining ADP hotels for GDI

Generated: 2026-09-17T18:15:00.000Z  
Universe: `listAdpGdiHotelUniverse()` (15 hotels)  
Selector: `listGdiSelectableHotels()`

## Already GDI-enabled (3)

| Hotel | HPC ID | Config | Opportunities | Selector |
|---|---|---|---|---|
| Bethesda Marriott | `recLuxvwwxID7U2B8` | yes | yes | yes |
| Waterstone Resort & Marina | `recgMYovrrZDJMqzX` | yes | yes | yes |
| Renaissance New York Times Square | `recG66DQJKP2c0UNh` | yes | yes | yes |

## Remaining ADP → need GDI (12)

| Wave | Hotel | HPC ID | ADP fixture |
|---|---|---|---|
| 1 | Hotel Phillips Kansas City | `rec8hHupaSwiWI3r7` | `hotel-phillips-kansas-city-property-profile.json` |
| 1 | NOW NOW NOHO | `recGkME49yYuxQl0u` | `now-now-noho-property-profile.json` |
| 1 | Cambridge Beaches Bermuda | `recIwaP1etgx2g9nA` | `cambridge-beaches-bermuda-property-profile.json` |
| 2 | JW Marriott Santo Domingo | `recESHsNsWUFYZrxR` | `jw-marriott-santo-domingo-property-profile.json` |
| 2 | Radisson Santo Domingo | `recUOyzOXn2Zdp98I` | `radisson-santo-domingo-property-profile.json` |
| 2 | Casas del XVI | `recjDsNzu93CFfe87` | `casas-del-xvi-property-profile.json` |
| 3 | St Regis Cap Cana | `recN76iEE6yAaPh8H` | `st-regis-cap-cana-property-profile.json` |
| 3 | St Regis Mexico City | `recRXmrakhSAuctwz` | `st-regis-mexico-city-property-profile.json` |
| 3 | JW Marriott Monterrey Valle | `recsn3BUKJ9PNfeZW` | `jw-marriott-monterrey-valle-property-profile.json` |
| 3 | Westin Monterrey Valle | `recD17Kxn6BcJjGFh` | `westin-monterrey-valle-property-profile.json` |
| 3 | Faranda Collection Bogotá | `rec9Tp0WBb2uk6w3u` | `faranda-collection-bogota-property-profile.json` |
| 3 | Hotel Caribe Faranda Grand | `recCEpdskZeUBvQwG` | `hotel-caribe-faranda-grand-property-profile.json` |

## Gate before Wave 1 discovery

Per `reports/research-engine/webhound-cutover-readiness.md`:

> Do not start Hotel #4 until Native (+ gated Parallel) blind discovery lanes are addressed.

Live `runGroupDemandResearch` still requires `seedCandidates` (historically Webhound-imported). Native blind opportunity discovery is **not** production-wired. Expansion must not reintroduce Webhound as a required production dependency.
