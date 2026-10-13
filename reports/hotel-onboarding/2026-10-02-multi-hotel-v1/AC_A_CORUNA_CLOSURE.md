# AC Hotel A Coruña — Closure

**HPC:** `rec2PVBDavppGpenm`  
**ADP:** `adp_ac_hotel_a_coruna` · 39 active attributes  
**HI_COMPLETE:** YES  
**Capability:** 116 rooms · 6 meeting rooms · Banquets to 290 · Expocoruña / port / university demand nodes present

## Why historically RESEARCHED_NO_READY

Cycle 1–2 summaries show discovery candidates qualifying to **watchlist only** with **0 customer-visible** promotions. Not an HI/ADP attribute failure — lodging evidence + actionability gates blocked Strict Ready.

## This sprint

| Step | Result |
|------|--------|
| HI completeness | HI_COMPLETE |
| ADP attribute sync | 39 active; 5 creates applied earlier then stable |
| GDI seed | WEEKLY_READY applied |
| Market cycle-2 dry-run | **FAILED** — Airtable `INVALID_VALUE_FOR_COLUMN` on field `eventStartDate` |
| Strict Ready | 0 |

## Exact blocker

1. **Product/research:** insufficient future lodging-validated, hotel-fit, open-placement opportunities under current gates for A Coruña/Galicia  
2. **Engineering:** cycle-2 dry path attempted an invalid `eventStartDate` write — must fix before next apply

Do not lower qualification thresholds. Fix schema/date normalization, then rerun market-first dry.

## Closure outcome

**ADP READY — GDI PUBLIC DATA CEILING** (plus schema defect to repair)

Documented true public-data ceiling for Strict Ready under current gates; engineering defect is separate and must not be papered over by relaxing fit.
