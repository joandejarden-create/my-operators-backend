# 38-Hotel Visual QA Report

Method: automated page-density + content flags on every regenerated PDF; representative visual sample review across pilots, long/short reports, and flagged hotels.

**Manual every-page review of all 38 was not claimed.** Flagged and sample hotels listed below should be opened in Admin Preview.

## Samples / flags

- **AC Hotel A Coruña** (8p, reason=design_pilot) chars/page=[367,3276,2027,1796,3087,2223,1913,2841] flags=none
- **Bethesda Marriott** (7p, reason=representative_sample) chars/page=[368,3088,2880,2220,2516,1904,2885] flags=none
- **Cambridge Beaches Resort & Spa** (10p, reason=long_report) chars/page=[386,3366,2384,1608,2339,2188,2210,2166,2654,1310] flags=none
- **Casas del XVI** (8p, reason=representative_sample) chars/page=[377,2721,2103,2206,1852,1701,2387,2169] flags=none
- **Castillo Hotel Son Vida, a Luxury Collection Hotel, Mallorca** (9p, reason=design_pilot) chars/page=[417,3515,2120,1903,2973,2449,2071,2510,3082] flags=none
- **Charlotte Marriott City Center** (8p, reason=representative_sample) chars/page=[388,3169,2494,2565,2250,2151,2443,1328] flags=none
- **Falls Church Marriott Fairview Park** (7p, reason=representative_sample) chars/page=[390,3195,2444,2182,2372,2315,1948] flags=none
- **Faranda Collection Bogotá** (8p, reason=representative_sample) chars/page=[377,2612,2291,2218,1798,2500,2704,1502] flags=none
- **Hilton New York Times Square** (10p, reason=flagged) chars/page=[385,3135,2127,2472,2410,2195,1761,2132,2914,78] flags=NEAR_EMPTY_FINAL_PAGE
- **Hotel Phillips Kansas City, Curio Collection by Hilton** (10p, reason=long_report) chars/page=[409,3193,1911,2473,1598,2268,1673,2546,2038,1283] flags=none
- **Sheraton Mallorca Arabella Golf Hotel** (10p, reason=long_report) chars/page=[395,3583,2026,1659,2228,2650,2054,1830,2682,1330] flags=none
- **Waterstone Resort & Marina** (7p, reason=design_pilot) chars/page=[378,3483,2896,2404,2044,1638,2189] flags=none

## Automated layout rules applied to all regenerated PDFs

- No `[object Object]`
- Near-empty final page (<400 chars): **1 hotel flagged** — Hilton New York Times Square
- Executive Audit Summary present
- HIGH actions appear in extracted text
- Annotation link count recorded
- Post-regen V4 CQA HTML gate rescan: **0 CRITICAL / 0 HIGH** across 38
