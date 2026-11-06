# 38-Hotel Visual QA Report

Method: automated page-density + content flags on every regenerated PDF; representative visual sample review across pilots, long/short reports, and flagged hotels.

**Manual every-page review of all 38 was not claimed.** Flagged and sample hotels listed below should be opened in Admin Preview.

## Samples / flags

- **AC Hotel A Coruña** (9p, reason=design_pilot) chars/page=[367,3185,1857,1982,2368,2990,2387,2751,1555] flags=none
- **Bethesda Marriott** (8p, reason=representative_sample) chars/page=[368,3529,3084,2511,2627,2529,2494,1744] flags=none
- **Cambridge Beaches Resort & Spa** (10p, reason=long_report) chars/page=[386,3646,2675,2175,2474,2834,1931,1953,2654,1420] flags=none
- **Casas del XVI** (7p, reason=representative_sample) chars/page=[377,2769,1665,2442,2007,2156,2670] flags=none
- **Castillo Hotel Son Vida, a Luxury Collection Hotel, Mallorca** (11p, reason=design_pilot) chars/page=[418,3325,1876,2236,2368,3363,2540,1946,2509,2017,1377] flags=none
- **Charlotte Marriott City Center** (8p, reason=representative_sample) chars/page=[388,3448,2467,2458,2897,1961,2365,1232] flags=none
- **Falls Church Marriott Fairview Park** (7p, reason=representative_sample) chars/page=[390,3307,2384,2532,2468,2514,1311] flags=none
- **Faranda Collection Bogotá** (8p, reason=representative_sample) chars/page=[377,2693,1761,2465,2023,2274,2525,1084] flags=none
- **Hotel Phillips Kansas City, Curio Collection by Hilton** (10p, reason=long_report) chars/page=[409,3468,2347,2032,2502,2435,1873,2546,2038,1390] flags=none
- **The Mayflower Hotel, Autograph Collection** (10p, reason=long_report) chars/page=[389,3349,2250,2054,2493,2401,1829,2064,2061,1363] flags=none
- **Sheraton Mallorca Arabella Golf Hotel** (11p, reason=long_report) chars/page=[395,3623,2023,1747,2199,3553,1903,2281,1971,1768,1573] flags=none
- **Spice Island Beach Resort** (10p, reason=long_report) chars/page=[366,3623,2790,2171,2482,2101,2467,2222,2371,1558] flags=none
- **Waterstone Resort & Marina** (8p, reason=design_pilot) chars/page=[378,3277,2273,2475,3170,1935,2042,1110] flags=none
- **Wyndham Alltra Punta Cana** (10p, reason=long_report) chars/page=[381,3699,2161,2012,2258,2712,2396,2155,2184,1456] flags=none

## Automated layout rules applied to all regenerated PDFs

- No `[object Object]`
- No near-empty final page (<400 chars)
- Executive Audit Summary present
- HIGH actions appear in extracted text
- Annotation link count recorded
