# YOTEL READY quality — UI QA

| Check | Result |
|---|---|
| Customer-facing after reclass | **9** (canonical `filterCustomerFacingOpportunities`) |
| Ready after gate | **9** |
| Palexpo Art Genève on READY list | **NO** (FUTURE_WATCH) |
| Generic homepage as buyer path | **NO** |
| Unsupported Overflow Only on READY cards | **stripped** (UNKNOWN / FUTURE_CYCLE) |
| Duplicate Hotel Fit label | **FIXED** → Overall Hotel Fit |
| Evidence explanation vs verified stamps | **FIXED** on CQ read path |
| Tournament leak on Art Genève | **FIXED**; CHI sports copy allowed |
| Fit:PLAUSIBLE / child-account jargon | **stripped** from customer thesis |
| Demand campaigns hidden | YES (unchanged) |
| API list (post server restart) | Must match facing=9 |

## Browser notes

1. Restart Node server so gate + CQ + UI label changes load.
2. Open YOTEL GDI opportunities — expect **9** READY cards (not 13).
3. Palexpo venue cards appear as Future Watch / not sold as READY.
4. AidEx remains READY with `aid-expo.com/when-where` function path.
5. Detail drawer: single **Overall Hotel Fit**; evidence text not “0 verified” when verified stamps exist.

## Server restart required

Live process on :8080 must be restarted after this audit for API/UI to match canonical gate.
