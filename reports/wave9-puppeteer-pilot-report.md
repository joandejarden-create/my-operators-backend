# Wave 9 puppeteer pilot report

**Generated:** 2026-07-24T12:34:48.761Z
**Mode:** headless · Chrome channel · profile `data\wave9-chrome-profile`
**Pass:** 1/5
**Verdict:** mixed — keep steward browser-save as primary

| ID | Family | Hotel | Pass | Desc len | Amenities | Reasons |
|----|--------|-------|------|---------:|----------:|---------|
| bwh-70262 | bwh | Best Western Premier Monterrey Aeropuerto | YES | 130 | 10 | hotelDetails_blocked_or_http; meta_description_ok; amenities_ok |
| bwh-71032 | bwh | Aruba Boutique & Art Hotel BW Signature Collection | no | 0 | 0 | page_blocked; hotelDetails_blocked_or_http |
| bwh-76413 | bwh | BW Signature Collection Libre Hotel | no | 0 | 0 | page_blocked; hotelDetails_blocked_or_http |
| mar-sjuao | marriott | Hato Rey / Alma San Juan, Autograph Collection | no | 0 | 0 | page_blocked |
| mar-curak | marriott | The Pyrmont Curaçao, Autograph Collection | no | 0 | 0 | page_blocked |

## Implication

- Keep Choice/BWH/Marriott steward openers as primary; use puppeteer only where pass=true.

JSON: `reports/wave9-puppeteer-pilot-report.json`
