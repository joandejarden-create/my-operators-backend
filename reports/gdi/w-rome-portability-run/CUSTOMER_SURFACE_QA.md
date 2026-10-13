# W Rome — Customer surface QA

## Contract
- Bethesda-style opportunity cards via `opportunityTileHtml` in `public/js/group-demand-intelligence/dealality-gdi-ui.js`
- Demand Campaigns / Generators: **hidden** on customer surface
- Sections: Customer-Ready Opportunities + Future Watch

## Counts (live gates)
| Metric | Value |
|---|---|
| filterCustomerFacingOpportunities | 6 |
| Ready (isGdiCustomerOpportunityReady) | 6 |
| Future Watch (isValidFutureWatch) | 7 |
| Customer-visible campaigns | 0 (must be 0) |
| Internal visible generators | 7 |

## Browser QA (2026-10-04)
| Check | Result |
|---|---|
| Property selector shows W Rome | PASS — `W Rome — Rome` |
| Ready / facing count | PASS — Showing 6 of 6 |
| Watch filter | PASS — Watch (6) |
| Bethesda opportunity tiles | PASS — tile grid |
| Buyer / contact on cards | PASS — role + org path URLs |
| Dates / timing | PASS — Oct 2026 / 2027 windows |
| Lodging evidence | PASS — overflow / housing notes |
| Next action | PASS — QUALIFY + View Details |
| Source links | PASS |
| Filters | PASS |
| Demand Campaigns visible | PASS — none |
| Empty state | N/A (data present) |

## Notes
Generators remain internal. Same Bethesda card component as YOTEL / Bethesda.
Run id: `gdi_w_rome_45bee0`
