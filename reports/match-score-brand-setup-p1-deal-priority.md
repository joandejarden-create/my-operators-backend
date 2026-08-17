# Match Score — P1 Brand Setup priority (brands on deals)

Generated: 2026-07-23T18:37:12.789Z

P1 fill priority = brands on live deal preferred lists from last cache refresh sample. Fill only from founder (A) or existing docs (B). Engine now excludes null soft factors from denominator (cache model v3).

Distinct preferred on deals: **17** · Matched Active/Live: **4** · Incomplete P1: **0** · Not in Active/Live: **13**

## Active/Live brands on deals

| Brand | Deals | P1 complete | Fill % | Remaining required | Gaps |
| --- | ---: | --- | ---: | ---: | --- |
| Curio Collection by Hilton | 4 | yes | 100 | 0 | — |
| Autograph Collection | 2 | yes | 100 | 0 | — |
| Vignette Collection | 1 | yes | 100 | 0 | — |
| Kimpton Hotels | 1 | yes | 100 | 0 | — |

## Preferred on deals but not Active/Live (or name mismatch)

| Preferred name on deal | Deals | Note |
| --- | ---: | --- |
| Unbound Collection | 2 | Not in Active/Live Brand Basics universe (or name mismatch) — score may stay thin until brand is Active/Live + Setup complete |
| Hyatt Unbound Collection | 1 | Not in Active/Live Brand Basics universe (or name mismatch) — score may stay thin until brand is Active/Live + Setup complete |
| Tapestry Collection | 1 | Not in Active/Live Brand Basics universe (or name mismatch) — score may stay thin until brand is Active/Live + Setup complete |
| Anantara | 1 | Not in Active/Live Brand Basics universe (or name mismatch) — score may stay thin until brand is Active/Live + Setup complete |
| Sheraton Hotel | 1 | Not in Active/Live Brand Basics universe (or name mismatch) — score may stay thin until brand is Active/Live + Setup complete |
| Andaz | 1 | Not in Active/Live Brand Basics universe (or name mismatch) — score may stay thin until brand is Active/Live + Setup complete |
| MGallery Hotel Collection | 1 | Not in Active/Live Brand Basics universe (or name mismatch) — score may stay thin until brand is Active/Live + Setup complete |
| Thompson Hotels | 1 | Not in Active/Live Brand Basics universe (or name mismatch) — score may stay thin until brand is Active/Live + Setup complete |
| Unbound Collection by Hyatt | 1 | Not in Active/Live Brand Basics universe (or name mismatch) — score may stay thin until brand is Active/Live + Setup complete |
| Hyatt Centric | 1 | Not in Active/Live Brand Basics universe (or name mismatch) — score may stay thin until brand is Active/Live + Setup complete |
| Moxy | 1 | Not in Active/Live Brand Basics universe (or name mismatch) — score may stay thin until brand is Active/Live + Setup complete |
| Luxury Collection | 1 | Not in Active/Live Brand Basics universe (or name mismatch) — score may stay thin until brand is Active/Live + Setup complete |
| Sofitel Legend | 1 | Not in Active/Live Brand Basics universe (or name mismatch) — score may stay thin until brand is Active/Live + Setup complete |

## Next actions

1. Fill remaining required gaps for incomplete Active/Live brands above (A/B only).
2. For preferred brands not Active/Live: either activate + complete Brand Setup, or leave scores as insufficient/thin.
3. `npm run refresh-deal-brand-cache-active-brands` after deploy (cache model v3).
4. Spot-check View Details: Gate → Mismatch → Missing → Fit; insufficient-data when scored weight < 40%.
