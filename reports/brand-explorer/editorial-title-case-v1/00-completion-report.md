# Editorial Title Case — Incomplete Enforcement Fix

**Status:** BROWSER_VERIFIED (enforcement) · **NOT** founder-approved for publication  
**Date:** 2026-11-19  
**Stream:** Brand Explorer (`ws-brand-explorer`)  
**Curio / Ascend:** **NOT APPROVED**  
**Airtable writes:** 0

## Root cause

White card headings (especially `overview.proof.*`) are rendered from Presentation `title` fields **verbatim**.

- CURRENT / Gate1 payload stores sentence-case titles (`Global soft-collection scale`, …).
- Cumulative assembler retained CURRENT titles onto RIGHT without editorial normalization.
- Prior contracts/tests asserted Title Case policy but did **not** enforce it on the proposed profile path.
- CSS `text-transform` was never the source of correctness (and is not used as such).

## Fix

| Layer | Change |
| --- | --- |
| Utility | `lib/brand-explorer/be-editorial-title-case-v1.js` + browser mirror |
| RIGHT assembly | `buildCumulativeProposedBrand` applies Title Case to semantic heading titles only |
| Atelier | `proposedHeading(brand, …)` when `beCumulativeProposal` (defense in depth) |
| LEFT | Unchanged / read-only — saved Airtable content not rewritten |
| Contract | `typographyAndWriting.cardHeadings` + sectionHeading enforcer pointers |
| Lesson | `BE-QL-TITLECASE-001` |
| Gate | `npm run test:be-editorial-title-case-v1` |
| Fixture | `fixtures/brand-explorer-title-case-regressions/failing-founder-screenshot-headings.json` |

## Browser evidence (Compare Studio)

URL: `http://127.0.0.1:8082/brand-explorer-compare.html?beInternalPreview=1`

### Curio proof cards

| Side | Titles |
| --- | --- |
| LEFT (CURRENT) | Global soft-collection scale · Conversion-weighted growth · Curio since 2014 |
| RIGHT (PROPOSED) | **Global Soft-Collection Scale** · **Conversion-Weighted Growth** · **Curio Since 2014** |

Highlight Changes ON marks the capitalization deltas as **MODIFIED** (yellow), without rewriting body prose.

### Curio commercial levers (RIGHT)

Distribution · Revenue Management · Digital Marketing · Corporate Group · Leisure Destination · International — Title Case present; LEFT retains prior sentence-case variants where stored.

### Ascend proof cards (RIGHT)

Collection Scale Context · Direct Booking Emphasis · Local Character Preserved · … — no residual non–Title-Case semantic headlines detected.

Screenshots:

- `screenshots/01-curio-proof-titles-titlecase-RIGHT.png`
- `screenshots/02-curio-commercial-lever-titles.png`

## Explicit non-claims

- Not FOUNDER_APPROVED / not publication-ready
- No production Airtable writes
- Gallery captions excluded from Title Case rewrite (hotel\|scene lines)
- Body paragraphs and quotations are not Title-Cased
