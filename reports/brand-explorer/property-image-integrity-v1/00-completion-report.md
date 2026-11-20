# Property Image Integrity — Research Source Separation

**Status:** BROWSER_VERIFIED (enforcement) · **NOT** founder-approved for publication  
**Date:** 2026-11-19  
**Stream:** Brand Explorer (`ws-brand-explorer`)  
**Curio / Ascend:** **NOT APPROVED**  
**Airtable writes:** 0

## Founder failure

Royal Palm Galápagos (`footprint.openings`) displayed a **Travel Weekly** publication graphic as the featured property photograph.

Root cause:

- Airtable attachment filename: `royal-palm-galapagos-curio-collection-by-hilton-santa-cruz-island-tw.jpg` (`-tw` = Travel Weekly)
- Atelier `propertyExampleCardFromBlock` used `imageUrl` verbatim
- No separation between research/article assets and property display images

## Fix (selection process, not a one-off URL patch)

| Layer | Change |
| --- | --- |
| Utility | `lib/brand-explorer/be-property-image-integrity-v1.js` + browser mirror |
| Reject rules | Publication filename markers (`-tw.jpg`), trade-press hosts, logos, article thumbnails |
| Separation | Research/source URLs retained in body; display image is independent |
| Verified replacement | Hilton Stories official aerial for Royal Palm Galápagos |
| RIGHT assembly | `buildCumulativeProposedBrand` applies integrity after Title Case |
| Atelier | Prefers `__propertyImageIntegrity.replacementUrl`; research-required empty state |
| Compare Studio | LEFT = current Travel Weekly graphic unchanged; RIGHT = verified aerial; Highlight marks MODIFIED |
| Contract | `fixedCollections.propertyFacingImages` |
| Lesson | `BE-QL-PROPERTY-IMAGE-001` |
| Gate | `npm run test:be-property-image-integrity-v1` |
| Fixture | `fixtures/brand-explorer-property-image-integrity/royal-palm-travel-weekly-publication-graphic.json` |

## Verified replacement (RIGHT only)

- **Image:** Hilton Stories aerial courtesy Royal Palm Galápagos  
  `…/Royal-Palm-Galapagos-Curio-Collection-by-Hilton_Aerial_Courtesy-of-Royal-Palm-Galapagos-Curio-Collection-by-Hilton.jpg`
- **Caption:** `Royal Palm Galápagos | Hotel & Guest Villas`
- **Source citation retained:** Hilton / hotel page URLs in card body (research evidence, not display image)

## Browser evidence (Compare Studio)

URL: `http://127.0.0.1:8082/brand-explorer-compare.html?beInternalPreview=1&brand=receQkxgjlezsc1xg`

| Side | Observed |
| --- | --- |
| LEFT (CURRENT) | Travel Weekly masthead / publication graphic (saved Airtable attachment) |
| RIGHT (PROPOSED) | Official highland-estate aerial of Royal Palm Galápagos |
| Highlight ON | Image replacement marked **MODIFIED** |
| Highlight OFF | Clean proposed customer-facing card with verified photo + caption |

Screenshots:

- `screenshots/01-curio-royal-palm-LEFT-vs-RIGHT-hl-on.png`
- `screenshots/02-curio-royal-palm-RIGHT-clean-hl-off.png`

## Explicit non-claims

- Not FOUNDER_APPROVED / not publication-ready
- No production Airtable writes
- No mass population
- Curio and Ascend remain **NOT APPROVED**
- Universal reject rules apply to Ascend and future brands; Royal Palm replacement is a verified exact-property fill for a known failing identity
