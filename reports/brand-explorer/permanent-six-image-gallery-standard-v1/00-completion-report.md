# Permanent Six-Image Hotel Gallery Standard — Evidence

**Status:** ENCODED + BROWSER-CHECKED (not founder-approved)  
**Date:** 2026-11-19  
**Stream:** Brand Explorer (`ws-brand-explorer`)  
**Curio / Ascend approval:** **NOT APPROVED**  
**Airtable writes:** 0  
**Publication:** none

## Founder correction addressed

Reconstructed galleries showed:

- Brand logo as a gallery card (`Curio-Collection-by-Hilton.webp`)
- Generic captions (`Property imagery`)
- Loss of hotel / scene / location meaning

## Permanent standard

- Module: `lib/brand-explorer/be-permanent-six-image-gallery-standard-v1.js`
- Contract: `configs/brand-explorer-permanent-content-standards-contract-v1.json#fixedCollections.imageGallery`
- Lesson: `BE-QL-GALLERY-001` in `configs/brand-explorer-founder-quality-lessons-registry-v1.json`
- Gate: `npm run test:be-permanent-six-image-gallery-standard-v1`

## Enforcement wired into

- Cumulative proposal assembler (lib + browser mirror) — never overwrite verified CURRENT hotel photos with logos / generic captions
- Phase 2F visual gates — logo + generic caption failures
- Phase 2F presentation repair — no `Property imagery` stamp
- Phase 2G sanitize — derive hotel|scene captions; reject logos
- Phase 2H `composeMaterials` — filter logos; require derivable captions

## Browser evidence (Comparison Studio `http://127.0.0.1:8082`)

| Check | Curio | Ascend |
| --- | --- | --- |
| Six gallery cards present LEFT + RIGHT | PASS | PASS |
| Images load (naturalWidth > 0) | PASS | PASS |
| Brand logo as gallery card | ABSENT | ABSENT |
| Generic “Property imagery” caption | ABSENT | ABSENT |
| Meaningful hotel/scene captions | PASS (CURRENT retained) | PASS (CURRENT retained) |
| materials.gallery.1–6 provenance | unchanged / retain CURRENT | unchanged / retain CURRENT |
| Highlight ON marks all six as changed | NO (0 gallery markers) | N/A (retained) |

Screenshots:

- `screenshots/01-curio-materials-gallery-highlight-OFF.png`
- `screenshots/02-curio-materials-gallery-highlight-ON.png`
- `screenshots/03-ascend-materials-gallery-retained-current.png`

## Notes

- RIGHT column is the cumulative publication proposal: verified CURRENT hotel photography is retained when staged candidates fail the gallery standard.
- Staged Curio candidate gallery captions were repaired for slots 2–6; slot 1 logo removed (blocked) rather than fabricated.
- Ascend staged candidate gallery was entirely logo/social stubs — rejected; CURRENT Ascend hotel photos retained.
- Some live CURRENT Curio caption↔scene mismatches pre-exist in saved Airtable content; this task did not rewrite live Presentation.
- Gallery requirements are **not** marked FOUNDER_APPROVED / VERIFIED for publication.
