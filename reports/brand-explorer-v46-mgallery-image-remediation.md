# v46 Image Remediation — MGallery Collection

Slug: `mgallery-collection`

## Eligibility

- Status: **asset_pack_ready**
- image_remediation_complete: true
- asset_pack_ready: true
- build_draft_ready: false
- apply_draft_allowed: false (always false in v46)
- Rationale: Candidate pack meets 6/3/3 with accepted property-specific images; Presentation imageUrl materialization still required before draft apply.

## Live API imageUrl counts

- Gallery: **0** / 6
- Property examples: **0** / 3
- Scenarios: **0** / 3

Section label: **MGallery Collection property examples · Prefer CALA Accor member hotels**

## Accepted candidate counts

- Gallery: 6
- Property: 3 (CALA=3, US=0, global=0)
- Scenarios: 3

## Property example candidates

- **Palladio Hotel Buenos Aires MGallery Collection** · CALA · accepted=true · render=needs_materialization
  - source: https://all.accor.com/hotel/B1R7/index.en.shtml
  - imageUrl: https://www.ahstatic.com/photos/b1r7_ho_00_p_1024x768.jpg
- **Hotel Costanero Montevideo - MGallery Collection** · CALA · accepted=true · render=needs_materialization
  - source: https://all.accor.com/hotel/B3G3/index.en.shtml
  - imageUrl: https://www.ahstatic.com/photos/b3g3_ho_00_p_1024x768.jpg
- **Santa Teresa Hotel RJ - MGallery Collection** · CALA · accepted=true · render=needs_materialization
  - source: https://all.accor.com/hotel/A1X5/index.en.shtml
  - imageUrl: https://www.ahstatic.com/photos/a1x5_ho_00_p_1024x768.jpg

## Gallery candidates (sample)

- `materials.gallery.1` · accepted=true · Palladio Hotel Buenos Aires MGallery Collection
- `materials.gallery.2` · accepted=true · Hotel Costanero Montevideo - MGallery Collection
- `materials.gallery.3` · accepted=true · Santa Teresa Hotel RJ - MGallery Collection
- `materials.gallery.4` · accepted=true · Palladio Hotel Buenos Aires MGallery Collection
- `materials.gallery.5` · accepted=true · Hotel Costanero Montevideo - MGallery Collection
- `materials.gallery.6` · accepted=true · Santa Teresa Hotel RJ - MGallery Collection

## Risks

- Wrong-brand rejects: 0
- Logo/generic rejects: 0
- Registry-only rejects: 0
- Missing URL rejects: 0

## Brand-specific notes

```json
{
  "validateAccorPropertySpecific": true,
  "rejectGenericAccorGraphics": true,
  "sectionLabel": "MGallery Collection property examples · Prefer CALA Accor member hotels",
  "towardAssetPackReady": true
}
```

## Guardrails

- presentationWrites: false
- unlock: false
- activeRelease: false
- companyValidatedChanges: false
- releasedBrandWrites: false
