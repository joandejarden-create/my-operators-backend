# v47 Image Materialization — MGallery Collection

Slug: `mgallery-collection`

## Eligibility

- Status: **build_draft_ready**
- image_remediation_complete: true
- materialization_plan_ready: true
- build_draft_ready: true
- apply_draft_allowed (projected): true
- Rationale: Materialization plan projects 6/6 gallery + 3/3 property + 3/3 scenario imageUrls. External profile remains locked until founder/active release. apply_draft may proceed in a later gated stage after Presentation Image writes.

## Live API (pre-materialization)

- Gallery: **0** / 6
- Property examples: **0** / 3
- Scenarios: **0** / 3

Section label: **MGallery Collection property examples · Prefer CALA Accor member hotels**

## Render readiness projection

- Gallery: 6/6
- Property: 3/3
- Scenarios: 3/3
- Pass: **true**
- External remains locked: **true**

## Planned rows

- **materials.gallery.1** · Exterior / Arrival - Palladio Hotel Buenos Aires MGallery Collection · imageOk=true · record=CREATE
  - imageUrl: https://www.ahstatic.com/photos/b1r7_ho_00_p_1024x768.jpg
  - sourcePageUrl (internal): https://all.accor.com/hotel/B1R7/index.en.shtml
- **materials.gallery.2** · Guest Room / Suite - Hotel Costanero Montevideo - MGallery Collection · imageOk=true · record=CREATE
  - imageUrl: https://www.ahstatic.com/photos/b3g3_ho_00_p_1024x768.jpg
  - sourcePageUrl (internal): https://all.accor.com/hotel/B3G3/index.en.shtml
- **materials.gallery.3** · Public Space - Santa Teresa Hotel RJ - MGallery Collection · imageOk=true · record=CREATE
  - imageUrl: https://www.ahstatic.com/photos/a1x5_ho_00_p_1024x768.jpg
  - sourcePageUrl (internal): https://all.accor.com/hotel/A1X5/index.en.shtml
- **materials.gallery.4** · F&B or Local Experience - Palladio Hotel Buenos Aires MGallery Collection · imageOk=true · record=CREATE
  - imageUrl: https://www.ahstatic.com/photos/b1r7_ho_00_p_1024x768.jpg
  - sourcePageUrl (internal): https://all.accor.com/hotel/B1R7/index.en.shtml
- **materials.gallery.5** · Design Detail - Hotel Costanero Montevideo - MGallery Collection · imageOk=true · record=CREATE
  - imageUrl: https://www.ahstatic.com/photos/b3g3_ho_00_p_1024x768.jpg
  - sourcePageUrl (internal): https://all.accor.com/hotel/B3G3/index.en.shtml
- **materials.gallery.6** · Property Setting - Santa Teresa Hotel RJ - MGallery Collection · imageOk=true · record=CREATE
  - imageUrl: https://www.ahstatic.com/photos/a1x5_ho_00_p_1024x768.jpg
  - sourcePageUrl (internal): https://all.accor.com/hotel/A1X5/index.en.shtml
- **footprint.openings.1** · Palladio Hotel Buenos Aires MGallery Collection — CALA Property Example · imageOk=true · record=CREATE
  - imageUrl: https://www.ahstatic.com/photos/b1r7_ho_00_p_1024x768.jpg
  - sourcePageUrl (internal): https://all.accor.com/hotel/B1R7/index.en.shtml
- **footprint.openings.2** · Hotel Costanero Montevideo - MGallery Collection — CALA Property Example · imageOk=true · record=CREATE
  - imageUrl: https://www.ahstatic.com/photos/b3g3_ho_00_p_1024x768.jpg
  - sourcePageUrl (internal): https://all.accor.com/hotel/B3G3/index.en.shtml
- **footprint.openings.3** · Santa Teresa Hotel RJ - MGallery Collection — CALA Property Example · imageOk=true · record=CREATE
  - imageUrl: https://www.ahstatic.com/photos/a1x5_ho_00_p_1024x768.jpg
  - sourcePageUrl (internal): https://all.accor.com/hotel/A1X5/index.en.shtml
- **overview.scenario.1** · Distinctive Local Character · imageOk=true · record=CREATE
  - imageUrl: https://www.ahstatic.com/photos/b1r7_ho_00_p_1024x768.jpg
  - sourcePageUrl (internal): https://all.accor.com/hotel/B1R7/index.en.shtml
- **overview.scenario.2** · Soft-Collection Conversion · imageOk=true · record=CREATE
  - imageUrl: https://www.ahstatic.com/photos/b3g3_ho_00_p_1024x768.jpg
  - sourcePageUrl (internal): https://all.accor.com/hotel/B3G3/index.en.shtml
- **overview.scenario.3** · CALA Collection Reference · imageOk=true · record=CREATE
  - imageUrl: https://www.ahstatic.com/photos/a1x5_ho_00_p_1024x768.jpg
  - sourcePageUrl (internal): https://all.accor.com/hotel/A1X5/index.en.shtml

## Brand-specific notes

```json
{
  "ahstaticPropertyOnly": true,
  "sectionLabel": "MGallery Collection property examples · Prefer CALA Accor member hotels"
}
```

## Guardrails

- presentationImageWritesPlanned: true (apply-gated)
- sourceLibraryWrites: false
- unlock: false
- activeRelease: false
- companyValidatedChanges: false
- releasedBrandWrites: false
- rawUrlsInOwnerFacingCopy: false
