# v46 Image Remediation — Hotel Indigo

Slug: `hotel-indigo`

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

Section label: **Hotel Indigo property examples · Prefer CALA; expand U.S./global if needed**

## Accepted candidate counts

- Gallery: 6
- Property: 3 (CALA=3, US=0, global=0)
- Scenarios: 3

## Property example candidates

- **Hotel Indigo Guanajuato** · CALA · accepted=true · render=needs_materialization
  - source: https://www.ihg.com/hotelindigo/hotels/us/en/guanajuato/bjxgd/hoteldetail
  - imageUrl: https://digital.ihg.com/is/image/ihg/hotel-indigo-guanajuato-6199618329-2x1
- **Hotel Indigo Guadalajara Expo** · CALA · accepted=true · render=needs_materialization
  - source: https://www.ihg.com/hotelindigo/hotels/us/en/guadalajara/gdlal/hoteldetail
  - imageUrl: https://digital.ihg.com/is/image/ihg/hotel-indigo-guadalajara-9196830737-2x1
- **Hotel Indigo Lima Miraflores** · CALA · accepted=true · render=needs_materialization
  - source: https://www.ihg.com/hotelindigo/hotels/us/en/lima/limmd/hoteldetail
  - imageUrl: https://digital.ihg.com/is/image/ihg/hotel-indigo-lima-10477383316-2x1

## Gallery candidates (sample)

- `materials.gallery.1` · accepted=true · Hotel Indigo Guanajuato
- `materials.gallery.2` · accepted=true · Hotel Indigo Guadalajara Expo
- `materials.gallery.3` · accepted=true · Hotel Indigo Lima Miraflores
- `materials.gallery.4` · accepted=true · Hotel Indigo Guanajuato
- `materials.gallery.5` · accepted=true · Hotel Indigo Guadalajara Expo
- `materials.gallery.6` · accepted=true · Hotel Indigo Lima Miraflores

## Risks

- Wrong-brand rejects: 0
- Logo/generic rejects: 0
- Registry-only rejects: 0
- Missing URL rejects: 0

## Brand-specific notes

```json
{
  "rejectGenericIhgHero": true,
  "rejectInterContinental": true,
  "calaFirst": true,
  "sectionLabel": "Hotel Indigo property examples · Prefer CALA; expand U.S./global if needed",
  "calaSupportCount": 3,
  "expansionNeeded": false
}
```

## Guardrails

- presentationWrites: false
- unlock: false
- activeRelease: false
- companyValidatedChanges: false
- releasedBrandWrites: false
