# v47 Image Materialization — Hotel Indigo

Slug: `hotel-indigo`

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

Section label: **Hotel Indigo property examples · Prefer CALA; expand U.S./global if needed**

## Render readiness projection

- Gallery: 6/6
- Property: 3/3
- Scenarios: 3/3
- Pass: **true**
- External remains locked: **true**

## Planned rows

- **materials.gallery.1** · Exterior / Arrival - Hotel Indigo Guanajuato · imageOk=true · record=CREATE
  - imageUrl: https://digital.ihg.com/is/image/ihg/hotel-indigo-guanajuato-6199618329-2x1
  - sourcePageUrl (internal): https://www.ihg.com/hotelindigo/hotels/us/en/guanajuato/bjxgd/hoteldetail
- **materials.gallery.2** · Guest Room / Suite - Hotel Indigo Guadalajara Expo · imageOk=true · record=CREATE
  - imageUrl: https://digital.ihg.com/is/image/ihg/hotel-indigo-guadalajara-9196830737-2x1
  - sourcePageUrl (internal): https://www.ihg.com/hotelindigo/hotels/us/en/guadalajara/gdlal/hoteldetail
- **materials.gallery.3** · Public Space - Hotel Indigo Lima Miraflores · imageOk=true · record=CREATE
  - imageUrl: https://digital.ihg.com/is/image/ihg/hotel-indigo-lima-10477383316-2x1
  - sourcePageUrl (internal): https://www.ihg.com/hotelindigo/hotels/us/en/lima/limmd/hoteldetail
- **materials.gallery.4** · F&B or Local Experience - Hotel Indigo Guanajuato · imageOk=true · record=CREATE
  - imageUrl: https://digital.ihg.com/is/image/ihg/hotel-indigo-guanajuato-6199618377-2x1
  - sourcePageUrl (internal): https://www.ihg.com/hotelindigo/hotels/us/en/guanajuato/bjxgd/hoteldetail
- **materials.gallery.5** · Design Detail - Hotel Indigo Guadalajara Expo · imageOk=true · record=CREATE
  - imageUrl: https://digital.ihg.com/is/image/ihg/hotel-indigo-guadalajara-9244720494-2x1
  - sourcePageUrl (internal): https://www.ihg.com/hotelindigo/hotels/us/en/guadalajara/gdlal/hoteldetail
- **materials.gallery.6** · Property Setting - Hotel Indigo Lima Miraflores · imageOk=true · record=CREATE
  - imageUrl: https://digital.ihg.com/is/image/ihg/hotel-indigo-lima-10320070053-2x1
  - sourcePageUrl (internal): https://www.ihg.com/hotelindigo/hotels/us/en/lima/limmd/hoteldetail
- **footprint.openings.1** · Hotel Indigo Guanajuato — CALA Property Example · imageOk=true · record=CREATE
  - imageUrl: https://digital.ihg.com/is/image/ihg/hotel-indigo-guanajuato-6199618329-2x1
  - sourcePageUrl (internal): https://www.ihg.com/hotelindigo/hotels/us/en/guanajuato/bjxgd/hoteldetail
- **footprint.openings.2** · Hotel Indigo Guadalajara Expo — CALA Property Example · imageOk=true · record=CREATE
  - imageUrl: https://digital.ihg.com/is/image/ihg/hotel-indigo-guadalajara-9196830737-2x1
  - sourcePageUrl (internal): https://www.ihg.com/hotelindigo/hotels/us/en/guadalajara/gdlal/hoteldetail
- **footprint.openings.3** · Hotel Indigo Lima Miraflores — CALA Property Example · imageOk=true · record=CREATE
  - imageUrl: https://digital.ihg.com/is/image/ihg/hotel-indigo-lima-10477383316-2x1
  - sourcePageUrl (internal): https://www.ihg.com/hotelindigo/hotels/us/en/lima/limmd/hoteldetail
- **overview.scenario.1** · Neighborhood Boutique Lifestyle · imageOk=true · record=CREATE
  - imageUrl: https://digital.ihg.com/is/image/ihg/hotel-indigo-guanajuato-6199618329-2x1
  - sourcePageUrl (internal): https://www.ihg.com/hotelindigo/hotels/us/en/guanajuato/bjxgd/hoteldetail
- **overview.scenario.2** · Conversion / Repositioning · imageOk=true · record=CREATE
  - imageUrl: https://digital.ihg.com/is/image/ihg/hotel-indigo-guadalajara-9196830737-2x1
  - sourcePageUrl (internal): https://www.ihg.com/hotelindigo/hotels/us/en/guadalajara/gdlal/hoteldetail
- **overview.scenario.3** · Urban Leisure–Business Mix · imageOk=true · record=CREATE
  - imageUrl: https://digital.ihg.com/is/image/ihg/hotel-indigo-lima-10477383316-2x1
  - sourcePageUrl (internal): https://www.ihg.com/hotelindigo/hotels/us/en/lima/limmd/hoteldetail

## Brand-specific notes

```json
{
  "scene7Only": true,
  "marshaCodes": [
    "BJXGD",
    "GDLAL",
    "LIMMD",
    "BNAUS"
  ],
  "calaPropertyCount": 3,
  "sectionLabel": "Hotel Indigo property examples · Prefer CALA; expand U.S./global if needed"
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
