# v46 Image Remediation — Small Luxury Hotels of the World

Slug: `small-luxury-hotels-of-the-world`

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

Section label: **SLH curated examples · Independent luxury consortium · Not a full directory**

## Accepted candidate counts

- Gallery: 6
- Property: 3 (CALA=1, US=0, global=2)
- Scenarios: 3

## Property example candidates

- **Coral Reef Club** · CALA · accepted=true · render=needs_materialization
  - source: https://slh.com/hotels/coral-reef-club
  - imageUrl: https://slh.com/-/media/slh/hotels/c/xl/coral-reef-club-78484-xl-1.jpg?h=1080&w=1920&rev=914c0471094d4db084081910510c2be
- **Quinta da Comporta** · global · accepted=true · render=needs_materialization
  - source: https://slh.com/hotels/quinta-da-comporta
  - imageUrl: https://slh.com/-/media/slh/hotels/q/xl/quinta-da-comporta-wellness-boutique-resort-3309-xl-2.jpg?h=1080&w=1920&rev=afa7
- **Hôtel San Régis** · global · accepted=true · render=needs_materialization
  - source: https://slh.com/hotels/hotel-san-regis
  - imageUrl: https://slh.com/-/media/slh/hotels/h/xl/hotel-san-regis-78718-xl-1.jpg?h=1080&w=1920&rev=62f04ed790114900b44234692ce88ac

## Gallery candidates (sample)

- `materials.gallery.1` · accepted=true · Coral Reef Club
- `materials.gallery.2` · accepted=true · Coral Reef Club
- `materials.gallery.3` · accepted=true · Coral Reef Club
- `materials.gallery.4` · accepted=true · Coral Reef Club
- `materials.gallery.5` · accepted=true · Coral Reef Club
- `materials.gallery.6` · accepted=true · Coral Reef Club

## Risks

- Wrong-brand rejects: 0
- Logo/generic rejects: 0
- Registry-only rejects: 0
- Missing URL rejects: 0

## Brand-specific notes

```json
{
  "consortiumLanguage": "independent_luxury_consortium",
  "noFranchiseLogic": true,
  "preferCalaIfOfficial": true,
  "sectionLabel": "SLH curated examples · Independent luxury consortium · Not a full directory",
  "calaSupportCount": 1
}
```

## Guardrails

- presentationWrites: false
- unlock: false
- activeRelease: false
- companyValidatedChanges: false
- releasedBrandWrites: false
