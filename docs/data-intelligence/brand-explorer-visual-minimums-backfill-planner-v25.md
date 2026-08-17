# Brand Explorer Visual Minimums & Image Backfill Planner v25

- Generated: 2026-07-09T07:59:04.315Z
- Mode: **dry-run** · Airtable modified: **no**
- Brand: **Tribute Portfolio** (`recCvV0PuZOi8c3hC`)

## Visual Coverage By Section
- **Hero**: required=1, current=1, missing=0, belowMinimum=no
- **Where This Brand Creates the Most Value**: required=3, current=2, missing=1, belowMinimum=yes
- **Image Gallery**: required=6, current=5, missing=1, belowMinimum=yes
- **Openings / Examples / Properties**: required=1, current=0, missing=1, belowMinimum=yes
- **Recent Momentum**: required=0, current=0, missing=1, belowMinimum=yes

## Missing Slots
- overview.scenario.3
- materials.gallery.3
- footprint.openings
- footprint.momentum

## Recommended Assignments
- `materials.gallery.3` ← Ermita, Cartagena, a Tribute Portfolio Hotel — Image Gallery 3 (gallery) (`recxVPbTlsrP9v4bQ`) · approved · confidence=medium

## Suppress Until Ready
- Hide scenario card image placeholder for overview.scenario.3 until asset is ready.
- Hide materials.gallery.3 slot until a valid image is assigned.
- Suppress Openings / Examples / Properties section until complete cards (image/title/location/summary/link) exist.
- Suppress Recent Momentum when no dated source-backed activity rows exist.

## Source Capture Tasks
- `overview.scenario.3`: Capture source-backed Image visual for overview.scenario.3 from Marriott/company-controlled property pages.
- `footprint.openings`: Capture property-specific opening example with image, location/descriptor, teaser, and property/source URL.
- `footprint.momentum`: Capture dated momentum rows with source-backed announcement URLs.

## v25B Candidate Write Set
- `materials.gallery.3` → `recxVPbTlsrP9v4bQ` (Ermita, Cartagena, a Tribute Portfolio Hotel — Image Gallery 3 (gallery))

## Next Command
```bash
npm run brand-explorer-visual-minimums-backfill-planner -- --brand tribute-portfolio --dry-run
```