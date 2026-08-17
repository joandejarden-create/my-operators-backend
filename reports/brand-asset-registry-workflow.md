# Brand Asset Registry / Approval Workflow v2

Generated: 2026-07-07T20:02:32.491Z
Mode: **dry-run** · Airtable modified: **no**
Brand Setup media untouched: **yes**
Brand: Tribute Portfolio `recCvV0PuZOi8c3hC`
Governance report source: cached-report
Text/governance Platform Ready: **yes**

## 1. Registry table status

- Table exists: **yes** (`tblwNaf9DZt8Lth4t`)
- Existing records for brand: **27**

## 2. Record writer (v2)

| Metric | Count |
|--------|-------|
| Records proposed | 0 |
| Records created | 0 |
| Records skipped (duplicates) | 9 |


### Skipped duplicates

- Brand Setup — existing logo (unconfirmed) — `recCvV0PuZOi8c3hC|Logo|Brand Setup — existing logo (unconfirmed)|Brand Setup — Logo`
- Tribute Portfolio logo — tribute-black.svg (preferred confirmation candidate) — `recCvV0PuZOi8c3hC|Logo|https://tribute-portfolio.marriott.com/wp-content/uploads/2025/03/tribute-black.svg|Brand Setup — Logo`
- Tribute Portfolio hero — consumer property wide (preferred) — `recCvV0PuZOi8c3hC|Hero Image|https://tribute-portfolio.marriott.com/wp-content/uploads/2023/07/trbcl.1256725ws2880-1152-2560x1024.jpg|Brand Setup — Explorer Hero`
- Tribute property/design image 1 — `recCvV0PuZOi8c3hC|Exterior / Property|https://tribute-portfolio.marriott.com/wp-content/uploads/2023/07/trbcl.1256725ws2880-1152-1536x614.jpg|materials.gallery.1`
- Tribute property/design image 2 — `recCvV0PuZOi8c3hC|Exterior / Property|https://tribute-portfolio.marriott.com/wp-content/uploads/2023/07/trbcl.1256725ws2880-1152.jpg|materials.gallery.2`
- Tribute property/design image 3 — `recCvV0PuZOi8c3hC|Exterior / Property|https://tribute-portfolio.marriott.com/wp-content/uploads/2023/07/trbcl.1256725ws2880-1152-1136x454.jpg|materials.gallery.3`
- 2026 Tribute Portfolio FDD (reference) — `recCvV0PuZOi8c3hC|PDF / Brochure|2026 Tribute Portfolio FDD (reference)|Source Library Reference`
- Marriott newsroom — Tribute Portfolio PR/openings (placeholder) — `recCvV0PuZOi8c3hC|Press Link|https://news.marriott.com/brands/tribute-portfolio|PR / Recent Openings`
- Brand Setup — Mock/Demo hero (do not replace) — `recCvV0PuZOi8c3hC|Hero Image|Brand Setup — Mock/Demo hero (do not replace)|Brand Setup — Explorer Hero`

## 3. Existing asset/media schema

- Brand Asset Registry table: **exists**
- Source Library suitable for asset governance: **no** — Use dedicated Brand Asset Registry table — Source Library is for document/source provenance, not visual asset approval workflow.
- Brand Setup media fields: `Logo` (yes), `Explorer Hero Data Source` (yes), `Explorer Hero Verification` (yes)

### Presentation tables (image slots — not scanned for assets in v1)

- Brand Setup - Brand Explorer Presentation: exists (1 image-related fields)
- Brand Setup - Brand Explorer Materials: not found
- Brand Setup - Brand Explorer Footprint: not found

## 4. Staged Tribute asset candidates

Staging run: `brand-asset-staging-tribute-portfolio-1783454551556` · **9** records

| Asset | Type | Status | Primary | Slot |
|-------|------|--------|---------|------|
| Brand Setup — existing logo (unconfirmed) | Logo | Needs Usage Review | no | Brand Setup — Logo |
| Tribute Portfolio logo — tribute-black.svg (preferred confirmation candidate) | Logo | Candidate | yes | Brand Setup — Logo |
| Tribute Portfolio hero — consumer property wide (preferred) | Hero Image | Candidate | yes | Brand Setup — Explorer Hero |
| Tribute property/design image 1 | Exterior / Property | Candidate | yes | materials.gallery.1 |
| Tribute property/design image 2 | Exterior / Property | Candidate | no | materials.gallery.2 |
| Tribute property/design image 3 | Exterior / Property | Candidate | no | materials.gallery.3 |
| 2026 Tribute Portfolio FDD (reference) | PDF / Brochure | Source-Confirmed | yes | Source Library Reference |
| Marriott newsroom — Tribute Portfolio PR/openings (placeholder) | Press Link | Do Not Use | yes | PR / Recent Openings |
| Brand Setup — Mock/Demo hero (do not replace) | Hero Image | Mock/Demo | no | Brand Setup — Explorer Hero |

## 5. Needs usage review

- **Brand Setup — existing logo (unconfirmed)** → Brand Setup — Logo
- **Tribute Portfolio logo — tribute-black.svg (preferred confirmation candidate)** → Brand Setup — Logo — `https://tribute-portfolio.marriott.com/wp-content/uploads/2025/03/tribute-black.svg`
- **Tribute Portfolio hero — consumer property wide (preferred)** → Brand Setup — Explorer Hero — `https://tribute-portfolio.marriott.com/wp-content/uploads/2023/07/trbcl.1256725ws2880-1152-2560x1024.jpg`
- **Tribute property/design image 1** → materials.gallery.1 — `https://tribute-portfolio.marriott.com/wp-content/uploads/2023/07/trbcl.1256725ws2880-1152-1536x614.jpg`
- **Tribute property/design image 2** → materials.gallery.2 — `https://tribute-portfolio.marriott.com/wp-content/uploads/2023/07/trbcl.1256725ws2880-1152.jpg`
- **Tribute property/design image 3** → materials.gallery.3 — `https://tribute-portfolio.marriott.com/wp-content/uploads/2023/07/trbcl.1256725ws2880-1152-1136x454.jpg`
- **Marriott newsroom — Tribute Portfolio PR/openings (placeholder)** → PR / Recent Openings — `https://news.marriott.com/brands/tribute-portfolio`

## 6. Do Not Use / Mock-Demo

- **Marriott newsroom — Tribute Portfolio PR/openings (placeholder)** (Do Not Use) — news.marriott.com requires Rendered Source Capture v1 before PR links or press imagery can be governed.
- **Brand Setup — Mock/Demo hero (do not replace)** (Mock/Demo) — Mock/Demo placeholder — not brand-verified.

## 7. Requires future tooling

- **Marriott newsroom — Tribute Portfolio PR/openings (placeholder)** — news.marriott.com requires Rendered Source Capture v1 before PR links or press imagery can be governed.
- Rendered Source Capture v1: **yes**

## 8. Records apply command

_No new records to write — registry is up to date._

## 9. Next command

```bash
npm run brand-asset-registry-workflow -- --brand tribute-portfolio --dry-run
```

## 10. Remaining work before hero/logo promotion

- Human usage review on logo, hero, and gallery candidates in registry
- Confirm tribute-black.svg matches Brand Setup logo attachment
- Approve hero replacement only after rights review — Mock/Demo hero stays until promotion gate
- Rendered Source Capture v1 for Marriott newsroom PR
- Future v3: asset download + Explorer hero/logo promotion writer

## 11. Visual parity gap (Kimpton / Radisson Blu)

- Target: Verified logo, hero, 3–6 property/design images, PR/recent-opening links, governed asset statuses
- Tribute now: Text/governance Platform Ready; hero Mock/Demo; logo unconfirmed; no governed image package; PR not captured

**Remaining:**
- Confirm logo source and usage rights
- Replace Mock/Demo hero with Marriott-controlled candidate
- Capture 3–6 property/design/lifestyle images with rights metadata
- Rendered capture of Marriott newsroom for PR/recent openings
- Future v2: asset status fields + Explorer field writer with staging

## Does not do

- Download images or attach binary files
- Overwrite Brand Setup logo, hero, image, or attachment fields
- Replace Mock/Demo hero
- Mark assets Approved For Explorer Use automatically
- Set Company Validated or Company Validation Date
- Imply Marriott validated assets or profile
