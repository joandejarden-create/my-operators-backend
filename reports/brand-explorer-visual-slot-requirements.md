# Brand Explorer Visual Slot Requirements v2

Generated: 2026-07-07T20:02:30.496Z
Mode: **dry-run** · Airtable modified: **no**
Brand: Tribute Portfolio `recCvV0PuZOi8c3hC`
Text/governance Platform Ready: **yes** (unchanged by this module)
Brand Setup media untouched: **yes**

## 1. Brand record

- Brand: Tribute Portfolio `recCvV0PuZOi8c3hC`
- Parent: Marriott International, Inc.
- Registry records audited: **27**

## 2. Visual slot definitions

### Logo
- Section: `Brand Setup — Logo`
- Purpose: Confirms brand identity.
- Requirements:
  - Official brand logo or existing Brand Setup logo confirmed against official source.
  - Does not need hotel/property context.
  - Must still pass usage/source review.
- Required checks: brandMatch, sourceControlled, notThirdPartyPrimary, usageReviewComplete

### Hero Image
- Section: `overview.hero / Brand Setup — Explorer Hero`
- Purpose: Primary brand-level visual.
- Requirements:
  - Real hotel/property image.
  - Brand-confirmed or property-confirmed as Tribute Portfolio.
  - Prefer CALA-relevant for CALA-focused profile.
  - Not abstract, generic lifestyle, non-hotel, or unrelated global imagery.
  - Official Marriott-controlled source or approved local reference source.
- Required checks: brandMatch, propertyMatch, sourceControlled, notMockDemo, notGenericLifestyle, notAbstract, notThirdPartyPrimary, usageReviewComplete

### Image Gallery
- Section: `materials.gallery.1–6`
- Purpose: Shows a collection of different real hotels under the brand.
- Requirements:
  - Multiple different Tribute Portfolio hotels.
  - Each image tied to a named property where possible.
  - Prefer mix of exterior, guestroom, public space, restaurant/bar/lifestyle.
  - Prefer CALA or region-relevant examples where possible.
  - Avoid duplicate crops from the same image unless intentional.
- Required checks: brandMatch, propertyMatch, sourceControlled, notMockDemo, notAbstract, notThirdPartyPrimary, usageReviewComplete

### Recent Openings
- Section: `footprint.openings`
- Purpose: Shows proof of current brand activity.
- Requirements:
  - Tied to the specific hotel/opening being referenced.
  - Image of that hotel or directly from the official opening/PR source.
  - Requires property name, opening/announcement date, source URL, region/country.
  - Generic brand images are not acceptable.
- Required checks: brandMatch, propertyMatch, openingMatch, hasNamedProperty, hasRegionOrCountry, hasSourcePageContext, sourceControlled, notThirdPartyPrimary, usageReviewComplete

### Where This Brand Creates the Most Value
- Section: `overview.why_value`
- Purpose: Visualizes the specific value driver shown.
- Requirements:
  - Image must match the value driver.
  - Urban → urban hotel/property context.
  - Resort → resort/leisure property context.
  - Conversion/adaptive reuse → conversion, repositioning, independent, or adaptive-reuse context.
  - Boutique/lifestyle → design-led or lifestyle hotel context.
  - Mixed-use → mixed-use/urban lifestyle context.
  - CALA relevance preferred where profile is CALA-focused.
  - Generic imagery is not acceptable.
- Required checks: brandMatch, propertyMatch, valueDriverMatch, sourceControlled, notMockDemo, notGenericLifestyle, notAbstract, notThirdPartyPrimary, usageReviewComplete

### Brand Standards / Owner Considerations
- Section: `materials.file / Source Library Reference`
- Purpose: Supports owner/developer understanding.
- Requirements:
  - Can be a PDF/brochure/source attachment, diagram, standards excerpt, or official source reference.
  - Does not necessarily require hotel photography.
  - Must be clearly source-backed and approved for display or internal reference.
- Required checks: sourceControlled, notThirdPartyPrimary

### PR / Opening Link
- Section: `PR / Recent Openings`
- Purpose: Links to official news/source evidence.
- Requirements:
  - Official brand/company source preferred.
  - JS-shell pages provenance-only until rendered capture exists.
  - Third-party PR context clearly marked; not used as company-materials evidence.
- Required checks: brandMatch, sourceControlled, notThirdPartyPrimary, hasSourcePageContext

## 3. Validation checks

- `brandMatch`
- `propertyMatch`
- `slotMatch`
- `geographyMatch`
- `valueDriverMatch`
- `openingMatch`
- `sourceControlled`
- `usageReviewComplete`
- `notMockDemo`
- `notGenericLifestyle`
- `notAbstract`
- `notThirdPartyPrimary`
- `hasNamedProperty`
- `hasRegionOrCountry`
- `hasSourcePageContext`

## 4. Registry schema sufficiency

- Table: `Partner Intelligence - Brand Asset Registry` (`tblwNaf9DZt8Lth4t`)
- Existing fields: 37
- Sufficient for slot governance: **yes**

### Proposed new fields

- **Explorer Section** (singleLineText) — Which Explorer section/slot key this asset targets.
- **Slot Purpose** (singleLineText) — Human-readable slot purpose.
- **Related Value Driver** (singleSelect) — Urban / Resort / Conversion / Boutique-Lifestyle / Mixed-Use.
- **Related Property Name** (singleLineText) — Named property this image depicts.
- **Related Opening / PR** (singleLineText) — Opening/announcement reference for footprint.openings.
- **Country / Region** (singleLineText) — Geography for the depicted property.
- **CALA Relevant?** (singleSelect) — Yes / No / Unknown.
- **Hotel / Property Confirmed?** (singleSelect) — Yes / No / Unknown.
- **Brand Confirmed?** (singleSelect) — Yes / No / Unknown.
- **Source Page Confirms Image Context?** (singleSelect) — Yes / No / Unknown.
- **Use Case Match** (singleSelect) — Matched value-driver/use-case, or None.
- **Visual Slot Validation Status** (singleSelect) — Slot-specific validation verdict.
- **Visual Slot Validation Notes** (multilineText) — Reviewer-facing validation detail.

## 5. Status correction writer v2

- Records scanned: **27**
- Records proposed: **10**
- Records updated: **0**
- Records skipped: **3**
- Candidate Only: Ermita, Cartagena, a Tribute Portfolio Hotel — Hero Image (exterior), Ermita, Cartagena, a Tribute Portfolio Hotel — Image Gallery 3 (exterior), Casa Nizuc, a Tribute Portfolio Resort — Image Gallery 1 (gallery), Tribute property/design image 2, Hotel Rumbao, a Tribute Portfolio Hotel — Image Gallery 4 (exterior), Crystal Cove, Barbados, a Tribute Portfolio All-Inclusive Resort — Where This Brand Creates the Most Value (property), Casa Nizuc, a Tribute Portfolio Resort — Image Gallery 1 (property), Tribute property/design image 3, Tribute property/design image 1, Crystal Cove, Barbados, a Tribute Portfolio All-Inclusive Resort — Image Gallery 2 (property), Hotel Rumbao, a Tribute Portfolio Hotel — Image Gallery 4 (gallery), Humano, Lima, a Tribute Portfolio Hotel — Image Gallery 5 (exterior), Humano, Lima, a Tribute Portfolio Hotel — Image Gallery 5 (gallery), Humano, Lima, a Tribute Portfolio Hotel — Where This Brand Creates the Most Value (exterior), Crystal Cove, Barbados, a Tribute Portfolio All-Inclusive Resort — Image Gallery 2 (gallery), Loma, Medellin, a Tribute Portfolio Hotel — Image Gallery 6 (exterior), Loma, Medellin, a Tribute Portfolio Hotel — Image Gallery 6 (gallery), Ermita, Cartagena, a Tribute Portfolio Hotel — Where This Brand Creates the Most Value (exterior), Tribute Portfolio hero — consumer property wide (preferred), Casa Nizuc, a Tribute Portfolio Resort — Where This Brand Creates the Most Value (property), Brand Setup — existing logo (unconfirmed), Hotel Rumbao, a Tribute Portfolio Hotel — Where This Brand Creates the Most Value (exterior), Ermita, Cartagena, a Tribute Portfolio Hotel — Image Gallery 3 (gallery), Tribute Portfolio logo — tribute-black.svg (preferred confirmation candidate)
- Do Not Use: Brand Setup — Mock/Demo hero (do not replace), Marriott newsroom — Tribute Portfolio PR/openings (placeholder)
- Internal Only: 2026 Tribute Portfolio FDD (reference)

### Proposed corrections

- **Ermita, Cartagena, a Tribute Portfolio Hotel — Hero Image (exterior)** (`generic-audit-fallback`) — slot fields: Explorer Section, Slot Purpose, Visual Slot Validation Status, Visual Slot Validation Notes, Brand Confirmed?, Hotel / Property Confirmed?, CALA Relevant?, Use Case Match; core: Asset Status, Explorer Use Permission, Usage Review Status
- **Tribute property/design image 2** (`gallery-2`) — slot fields: Explorer Section, Slot Purpose, Brand Confirmed?, Hotel / Property Confirmed?, CALA Relevant?, Source Page Confirms Image Context?, Use Case Match, Visual Slot Validation Status, Visual Slot Validation Notes; core: Asset Status, Explorer Use Permission, Usage Review Status
- **Hotel Rumbao, a Tribute Portfolio Hotel — Image Gallery 4 (exterior)** (`generic-audit-fallback`) — slot fields: Explorer Section, Slot Purpose, Visual Slot Validation Status, Visual Slot Validation Notes, Brand Confirmed?, Hotel / Property Confirmed?, CALA Relevant?, Use Case Match; core: Asset Status, Explorer Use Permission, Usage Review Status
- **Tribute property/design image 3** (`gallery-3`) — slot fields: Explorer Section, Slot Purpose, Brand Confirmed?, Hotel / Property Confirmed?, CALA Relevant?, Source Page Confirms Image Context?, Use Case Match, Visual Slot Validation Status, Visual Slot Validation Notes; core: Asset Status, Explorer Use Permission, Usage Review Status
- **Tribute property/design image 1** (`gallery-1`) — slot fields: Explorer Section, Slot Purpose, Brand Confirmed?, Hotel / Property Confirmed?, CALA Relevant?, Source Page Confirms Image Context?, Use Case Match, Visual Slot Validation Status, Visual Slot Validation Notes; core: Asset Status, Explorer Use Permission, Usage Review Status
- **Humano, Lima, a Tribute Portfolio Hotel — Image Gallery 5 (exterior)** (`generic-audit-fallback`) — slot fields: Explorer Section, Slot Purpose, Visual Slot Validation Status, Visual Slot Validation Notes, Brand Confirmed?, Hotel / Property Confirmed?, CALA Relevant?, Use Case Match; core: Asset Status, Explorer Use Permission, Usage Review Status
- **Loma, Medellin, a Tribute Portfolio Hotel — Image Gallery 6 (exterior)** (`generic-audit-fallback`) — slot fields: Explorer Section, Slot Purpose, Visual Slot Validation Status, Visual Slot Validation Notes, Brand Confirmed?, Hotel / Property Confirmed?, CALA Relevant?, Use Case Match; core: Asset Status, Explorer Use Permission, Usage Review Status
- **Tribute Portfolio hero — consumer property wide (preferred)** (`hero-candidate`) — slot fields: Explorer Section, Slot Purpose, Brand Confirmed?, Hotel / Property Confirmed?, CALA Relevant?, Source Page Confirms Image Context?, Use Case Match, Visual Slot Validation Status, Visual Slot Validation Notes; core: Asset Status, Explorer Use Permission, Usage Review Status
- **Brand Setup — existing logo (unconfirmed)** (`existing-logo-unconfirmed`) — slot fields: Explorer Section, Slot Purpose, Brand Confirmed?, Hotel / Property Confirmed?, CALA Relevant?, Source Page Confirms Image Context?, Use Case Match, Visual Slot Validation Status, Visual Slot Validation Notes; core: Asset Status, Explorer Use Permission, Usage Review Status
- **Tribute Portfolio logo — tribute-black.svg (preferred confirmation candidate)** (`tribute-black-svg`) — slot fields: Explorer Section, Slot Purpose, Brand Confirmed?, Hotel / Property Confirmed?, CALA Relevant?, Source Page Confirms Image Context?, Use Case Match, Visual Slot Validation Status, Visual Slot Validation Notes; core: Asset Status, Explorer Use Permission, Usage Review Status

## 6. Audit of existing asset records

| Asset | Slot | Classification | Validation Status | CALA | Rec. Status | Rec. Explorer Use |
|-------|------|----------------|-------------------|------|-------------|-------------------|
| Ermita, Cartagena, a Tribute Portfolio Hotel — Hero Image (exterior) | Hero Image | Valid for slot | Valid for Slot | Yes | Candidate | Candidate Only |
| Ermita, Cartagena, a Tribute Portfolio Hotel — Image Gallery 3 (exterior) | Image Gallery | Candidate but needs usage review | Needs Usage Review | Yes | Approved For Explorer Use | Candidate Only |
| Casa Nizuc, a Tribute Portfolio Resort — Image Gallery 1 (gallery) | Image Gallery | Not CALA-relevant | Needs CALA Property | Unknown | Approved For Explorer Use | Candidate Only |
| Tribute property/design image 2 | Image Gallery | Not enough context | Needs Property Confirmation | Unknown | Candidate | Candidate Only |
| Hotel Rumbao, a Tribute Portfolio Hotel — Image Gallery 4 (exterior) | Image Gallery | Not CALA-relevant | Needs CALA Property | Unknown | Candidate | Candidate Only |
| Crystal Cove, Barbados, a Tribute Portfolio All-Inclusive Resort — Where This Brand Creates the Most Value (property) | Where This Brand Creates the Most Value | Candidate but needs usage review | Needs Usage Review | Yes | Approved For Explorer Use | Candidate Only |
| Casa Nizuc, a Tribute Portfolio Resort — Image Gallery 1 (property) | Image Gallery | Not CALA-relevant | Needs CALA Property | Unknown | Approved For Explorer Use | Candidate Only |
| Tribute property/design image 3 | Image Gallery | Not enough context | Needs Property Confirmation | Unknown | Candidate | Candidate Only |
| Tribute property/design image 1 | Image Gallery | Not enough context | Needs Property Confirmation | Unknown | Candidate | Candidate Only |
| Crystal Cove, Barbados, a Tribute Portfolio All-Inclusive Resort — Image Gallery 2 (property) | Image Gallery | Candidate but needs usage review | Needs Usage Review | Yes | Approved For Explorer Use | Candidate Only |
| Hotel Rumbao, a Tribute Portfolio Hotel — Image Gallery 4 (gallery) | Image Gallery | Not CALA-relevant | Needs CALA Property | Unknown | Approved For Explorer Use | Candidate Only |
| Humano, Lima, a Tribute Portfolio Hotel — Image Gallery 5 (exterior) | Image Gallery | Candidate but needs usage review | Needs Usage Review | Yes | Candidate | Candidate Only |
| Humano, Lima, a Tribute Portfolio Hotel — Image Gallery 5 (gallery) | Image Gallery | Valid for slot | Valid for Slot | Yes | Approved For Explorer Use | Candidate Only |
| Humano, Lima, a Tribute Portfolio Hotel — Where This Brand Creates the Most Value (exterior) | Where This Brand Creates the Most Value | Candidate but needs usage review | Needs Usage Review | Yes | Approved For Explorer Use | Candidate Only |
| Crystal Cove, Barbados, a Tribute Portfolio All-Inclusive Resort — Image Gallery 2 (gallery) | Image Gallery | Valid for slot | Valid for Slot | Yes | Approved For Explorer Use | Candidate Only |
| Brand Setup — Mock/Demo hero (do not replace) | Hero Image | Mock/Demo guard | Mock/Demo Guard | Unknown | Mock/Demo | Do Not Use |
| Loma, Medellin, a Tribute Portfolio Hotel — Image Gallery 6 (exterior) | Image Gallery | Candidate but needs usage review | Needs Usage Review | Yes | Candidate | Candidate Only |
| Loma, Medellin, a Tribute Portfolio Hotel — Image Gallery 6 (gallery) | Image Gallery | Valid for slot | Valid for Slot | Yes | Approved For Explorer Use | Candidate Only |
| Ermita, Cartagena, a Tribute Portfolio Hotel — Where This Brand Creates the Most Value (exterior) | Where This Brand Creates the Most Value | Candidate but needs usage review | Needs Usage Review | Yes | Approved For Explorer Use | Candidate Only |
| Tribute Portfolio hero — consumer property wide (preferred) | Hero Image | Not enough context | Needs Property Confirmation | Unknown | Candidate | Candidate Only |
| Casa Nizuc, a Tribute Portfolio Resort — Where This Brand Creates the Most Value (property) | Where This Brand Creates the Most Value | Not CALA-relevant | Needs CALA Property | Unknown | Approved For Explorer Use | Candidate Only |
| 2026 Tribute Portfolio FDD (reference) | Brand Standards / Owner Considerations | Valid for slot | Source Reference Only | Unknown | Source-Confirmed | Internal Only |
| Brand Setup — existing logo (unconfirmed) | Logo | Candidate but needs usage review | Needs Usage Review | Unknown | Needs Usage Review | Candidate Only |
| Marriott newsroom — Tribute Portfolio PR/openings (placeholder) | PR / Opening Link | PR provenance-only | Provenance Only | Unknown | Do Not Use | Do Not Use |
| Hotel Rumbao, a Tribute Portfolio Hotel — Where This Brand Creates the Most Value (exterior) | Where This Brand Creates the Most Value | Not CALA-relevant | Needs CALA Property | Unknown | Approved For Explorer Use | Candidate Only |
| Ermita, Cartagena, a Tribute Portfolio Hotel — Image Gallery 3 (gallery) | Image Gallery | Valid for slot | Valid for Slot | Yes | Approved For Explorer Use | Candidate Only |
| Tribute Portfolio logo — tribute-black.svg (preferred confirmation candidate) | Logo | Valid for slot | Valid for Slot | Unknown | Approved For Explorer Use | Candidate Only |

## 7. Invalid for Explorer visual use

- **Casa Nizuc, a Tribute Portfolio Resort — Image Gallery 1 (gallery)** (Image Gallery) — Not CALA-relevant: Named Tribute property confirmed, but not CALA-relevant; prefer CALA property for CALA-focused profile.
- **Tribute property/design image 2** (Image Gallery) — Not enough context: Confirm this is a real, named Tribute hotel/property image; prefer a CALA-relevant property for CALA-focused profile.
- **Hotel Rumbao, a Tribute Portfolio Hotel — Image Gallery 4 (exterior)** (Image Gallery) — Not CALA-relevant: Named Tribute property confirmed, but not CALA-relevant; prefer CALA property for CALA-focused profile.
- **Casa Nizuc, a Tribute Portfolio Resort — Image Gallery 1 (property)** (Image Gallery) — Not CALA-relevant: Named Tribute property confirmed, but not CALA-relevant; prefer CALA property for CALA-focused profile.
- **Tribute property/design image 3** (Image Gallery) — Not enough context: Confirm this is a real, named Tribute hotel/property image; prefer a CALA-relevant property for CALA-focused profile.
- **Tribute property/design image 1** (Image Gallery) — Not enough context: Confirm this is a real, named Tribute hotel/property image; prefer a CALA-relevant property for CALA-focused profile.
- **Hotel Rumbao, a Tribute Portfolio Hotel — Image Gallery 4 (gallery)** (Image Gallery) — Not CALA-relevant: Named Tribute property confirmed, but not CALA-relevant; prefer CALA property for CALA-focused profile.
- **Tribute Portfolio hero — consumer property wide (preferred)** (Hero Image) — Not enough context: Confirm this is a real, named Tribute hotel/property image; prefer a CALA-relevant property for CALA-focused profile.
- **Casa Nizuc, a Tribute Portfolio Resort — Where This Brand Creates the Most Value (property)** (Where This Brand Creates the Most Value) — Not CALA-relevant: Named Tribute property confirmed, but not CALA-relevant; prefer CALA property for CALA-focused profile.
- **Hotel Rumbao, a Tribute Portfolio Hotel — Where This Brand Creates the Most Value (exterior)** (Where This Brand Creates the Most Value) — Not CALA-relevant: Named Tribute property confirmed, but not CALA-relevant; prefer CALA property for CALA-focused profile.

## 8. Valid only for logo / PDF / source reference

- **2026 Tribute Portfolio FDD (reference)** (Brand Standards / Owner Considerations) — Valid for slot
- **Brand Setup — existing logo (unconfirmed)** (Logo) — Candidate but needs usage review
- **Tribute Portfolio logo — tribute-black.svg (preferred confirmation candidate)** (Logo) — Valid for slot

## 9. Needs CALA hotel/property replacement

- **Casa Nizuc, a Tribute Portfolio Resort — Image Gallery 1 (gallery)** (Image Gallery) — CALA relevant: Unknown
- **Tribute property/design image 2** (Image Gallery) — CALA relevant: Unknown
- **Hotel Rumbao, a Tribute Portfolio Hotel — Image Gallery 4 (exterior)** (Image Gallery) — CALA relevant: Unknown
- **Casa Nizuc, a Tribute Portfolio Resort — Image Gallery 1 (property)** (Image Gallery) — CALA relevant: Unknown
- **Tribute property/design image 3** (Image Gallery) — CALA relevant: Unknown
- **Tribute property/design image 1** (Image Gallery) — CALA relevant: Unknown
- **Hotel Rumbao, a Tribute Portfolio Hotel — Image Gallery 4 (gallery)** (Image Gallery) — CALA relevant: Unknown
- **Brand Setup — Mock/Demo hero (do not replace)** (Hero Image) — CALA relevant: Unknown
- **Tribute Portfolio hero — consumer property wide (preferred)** (Hero Image) — CALA relevant: Unknown
- **Casa Nizuc, a Tribute Portfolio Resort — Where This Brand Creates the Most Value (property)** (Where This Brand Creates the Most Value) — CALA relevant: Unknown
- **Hotel Rumbao, a Tribute Portfolio Hotel — Where This Brand Creates the Most Value (exterior)** (Where This Brand Creates the Most Value) — CALA relevant: Unknown

## 10. Slot coverage

| Slot | Status | Records | Valid |
|------|--------|---------|-------|
| Logo | Covered | 2 | 1 |
| Hero Image | Covered | 3 | 1 |
| Image Gallery | Covered | 15 | 4 |
| Recent Openings | Missing | 0 | 0 |
| Where This Brand Creates the Most Value | Candidate Only | 5 | 0 |
| Brand Standards / Owner Considerations | Covered | 1 | 1 |
| PR / Opening Link | Candidate Only | 1 | 0 |

**Missing slots:** Recent Openings

## 11. Recommended status corrections

- None

## 12. Airtable modified

**No**

## 13. Schema apply command

_Registry schema already sufficient for slot governance._

## 14. Status-correction apply command

```bash
npm run brand-explorer-visual-slot-requirements -- --brand tribute-portfolio --apply --approve-brand-visual-slot-status
```

## 15. Remaining work to visual parity (Kimpton / Radisson Blu)

- Capture and confirm named Tribute hotel/property images (hero + 3–6 gallery), ideally CALA-relevant.
- Populate value-driver-matched imagery for 'Where This Brand Creates the Most Value'.
- Capture specific opening/property images + dates for Recent Openings.
- Rendered Source Capture v1 for news.marriott.com PR/openings.
- Confirm logo against official source and complete usage review.
- Future v3: asset download + Explorer hero/logo promotion writer with staging.

## Does not do

- Download images
- Approve image assets for Explorer use automatically
- Overwrite Brand Setup logo/hero/image/media fields
- Replace the Mock/Demo hero
- Set Company Validated or Company Validation Date
- Imply Marriott validated the assets
