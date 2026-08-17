# Brand Explorer Visual Slot Requirements v1/v2

**Status:** Slot governance + status correction writer v2 (Tribute Portfolio pilot)
**Module:** `lib/partner-intelligence/brand-explorer-visual-slot-requirements.js`
**Script:** `npm run brand-explorer-visual-slot-requirements -- --brand tribute-portfolio --dry-run`

## Purpose

The Brand Asset Registry exists and Tribute has 9 staged asset records, but the image-candidate process is too generic. Brand Explorer needs **slot-specific** imagery: Recent Openings must show the referenced hotel, the gallery must show different real hotels, and the "Where This Brand Creates the Most Value" section must match the value driver shown. This module **defines and enforces slot-specific requirements** before any asset can be approved for Explorer use.

It does **not** download images, approve assets, overwrite Brand Setup media fields, or replace the Mock/Demo hero.

## Commands

```bash
# Default — define slots, audit registry records, propose status corrections
npm run brand-explorer-visual-slot-requirements -- --brand tribute-portfolio --dry-run

# Gated schema apply (adds slot-governance fields to the registry table)
npm run brand-explorer-visual-slot-requirements -- --brand tribute-portfolio --apply --approve-brand-visual-slot-schema

# Gated status writer v2 — populate slot-governance fields + safe status corrections
npm run brand-explorer-visual-slot-requirements -- --brand tribute-portfolio --apply --approve-brand-visual-slot-status
```

`--apply` requires exactly one of `--approve-brand-visual-slot-schema` or `--approve-brand-visual-slot-status`.

## Outputs

- `reports/brand-explorer-visual-slot-requirements.md`
- `reports/brand-explorer-visual-slot-requirements.json`

## Visual slots

| Slot | Explorer section | Property image required? |
|------|------------------|--------------------------|
| Logo | `Brand Setup — Logo` | No |
| Hero Image | `overview.hero` | Yes (CALA preferred) |
| Image Gallery | `materials.gallery.1–6` | Yes (different named hotels) |
| Recent Openings | `footprint.openings` | Yes (specific opening) |
| Where This Brand Creates the Most Value | `overview.why_value` | Yes (value-driver-matched) |
| Brand Standards / Owner Considerations | `materials.file` / Source Library | No (source/PDF ok) |
| PR / Opening Link | `PR / Recent Openings` | No (provenance link) |

## Validation checks

`brandMatch` · `propertyMatch` · `slotMatch` · `geographyMatch` · `valueDriverMatch` · `openingMatch` · `sourceControlled` · `usageReviewComplete` · `notMockDemo` · `notGenericLifestyle` · `notAbstract` · `notThirdPartyPrimary` · `hasNamedProperty` · `hasRegionOrCountry` · `hasSourcePageContext`

## Proposed registry fields (not applied unless gated)

Explorer Section · Slot Purpose · Related Value Driver · Related Property Name · Related Opening / PR · Country / Region · CALA Relevant? · Hotel / Property Confirmed? · Brand Confirmed? · Source Page Confirms Image Context? · Use Case Match · Visual Slot Validation Status · Visual Slot Validation Notes

## Audit classifications

Valid for slot · Candidate but needs usage review · Candidate but wrong slot · Not enough context · Not hotel/property image · Not CALA-relevant · Do Not Use · Mock/Demo guard · PR provenance-only

## Tribute pilot findings (9 records)

- **Existing logo / tribute-black.svg** → Candidate but needs usage review (confirm against official source).
- **Hero candidate + property images 1–3** → Not enough context; confirm real, named Tribute hotels, prefer CALA.
- **FDD** → Valid for slot (source/PDF reference only, not visual imagery).
- **Mock/Demo hero** → Mock/Demo guard (never promote).
- **Newsroom PR placeholder** → PR provenance-only / Do Not Use until Rendered Source Capture v1.
- **Missing slots:** Recent Openings, Where This Brand Creates the Most Value.

The module also demotes any registry record improperly marked "Approved For Explorer Use" that has not passed usage review (recommendation only — never auto-applied).

## Status writer v2 behavior

- Reads existing registry records (metadata-only; no image download).
- Populates the 13 slot-governance fields per record using Tribute-specific correction rules.
- Writes safe status corrections on `--apply --approve-brand-visual-slot-status`.
- Never sets Explorer Use Permission to **Approved For Explorer** or Asset Status to **Approved For Explorer Use**.
- Never sets Company Validated / Company Validation Date.
- Skips records that already match proposed corrections (idempotent).
- Preserves Mock/Demo hero guard, FDD as internal source reference, and PR provenance-only status.

## Does not do (v1/v2)

- Download images or approve assets for Explorer use
- Overwrite Brand Setup logo/hero/image/media fields
- Replace the Mock/Demo hero
- Set Company Validated / Company Validation Date
- Apply status corrections without `--approve-brand-visual-slot-status`

## Related

- [brand-asset-registry-workflow-v1.md](./brand-asset-registry-workflow-v1.md)
- [cala-tribute-property-visual-discovery-v1.md](./cala-tribute-property-visual-discovery-v1.md) — discovers named CALA Tribute property image candidates
- [tribute-visual-asset-slot-review-v3.md](./tribute-visual-asset-slot-review-v3.md) — slot review + primary/alternate selection for competing candidates
- [brand-asset-human-review-readiness-v4.md](./brand-asset-human-review-readiness-v4.md) — human review readiness for primary candidates
- [brand-asset-pr-package-governance-v1.md](./brand-asset-pr-package-governance-v1.md)

## Future v2/v3

- Slot-governance field writer + status-correction apply
- Rendered Source Capture v1 for Marriott newsroom
- Named-property / CALA image capture + rights registry
- Explorer hero/logo promotion writer with staging
