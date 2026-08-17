# Brand Asset Registry / Approval Workflow v1/v2

**Status:** Schema applied · Record writer v2 (Tribute Portfolio pilot)  
**Module:** `lib/partner-intelligence/brand-asset-registry-workflow.js`  
**Script:** `npm run brand-asset-registry-workflow -- --brand tribute-portfolio --dry-run`

## Purpose

Brand Asset & PR Package Governance v1 identified official Tribute logo, hero, property, and PR candidates. The registry schema is applied (`Partner Intelligence - Brand Asset Registry`, `tblwNaf9DZt8Lth4t`). **v2** writes metadata-only staged asset records into the registry table.

## Commands

```bash
# Default — inspect, compare staged vs existing, no writes
npm run brand-asset-registry-workflow -- --brand tribute-portfolio --dry-run

# Schema apply (one-time, already done for Tribute)
npm run brand-asset-registry-workflow -- --brand tribute-portfolio --apply --approve-brand-asset-registry-schema

# Write metadata-only registry records (v2)
npm run brand-asset-registry-workflow -- --brand tribute-portfolio --apply --approve-brand-asset-registry-records
```

## Outputs

- `reports/brand-asset-registry-workflow.md`
- `reports/brand-asset-registry-workflow.json`

## Record writer v2 behavior

1. Dry-run by default.
2. Lists existing registry rows for brand (`Brand Record ID`).
3. Maps 9 staged Tribute candidates to registry fields.
4. De-duplicates on: Brand Record ID + Asset Type + Source URL or Asset Name + Recommended Explorer Slot.
5. Creates metadata-only records on apply — **no Attachment field, no image downloads**.
6. Never sets Approved For Explorer Use or Company Validated.
7. Mock/Demo hero recorded as guard row; PR newsroom as Do Not Use.

## Does not do (v2)

- Download images or populate Attachment fields
- Overwrite Brand Setup logo, hero, or presentation images
- Replace Mock/Demo hero
- Mark assets Approved For Explorer Use automatically
- Set Company Validated / Company Validation Date

## Related

- [brand-asset-pr-package-governance-v1.md](./brand-asset-pr-package-governance-v1.md)
- [brand-explorer-visual-slot-requirements-v1.md](./brand-explorer-visual-slot-requirements-v1.md) — enforces slot-specific image requirements before Explorer approval
- [cala-tribute-property-visual-discovery-v1.md](./cala-tribute-property-visual-discovery-v1.md) — CALA named-property visual candidate discovery
- [tribute-visual-asset-slot-review-v3.md](./tribute-visual-asset-slot-review-v3.md) — slot review + primary/alternate selection for competing candidates
- [brand-asset-human-review-readiness-v4.md](./brand-asset-human-review-readiness-v4.md) — human review readiness for primary candidates
- [brand-asset-download-attachment-writer-v6.md](./brand-asset-download-attachment-writer-v6.md) — download + attachment staging for formally approved records
- [tribute-portfolio-package-pipeline-v1.md](./tribute-portfolio-package-pipeline-v1.md)

## Future v3

- Human usage-review workflow in Airtable
- Asset download + rights registry
- Staged Explorer hero/logo promotion writer
- Rendered Source Capture v1 for Marriott newsroom
