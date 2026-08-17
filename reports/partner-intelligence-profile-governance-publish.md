# Partner Intelligence → Profile Governance Publish

Generated: 2026-07-06T19:25:06.524Z
Mode: **dry-run**
Base: `appvtnDurnMSjINP6`
Readiness input: file (2026-07-06T19:21:42.877Z)

## Summary

| Metric | Count |
|--------|-------|
| Packages considered | 1 |
| Eligible in input | 1 |
| Would publish / published | 1 |
| Skipped | 0 |
| Records modified | 0 |

## Publish mapping

Writable API keys:
- `validationStatus`
- `usagePermission`
- `sourceType`
- `sourceRegion`
- `lastReviewedDate`
- `refreshDueDate`
- `evidenceNotes`
- `missingDataFlags`
- `reviewedBy`
- `externalDisplayStatus`
- `internalNotes`
- `confidenceLevel`

Never written:
- `companyValidated`
- `companyValidationDate`

Operator confidence column: **Data Confidence Level** (alias for Confidence Level).

## Radisson Blu by Choice (brand)

- Target: `recWPEvxBQxVVzSq3` on **Brand Setup - Brand Basics**
- Write status: **dry_run**

### Field diff

| Field | Live column | From | To |
|-------|-------------|------|-----|
| `Internal Notes` | Internal Notes | "PI profile-governance publish 2026-07-06 (brand:recWPEvxBQxVVzSq3)." | "PI publish readiness audit proposal — not written." |

### Skipped fields

- `Company Validated`: never_written_by_pi_publish
- `Company Validation Date`: never_written_by_pi_publish

### Expected Explorer chip

- **displayLabel:** `AI-Assisted Profile`
- **displaySubtitle:** `Last Reviewed: Jul 6, 2026 · Source Basis: Company Materials`

## Apply

Dry-run only. After founder approval:

```bash
npm run publish-partner-intelligence-profile-governance -- --apply --entity-type operator --target-rec-id recF5Z87OAqFgndoq
```
