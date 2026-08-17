# Brand Asset Human Review Readiness v4

**Status:** Human review readiness checker (Tribute Portfolio pilot)  
**Module:** `lib/partner-intelligence/brand-asset-human-review-readiness.js`  
**Script:** `npm run brand-asset-human-review-readiness -- --brand tribute-portfolio --dry-run`

## Purpose

After Tribute Visual Asset Slot Review v3 applied primary/alternate selection to the Brand Asset Registry, **11 primary candidates** need human usage review before any image download, attachment, Explorer approval, or Brand Setup media promotion.

This module evaluates whether each primary candidate has **enough metadata and source context** for a human reviewer to approve, reject, or request more evidence. It does **not** approve or reject records automatically.

## Command

```bash
npm run brand-asset-human-review-readiness -- --brand tribute-portfolio --dry-run
```

**Report-only in v4** — no apply gate, no Airtable writes.

## Outputs

- `reports/brand-asset-human-review-readiness.md`
- `reports/brand-asset-human-review-readiness.json`

## Readiness outcomes

| Outcome | Meaning |
|---------|---------|
| **Ready For Human Review** | Metadata complete; human can proceed (e.g. logo) |
| **Needs More Metadata** | Required registry fields missing or incomplete |
| **Needs Source Review** | Source basis or controlled-source evidence weak |
| **Needs Visual Inspection** | Metadata OK; human must open Source URL and inspect image |
| **Not Ready** | Hard blockers (wrong permission, not primary, etc.) |
| **Missing** | Slot has no primary candidate (Recent Openings, some value drivers) |

## Checks per primary candidate

**Common:** Asset Name, Brand Record ID, Asset Type, Explorer Section, Recommended Explorer Slot, Source URL, Source Page URL (property images), Source Basis, controlled source, Related Property Name, Country/Region, CALA/Property/Brand confirmed, Source Page Confirms Context, Visual Slot Validation Status/Notes, Usage Review Status (Pending/Needs Review), Explorer Use Permission = Candidate Only, Is Primary Candidate = true, Company Validated/Date blank.

**Slot-specific:**
- **Logo** — official source; human compares tribute-black.svg vs Brand Setup logo
- **Hero** — named CALA property, property-confirmed; human inspects URL
- **Gallery** — different real hotels, no duplicate crops; human inspects each URL
- **Value driver** — must match Resort/Urban/Conversion; human inspects URL
- **Recent Openings** — remains Missing; do not approve without PR/date

## Recommended Airtable fields for human reviewer

Usage Review Status · Review Notes · Reviewed By · Last Reviewed Date · Visual Slot Validation Status/Notes · Explorer Use Permission (keep Candidate Only until promotion writer)

**Do not set** Company Validated or Company Validation Date unless explicitly confirmed by Marriott/brand.

## Does not do

- Download images or attach files
- Approve assets for Explorer use
- Overwrite Brand Setup media fields
- Replace Mock/Demo hero
- Delete records
- Write Airtable (v4 is read-only)

## Related

- [tribute-visual-asset-slot-review-v3.md](./tribute-visual-asset-slot-review-v3.md)
- [brand-explorer-visual-slot-requirements-v1.md](./brand-explorer-visual-slot-requirements-v1.md)
- [brand-asset-registry-workflow-v1.md](./brand-asset-registry-workflow-v1.md)

## Future

- Governed hero/logo promotion writer after review complete
- Rendered Source Capture v1 for Marriott newsroom PR
- Asset download + attachment writer (after approval)

## Related

- [brand-asset-review-decision-writer-v5.md](./brand-asset-review-decision-writer-v5.md) — applies explicit human approval/rejection decisions
- [brand-asset-download-attachment-writer-v6.md](./brand-asset-download-attachment-writer-v6.md) — stages approved binaries + registry attachment metadata after formal approvals
