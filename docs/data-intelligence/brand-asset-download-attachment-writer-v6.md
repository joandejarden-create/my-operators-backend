# Brand Asset Download & Attachment Writer v6.1

**Status:** Download + attachment staging/materialization repair writer (Tribute Portfolio pilot)  
**Module:** `lib/partner-intelligence/brand-asset-download-attachment-writer.js`  
**Script:** `npm run brand-asset-download-attachment-writer -- --brand tribute-portfolio --dry-run`

## Purpose

After formal approvals are complete in the Brand Asset Registry, this module stages approved asset binaries and attachment metadata for those records only.

It is **not** an Explorer promotion writer.

## Command

```bash
# default (report + validation only)
npm run brand-asset-download-attachment-writer -- --brand tribute-portfolio --dry-run

# apply (gated)
npm run brand-asset-download-attachment-writer -- --brand tribute-portfolio \
  --apply --approve-brand-asset-download-attachments

# materialization repair mode (gated)
npm run brand-asset-download-attachment-writer -- --brand tribute-portfolio \
  --dry-run --repair-missing-attachments

npm run brand-asset-download-attachment-writer -- --brand tribute-portfolio \
  --apply --approve-brand-asset-download-attachments \
  --repair-missing-attachments --approve-brand-asset-attachment-materialization-repair
```

## Formal approval filter

Only records matching the v5.1 formal approval definition are eligible:

- `Asset Status` = `Approved For Explorer Use`
- `Explorer Use Permission` = `Approved For Explorer`
- `Usage Review Status` = `Usage Review Complete` (or `Reviewed` compatibility)
- `Review Notes` include v5 approval stamp (`Approved after human source/visual review by …`)

Everything else is excluded.

## Download/validation behavior

For each eligible record, v6:

1. Validates metadata requirements:
   - Source URL
   - Source Page URL where relevant
   - Asset Type
   - Recommended Explorer Slot
   - Related Property Name / Country / CALA / Property Confirmed / Brand Confirmed for hotel imagery
2. Fetches Source URL and validates:
   - HTTP 200
   - image/* or SVG-compatible content type
   - non-empty body
   - max size <= 25MB
3. Uses deterministic file names and staging paths:
   - `data/partner-intelligence/assets/tribute-portfolio/...`
4. Reports the exact download plan in dry-run.

## Apply writes (registry only)

On apply, writes only allowed registry fields:

- `Attachment` (via Airtable URL attachment mode)
- `Local File Path`
- append `Source Notes` / `Review Notes` staging markers
- `Last Reviewed Date`

## Materialization repair behavior (v6.1)

- Re-reads approved registry records and reports, per record:
  - attachment present/missing
  - local file present/missing
  - source URL present/missing
- Prefers Airtable Content API byte upload (`uploadAttachment`) for missing attachments.
- Falls back to URL patch only when direct upload is not possible.
- **Success criteria changed:** patch acceptance alone is insufficient; each repaired record is re-read and must have `Attachment count > 0`.
- `readyForExplorerMediaPromotionWriterV7` is true only when approved records are materially attachable/readable.

## Does not do

- Write Brand Setup fields
- Write Explorer media fields
- Replace Mock/Demo hero
- Approve additional assets
- Set `Company Validated` / `Company Validation Date`
- Promote to Explorer

## Related

- [brand-asset-review-decision-writer-v5.md](./brand-asset-review-decision-writer-v5.md)
- [brand-asset-human-review-readiness-v4.md](./brand-asset-human-review-readiness-v4.md)
- [tribute-visual-asset-slot-review-v3.md](./tribute-visual-asset-slot-review-v3.md)
- [explorer-media-promotion-writer-v7.md](./explorer-media-promotion-writer-v7.md)
