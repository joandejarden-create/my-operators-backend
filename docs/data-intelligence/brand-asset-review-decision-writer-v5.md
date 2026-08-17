# Brand Asset Review Decision Writer v5 / v5.1

**Status:** Human review decision writer + approval-state consistency audit (Tribute Portfolio pilot)  
**Module:** `lib/partner-intelligence/brand-asset-review-decision-writer.js`  
**Script:** `npm run brand-asset-review-decision-writer -- --brand tribute-portfolio --dry-run`

## Purpose

After human usage review, this module applies **explicit approval/rejection decisions** to selected Brand Asset Registry records. It is an **approval-decision writer only** — it does not download images, attach files, write Brand Setup media fields, or promote assets into Brand Explorer.

**v5.1** adds an **approval-state consistency audit** that detects records where `Asset Status` says approved but `Explorer Use Permission` and `Usage Review Status` do not match formal v5 approval. These are classified as **Approval State Conflict**, not protected approved records.

**Approval is never automatic.** Only record IDs passed via `--approve-records` are approved.

## Commands

```bash
# Default — list primaries with record IDs; no Airtable writes
npm run brand-asset-review-decision-writer -- --brand tribute-portfolio --dry-run

# Approve specific records (explicit IDs required)
npm run brand-asset-review-decision-writer -- --brand tribute-portfolio \
  --apply --approve-brand-asset-review-decisions \
  --approve-records reczTkwignWPydWJp,rec610hZiW38N0A3e

# Reject specific records
npm run brand-asset-review-decision-writer -- --brand tribute-portfolio \
  --apply --approve-brand-asset-review-decisions \
  --reject-records recXXXX

# Keep as candidate (pending further review)
npm run brand-asset-review-decision-writer -- --brand tribute-portfolio \
  --apply --approve-brand-asset-review-decisions \
  --keep-candidate-records recXXXX
```

`--apply` requires `--approve-brand-asset-review-decisions` (decisions) or `--approve-brand-asset-approval-state-corrections` (state corrections).  
`--apply` without `--approve-records` / `--reject-records` / `--keep-candidate-records` does **not** modify Airtable for decisions.

### Approval-state correction (v5.1)

```bash
# Audit only (default dry-run includes conflict detection)
npm run brand-asset-review-decision-writer -- --brand tribute-portfolio --dry-run

# Apply safe corrections for inconsistent approval states
npm run brand-asset-review-decision-writer -- --brand tribute-portfolio \
  --apply --approve-brand-asset-approval-state-corrections
```

## Formal approval definition (v5.1)

A record is **formally approved** only when **all three** fields align:

| Field | Value |
|-------|-------|
| Explorer Use Permission | `Approved For Explorer` |
| Usage Review Status | `Usage Review Complete` |
| Asset Status | `Approved For Explorer Use` |
| Review Notes | Must include v5 writer stamp: `Approved after human source/visual review by …` |

`Asset Status` alone does **not** constitute full approval. Records with approved asset status but `Candidate Only` permission are **Approval State Conflict**.

## Approval-state conflict (v5.1)

Conflict when:

- `Asset Status` = `Approved For Explorer Use`
- **and** `Explorer Use Permission` ≠ `Approved For Explorer` **or** `Usage Review Status` ∉ {`Usage Review Complete`, `Reviewed`} **or** missing v5 approval Review Notes

Proposed safe correction:

| Field | Value |
|-------|-------|
| Asset Status | `Candidate` |
| Explorer Use Permission | `Candidate Only` |
| Usage Review Status | `Pending Review` |
| Review Notes | Approval state corrected because Asset Status previously said Approved For Explorer Use while Explorer Use Permission remained Candidate Only. Not approved for Explorer until human review. |

## Approved record updates

| Field | Value |
|-------|-------|
| Usage Review Status | `Usage Review Complete` |
| Explorer Use Permission | `Approved For Explorer` |
| Asset Status | `Approved For Explorer Use` |
| Last Reviewed Date | today |
| Review Notes | Approved after human source/visual review by Joan. Marriott-controlled source. No Marriott validation implied. |

> **Note:** `Reviewed By` is an Airtable `singleCollaborator` field and cannot be set from a plain name string via the API. The reviewer name is recorded in **Review Notes** instead. Set the collaborator field manually in Airtable if attribution is required there.

## Rejected record updates

| Field | Value |
|-------|-------|
| Usage Review Status | `Usage Review Complete` |
| Explorer Use Permission | `Do Not Use` |
| Asset Status | `Do Not Use` |
| Review Notes | Rejected after human review. Do not use for Explorer. |

## Blocked from approval

- Recent Openings
- PR / Opening Link
- Mock/Demo Guard
- PR Provenance Only
- Explorer Use Permission = Do Not Use
- Asset Status = Mock/Demo
- FDD / PDF reference (internal only)
- Missing Source URL or property context
- Non-primary (unless `--allow-non-primary`)

## Does not do

- Auto-approve based on metadata
- Approve all primaries at once
- Download or attach images
- Write Brand Setup media fields
- Promote to Brand Explorer
- Set Company Validated / Company Validation Date
- Delete records

## Related

- [brand-asset-human-review-readiness-v4.md](./brand-asset-human-review-readiness-v4.md)
- [tribute-visual-asset-slot-review-v3.md](./tribute-visual-asset-slot-review-v3.md)
- [brand-asset-registry-workflow-v1.md](./brand-asset-registry-workflow-v1.md)

## Future

- Asset download + attachment writer (after approval)
- Governed Explorer hero/logo promotion writer

## Next Stage

- [brand-asset-download-attachment-writer-v6.md](./brand-asset-download-attachment-writer-v6.md) — downloads/attaches only formally approved records; does not promote Explorer media fields.
- [explorer-media-promotion-writer-v7.md](./explorer-media-promotion-writer-v7.md) — promotes only approved attachment-ready assets into Explorer-facing media fields using gated overwrite controls.
