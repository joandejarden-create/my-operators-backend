# Tribute Visual Asset Slot Review & Candidate Selection v3

**Status:** Slot review + candidate selection writer v3 (Tribute Portfolio pilot)  
**Module:** `lib/partner-intelligence/tribute-visual-asset-slot-review.js`  
**Script:** `npm run tribute-visual-asset-slot-review -- --dry-run`

## Purpose

CALA Tribute Property Visual Discovery v2 staged additional metadata-only gallery records from official Marriott `/photos/` pages. The registry now contains **multiple Tribute image candidates** — older v1 generic crops and newer v2 named-property CALA candidates — some competing for the same Brand Explorer slots.

Before any download, approval, or Explorer promotion, this module:

1. Reads all Tribute records from **Partner Intelligence - Brand Asset Registry**
2. Groups by **Explorer Section** and **Recommended Explorer Slot**
3. Identifies **competing candidates** per slot
4. Scores candidate quality (named property, Marriott-controlled source, CALA relevance, etc.)
5. Recommends **one primary** and useful **alternates** per slot
6. Marks weak v1 candidates as **superseded / not selected** (retained, not deleted)

It does **not** download images, approve assets for Explorer use, overwrite Brand Setup media fields, or replace the Mock/Demo hero.

## Commands

```bash
# Default — group, score, recommend primary/alternate, report only
npm run tribute-visual-asset-slot-review -- --dry-run

# Gated selection writer — metadata-only PATCH to registry
npm run tribute-visual-asset-slot-review -- --apply --approve-tribute-visual-slot-selection
```

`--apply` requires `--approve-tribute-visual-slot-selection`.

## Outputs

- `reports/tribute-visual-asset-slot-review.md`
- `reports/tribute-visual-asset-slot-review.json`

## Quality scoring (metadata-only)

Candidates are scored using registry slot-governance fields and source metadata:

| Signal | Weight |
|--------|--------|
| Named property confirmed | +12 |
| Marriott-controlled source | +10 |
| CALA property discovery v2 | +8 |
| CALA relevant | +6 |
| Official `/photos/` page source | +6 |
| Source page confirms context | +5 |
| Value-driver match | +5 |
| Weak v1 generic crop | −20 |
| Generic brand-site without property | −6 |

## Selection behavior

| Role | Is Primary Candidate | Explorer Use Permission | Usage Review Status | Visual Slot Validation Status |
|------|---------------------|----------------------|---------------------|------------------------------|
| Primary | `true` | Candidate Only | Pending Review | Needs Usage Review |
| Alternate | `false` | Candidate Only | Pending Review | Needs Usage Review |
| Superseded | `false` | Candidate Only | Pending Review | Not Enough Context (notes: Superseded Candidate) |
| Approved (locked) | unchanged | Approved For Explorer | Reviewed | unchanged — no PATCH |
| Mock/Demo hero | `false` | Do Not Use | Blocked | Mock/Demo Guard |
| Newsroom PR | `false` | Do Not Use | Blocked | Provenance Only |
| FDD reference | `true` | Internal Only | Usage Review Complete | Source Reference Only |

> **Note:** Usage Review Status uses schema value **Pending Review** (equivalent to "Needs Review" in workflow copy). Superseded candidates use **Not Enough Context** with notes identifying them as Not Selected / Superseded Candidate — no records are deleted.

## Protected records (unchanged intent)

- **Approved for Explorer** — locked after Brand Asset Review Decision Writer v5; slot-review v3 skips updates and does not downgrade
- **Mock/Demo hero** — remain Do Not Use; never replace
- **Newsroom PR placeholder** — provenance only until Rendered Source Capture v1
- **FDD** — internal source reference only

### Approved record detection

A record is **protected / locked** only when **formally approved** (all three fields above). `Asset Status` alone does not trigger protection.

See [brand-asset-review-decision-writer-v5.md](./brand-asset-review-decision-writer-v5.md) for approval-state conflict audit and correction.

## Missing slots (v3)

- **Recent Openings** — remains Missing until property + opening/PR/date are available
- **Value drivers** — Urban, Conversion, Mixed-Use may remain Missing where no matching property/use-case image exists

## Does not do

- Download images or attach files
- Approve assets for Explorer use
- Overwrite Brand Setup logo/hero/image/media fields
- Replace the Mock/Demo hero
- Set Company Validated / Company Validation Date
- Delete asset records
- Imply Marriott validated the assets

## Related

- [cala-tribute-property-visual-discovery-v1.md](./cala-tribute-property-visual-discovery-v1.md)
- [brand-explorer-visual-slot-requirements-v1.md](./brand-explorer-visual-slot-requirements-v1.md)
- [brand-asset-registry-workflow-v1.md](./brand-asset-registry-workflow-v1.md)
- [brand-asset-human-review-readiness-v4.md](./brand-asset-human-review-readiness-v4.md) — human review readiness for primary candidates
- [brand-asset-review-decision-writer-v5.md](./brand-asset-review-decision-writer-v5.md) — applies explicit human approval/rejection decisions

## Future

- Human usage-review approval workflow in Airtable
- Governed hero/logo promotion writer after review approval
- Rendered Source Capture v1 for Marriott newsroom PR
