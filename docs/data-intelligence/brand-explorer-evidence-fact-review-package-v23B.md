# Brand Explorer Evidence Fact Review Package v23B

- Generated: 2026-08-09T18:39:57.401Z
- Mode: **dry-run** · Airtable modified: **no**
- Brand: **Tribute Portfolio** (`recob7tgHRryRSbeO`)
- Extraction apply run: **no**
- Candidate facts: **8 proposed** · **0 created** · **0 Pending Review live**
- Extraction apply not run — facts are proposed in report only.

## Review buckets
- Safe for founder review: none
- Internal-only: none
- Needs source capture: `be.standards.designStandardsDelivery`, `loyalty.kpi.hotels`, `loyalty.kpi.markets`, `loyalty.kpi.members`, `loyalty.kpi.mix`, `loyalty.proof`, `materials.caseStudy`, `overview.proof_operator`…
- Not safe for display: none

## Fact-by-fact review
| Field | Live ID | Status | Risk | Bucket | Slots | Writer? |
|-------|---------|--------|------|--------|-------|---------|
| `be.loyalty.earnMechanics` | — | proposed_not_created | low | needs_source_capture | loyalty.earn | no |
| `be.loyalty.redeemMechanics` | — | proposed_not_created | low | needs_source_capture | loyalty.redeem | no |
| `be.loyalty.eliteTierLadder` | — | proposed_not_created | low | needs_source_capture | loyalty.elite | no |
| `be.loyalty.memberRatesBenefit` | — | proposed_not_created | low | needs_source_capture | loyalty.earn, loyalty.proof | no |
| `be.loyalty.programScaleStatement` | — | proposed_not_created | medium | needs_source_capture | loyalty.proof | no |
| `be.standards.qualityAssuranceTheme` | — | proposed_not_created | high | needs_source_capture | standards.requirement | no |
| `be.meta.fddDocumentVintage` | — | proposed_not_created | low | needs_source_capture | standards.last_reviewed | no |
| `be.positioning.independentCollectionStatement` | — | proposed_not_created | low | needs_source_capture | overview.proof_operator | no |

## v23C writer (after approval)
- Ready after fact approval: **no**
- Complete fact stewardship and fill standards.requirement gap before v23C writer.

## Exact next command
```bash
npm run tribute-portfolio-targeted-extract -- --apply
```