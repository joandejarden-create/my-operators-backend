# Brand Explorer WoodSpring Visual Completion Writer v33D

## Purpose

Complete WoodSpring Brand Explorer visual readiness after v33C openings/momentum build:

- Attach founder-approved images to 4 `footprint.openings` rows
- Audit and materialize `overview.scenario.1` / `.2` / clean `.3` images
- Fill `materials.gallery.1`–`6` where safe assets exist
- Approve and canonicalize Brand Asset Registry rows
- Preserve quarantined Everhome `overview.scenario.3` row (`recrnaRxigUSoDDTJ`)

## Target brand

- **WoodSpring Suites** · `woodspring-suites` · `recsOd51NzRPYsMko`

## Protected scope

Never modify:

- Company Validated / Company Validation Date
- Source Library approval statuses
- `footprint.momentum` rows
- Opening copy / titles / Body source links (unless Summary URL — field does not exist)
- Quarantined Everhome scenario row
- Everhome Suites / Suburban Studios

## v33C lesson

**Do not write `Summary URL`.** Brand Explorer Presentation has no Summary URL field. Source URLs remain in Body only.

## v33D-R1 patch (gallery diversity)

- Gallery slots use **distinct official images only** — no 3-asset filler across 6 slots.
- Insufficient distinct assets → `Do Not Display` hide on deferred gallery rows.
- Opening reuse remains allowed with `imageReuseNote` for footprint/example rows.
- Premium display guardrails block apply for visible placeholders, gallery duplicate filler, wrong-brand, or temp URLs as durable source pages.

## Commands

```bash
# Dry-run (default)
npm run brand-explorer-woodspring-visual-completion-writer -- --brand woodspring-suites --dry-run

# Apply (requires founder approval gates)
npm run brand-explorer-woodspring-visual-completion-writer -- --brand woodspring-suites --apply \
  --approve-brand-explorer-v33D-woodspring-visual-completion \
  --founder-approved-woodspring-official-images \
  --confirm-official-source-images-only \
  --confirm-no-company-validation-claim \
  --confirm-no-source-library-changes \
  --confirm-no-summary-url-field \
  --confirm-no-momentum-changes \
  --confirm-woodspring-only
```

## Apply gates

| Flag | Purpose |
|------|---------|
| `--approve-brand-explorer-v33D-woodspring-visual-completion` | Batch approve |
| `--founder-approved-woodspring-official-images` | Founder confirms official WoodSpring images |
| `--confirm-official-source-images-only` | Block non-official image materialization |
| `--confirm-no-company-validation-claim` | No Company Validated writes |
| `--confirm-no-source-library-changes` | Source Library untouched |
| `--confirm-no-summary-url-field` | Block Summary URL writes |
| `--confirm-no-momentum-changes` | Momentum rows read-only |
| `--confirm-woodspring-only` | WoodSpring-only scope |

## Outputs

- `reports/brand-explorer-woodspring-visual-completion-writer.json`
- `reports/brand-explorer-woodspring-visual-completion-writer.md`

## Rollback

Revert presentation `Image` fields and registry approval patches by staging run ID `v33D-woodspring-visual-completion`.
