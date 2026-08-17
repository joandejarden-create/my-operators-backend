# Brand Explorer WoodSpring Final Content Cleanup Writer v33E

## Purpose

Move WoodSpring from **almost_ready (85)** to **ready / readyForActiveProfile** by completing content bullets, cleaning loyalty copy, stewarding pending Partner Facts, and reconciling hidden gallery audit behavior — **without touching visual assets**.

## Target brand

- **WoodSpring Suites** · `woodspring-suites` · `recsOd51NzRPYsMko`

## Scope

| Area | Action |
|------|--------|
| `overview.why_value` | 5 owner-facing bullets |
| `overview.differentiators.identity` | 4 bullets |
| `overview.differentiators.commercial` | 4 bullets |
| `loyalty.proof` | Remove consumer-site / FDD / Item 19 language |
| Partner Facts | Approve safe public facts; Internal Only for sensitive/thin |
| Gallery audit | Code fix — Do Not Display slots not blocking QA |
| Sort order | Deferred to v24D |

## Protected (never modify)

- Image fields / attachments / registry approvals
- Source Library approvals
- Openings / momentum rows
- Gallery 1–3 images/titles
- Gallery 4–6 visibility (stay hidden)
- Company Validated / Company Validation Date
- Everhome / Suburban / non-target brands

## Commands

```bash
# Dry-run
npm run brand-explorer-woodspring-final-content-cleanup-writer -- --brand woodspring-suites --dry-run

# Apply
npm run brand-explorer-woodspring-final-content-cleanup-writer -- --brand woodspring-suites --apply \
  --approve-brand-explorer-v33E-woodspring-final-content-cleanup \
  --confirm-no-company-validation-claim \
  --confirm-no-image-field-changes \
  --confirm-no-registry-approval-changes \
  --confirm-no-source-library-changes \
  --confirm-no-openings-or-momentum-changes \
  --confirm-hidden-gallery-stays-hidden \
  --confirm-woodspring-only
```

## Post-run validation

```bash
npm run brand-explorer-final-qa-auditor -- --brand woodspring-suites --dry-run
npm run brand-explorer-complete-build -- --brand woodspring-suites --dry-run --target-quality active-profile
npm run brand-explorer-visual-display-defect-audit -- --brand woodspring-suites --dry-run
```

## Outputs

- `reports/brand-explorer-woodspring-final-content-cleanup-writer.json`
- `reports/brand-explorer-woodspring-final-content-cleanup-writer.md`
