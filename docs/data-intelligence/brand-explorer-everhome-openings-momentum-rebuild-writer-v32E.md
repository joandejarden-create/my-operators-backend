# Brand Explorer Everhome Openings Description Cleanup + Momentum Tribute-Parity v32E

Everhome-only writer for polishing `footprint.openings` teaser descriptions and aligning `footprint.momentum` copy/source evidence with Tribute parity rules.

## Command

```bash
npm run brand-explorer-everhome-openings-momentum-rebuild-writer -- \
  --brand everhome-suites --dry-run
```

Apply:

```bash
npm run brand-explorer-everhome-openings-momentum-rebuild-writer -- \
  --brand everhome-suites \
  --apply \
  --approve-brand-explorer-v32E-everhome-openings-momentum-rebuild \
  --confirm-no-company-validation-claim \
  --confirm-no-image-field-changes \
  --confirm-no-image-or-registry-approval-changes \
  --confirm-no-visibility-changes \
  --confirm-preserve-existing-opening-labels \
  --confirm-everhome-only
```

## Guardrails

- Preserve opening images, labels/chips, and structure.
- Rewrite only opening teaser body text when metadata-like or awkward.
- Rewrite momentum summary and source URL only when evidence type is weak.
- No image fields, visibility, approvals, or Company Validated changes.

## Output

- `reports/brand-explorer-everhome-openings-momentum-rebuild-writer.json`
- `reports/brand-explorer-everhome-openings-momentum-rebuild-writer.md`

Next writer after v32E: v32F image-governance recognition/materialization.
