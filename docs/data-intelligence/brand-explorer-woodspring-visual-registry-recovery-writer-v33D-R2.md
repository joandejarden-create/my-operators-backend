# Brand Explorer WoodSpring Visual Registry Recovery Writer v33D-R2

## Purpose

Recover v33D-R1 partial apply for **WoodSpring Suites**:

- v33D-R1 materialized presentation `Image` fields but **registry creates failed** on v2 validator
  (`Approved For Explorer Use` / `Approved For Explorer` rejected)
- Post-apply QA regressed (blocked Final QA, visual defects, Complete Build blocked)
- UI showed blank gallery/opening cards and wrong luxury gallery labels

v33D-R2:

1. Audits live presentation rows vs API `imageUrl` exposure
2. Patches/creates registry rows using **Everhome v32G-R1** `validateV32gR1RegistryWritePayload`
3. Fixes WoodSpring gallery **Title** labels on visible slots 1–3
4. Re-hides gallery slots 4–6 (`Do Not Display`) when needed
5. Rematerializes images **only** when attachments are missing
6. UI fix: gallery renders API blocks only (no hardcoded 6 luxury placeholders)

## Target brand

- **WoodSpring Suites** · `woodspring-suites` · `recsOd51NzRPYsMko`

## Protected scope

Never modify:

- Company Validated / Company Validation Date
- Source Library approvals
- `footprint.momentum` rows
- Quarantined Everhome scenario row (`recrnaRxigUSoDDTJ`)
- Summary URL / View Summary URL (field does not exist)
- Everhome Suites / Suburban Studios

## Commands

```bash
# Dry-run (default)
npm run brand-explorer-woodspring-visual-registry-recovery-writer -- --brand woodspring-suites --dry-run

# Apply
npm run brand-explorer-woodspring-visual-registry-recovery-writer -- --brand woodspring-suites --apply \
  --approve-brand-explorer-v33D-R2-woodspring-visual-registry-recovery \
  --founder-approved-woodspring-official-images \
  --confirm-official-source-images-only \
  --confirm-no-company-validation-claim \
  --confirm-no-source-library-changes \
  --confirm-no-summary-url-field \
  --confirm-no-momentum-changes \
  --confirm-quarantined-everhome-row-stays-hidden \
  --confirm-woodspring-only
```

## Post-run validation

```bash
npm run brand-explorer-final-qa-auditor -- --brand woodspring-suites --dry-run
npm run brand-explorer-complete-build -- --brand woodspring-suites --dry-run --target-quality active-profile
npm run brand-explorer-visual-display-defect-audit -- --brand woodspring-suites --dry-run
```

## Outputs

- `reports/brand-explorer-woodspring-visual-registry-recovery-writer.json`
- `reports/brand-explorer-woodspring-visual-registry-recovery-writer.md`

## Rollback

Revert registry rows by staging run ID `v33D-R2-woodspring-visual-registry-recovery`.
Presentation gallery titles / visibility can be reverted per row if needed.
