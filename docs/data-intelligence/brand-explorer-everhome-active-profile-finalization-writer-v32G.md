# Brand Explorer Everhome Active Profile Finalization Writer v32G

**Batch:** v32G  
**Brand:** Everhome Suites (`everhome-suites`, `recqkkrsevi4r9ibj`)  
**Default:** dry-run / read-only

## Purpose

Bridge Everhome from post-v32E-R1 copy readiness to active-profile by:

1. Auditing all visual presentation rows and registry assets
2. Mapping canonical registry assets to working presentation images
3. Reconciling duplicate registry rows (metadata notes only — no deletes)
4. Preserving working Image fields; materializing only when founder-approved + missing image
5. Reporting Final QA / Complete Build / openings / momentum readiness gaps

**Does not auto-approve registry assets.**

## Apply gates

```bash
npm run brand-explorer-everhome-active-profile-finalization-writer -- \
  --brand everhome-suites \
  --apply \
  --approve-brand-explorer-v32G-everhome-active-profile-finalization \
  --confirm-founder-approved-assets-only \
  --confirm-preserve-working-images \
  --confirm-no-company-validation-claim \
  --confirm-no-opening-or-momentum-copy-changes \
  --confirm-everhome-only
```

Apply is blocked when founder-approved assets are missing or active-profile gates would be forced without a real pass.

## Change impact

**High** — registry metadata writes; optional founder-approved image materialization only. Rollback via report patches list.

## Post-run validation

```bash
npm run brand-explorer-everhome-active-profile-finalization-writer -- --brand everhome-suites --dry-run
npm run brand-explorer-final-qa-auditor -- --brand everhome-suites --dry-run
npm run brand-explorer-complete-build -- --brand everhome-suites --dry-run --target-quality active-profile
npm run brand-explorer-visual-display-defect-audit -- --brand everhome-suites --dry-run
npm run test:partner-intelligence-publish-readiness
npm run test:partner-intelligence-profile-governance-publish
```

## Prerequisite

Founder must approve Everhome Brand Asset Registry rows (`Approved For Explorer` + `Usage Review Complete`) before active-profile can pass. v32G reports exactly which visual rows remain blocked.
