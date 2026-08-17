# Brand Explorer Everhome Final Gate Backfill Writer v32H

**Batch:** v32H  
**Brand:** Everhome Suites (`everhome-suites`, `recqkkrsevi4r9ibj`)  
**Default:** dry-run / read-only

## Purpose

After v32G-R1 image governance alignment, Everhome remains blocked on non-image gates. v32H:

1. Backfills **Portfolio Mix** chips (`footprint.portfolio_mix`)
2. Adds **Standard Detail governance** rows (`standards.last_reviewed`, `standards.source_confidence`)
3. Backfills **Value Creation scenario** bodies (`valueOwners.scenario.*`)
4. Stewards **pending Explorer facts** (approve safe public facts only)
5. Fixes shared **wrong-brand false positives** for current-brand aliases in `detectWrongBrandSignageRisk`

**Does not touch** presentation Image fields, openings/momentum structure, or Company Validated.

## Apply gates

```bash
npm run brand-explorer-everhome-final-gate-backfill-writer -- \
  --brand everhome-suites \
  --apply \
  --approve-brand-explorer-v32H-everhome-final-gate-backfill \
  --confirm-no-company-validation-claim \
  --confirm-no-image-field-changes \
  --confirm-no-opening-or-momentum-structure-changes \
  --confirm-everhome-only
```

## Change impact

**High** — presentation row creates/patches + fact stewardship + shared QA code fix. Rollback via report patch lists.

## Post-run validation

```bash
npm run brand-explorer-everhome-final-gate-backfill-writer -- --brand everhome-suites --dry-run
npm run brand-explorer-final-qa-auditor -- --brand everhome-suites --dry-run
npm run brand-explorer-complete-build -- --brand everhome-suites --dry-run --target-quality active-profile
npm run brand-explorer-visual-display-defect-audit -- --brand everhome-suites --dry-run
npm run test:partner-intelligence-publish-readiness
npm run test:partner-intelligence-profile-governance-publish
```
