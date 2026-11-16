# REGRESSION_QA

Run on `exec/adp-radisson-blu-toronto-customer-release-v1` @ `0504fe0de` (post merge-with-main).

| Gate | Result |
|------|--------|
| test:adp-period-family-resolution-v1 | **PASS** |
| test:adp-core-metric-reconciliation-v1 | **PASS** |
| test:adp-evidence-drawer-period-integrity-v1 | **PASS** |
| test:adp-loyalty-core-isolation-v1 | **PASS** |
| test:adp-toronto-forensic-reconciliation-v1 | **PASS** |
| test:adp-brand-portfolio-period-resolution-v1 | **PASS** |
| test:adp-brand-portfolio-rendering-v1 | **PASS** |
| test:adp-brand-portfolio-evidence-family-v1 | **PASS** |
| test:adp-brand-portfolio-founder-staged-v1 | **PASS** (customer published path) |
| test:adp-full-e2e-closeout-cli-v1 | **ABSENT** (script not in repo / package.json) |
| test:adp-local-dropdown-runtime-inventory-v1 | **ABSENT** (script not in repo / package.json) |

## Aggregate
**REGRESSION PASS: YES** (all present gates PASS; two named gates ABSENT and documented)
