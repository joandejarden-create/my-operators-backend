# Brand Explorer Kimpton Portfolio Context Gate Reconciliation v30A-R2

- Generated: 2026-07-10T12:25:04.096Z
- Brand: **Kimpton Hotels** (`kimpton`)
- v30A-R2 exists: **yes**
- Mode: **dry-run**
- Root cause: **resolved_after_v30A_R2**
- Issue class: **resolved**
- Row content missing: **no**
- Audit logic stale: **no**
- Code fix deployed: **yes**
- Gates agree: **yes**
- Airtable modified: **no**
- Company Validated untouched: **yes**

## Portfolio context row
- Record: `rec2VtIIPtFfX605Q`
- Parent (API): **InterContinental Hotels Group**
- Copy is good: **yes**
- Body preview: Luxury & lifestyle flagship within IHG—Kimpton sits with InterContinental, Regent, and Six Senses at the experiential apex; above Hotel Indigo, voco, and Crowne

## Gate comparison
- Ladder parentPortfolioReady: **true**
- Visual missing_peer: **false**
- Final QA missing_peer: **false**
- Complete Build blocker: **false**

## Expected after fix
- Final QA: **99** (ready)
- Complete Build ready: **yes**

## Code repairs
- isIhgParent() recognizes InterContinental Hotels Group (aligns with atelier isIhgParentCompanyKey)
- evaluatePortfolioContextGate() shared across visual audit reconstruct + defect gate
- Final QA visual audit call prefers recordId over slug

## Apply command
`npm run brand-explorer-kimpton-portfolio-context-gate-reconciliation-writer -- --brand kimpton --apply --approve-brand-explorer-v30A-R2-kimpton-portfolio-context-gate-reconciliation --confirm-no-company-validation-claim`