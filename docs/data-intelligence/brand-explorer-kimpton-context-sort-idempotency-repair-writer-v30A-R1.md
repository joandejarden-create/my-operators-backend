# Brand Explorer Kimpton Context + Sort Idempotency Repair v30A-R1

- Generated: 2026-07-10T11:17:38.444Z
- Brand: **Kimpton Hotels** (`kimpton`)
- v30A exists: **yes**
- v30A-R1 exists: **yes**
- Mode: **dry-run**
- Dry-run clean: **yes**
- Airtable modified: **no**
- Company Validated untouched: **yes**

## Portfolio Context diagnosis
- Record: `rec2VtIIPtFfX605Q`
- Issue class: **resolved**
- Root cause: **resolved_after_v30A_R1**
- Repair strategy: **fix_audit_and_ladder_mapping_only_no_airtable_rewrite**
- Copy is good: **yes**
- Body preview: Luxury & lifestyle flagship within IHG—Kimpton sits with InterContinental, Regent, and Six Senses at the experiential apex; above Hotel Indigo, voco, and Crowne

## Sort-order drift diagnosis
- Root cause: **v30A_proposeSortOrderRepairs_non_idempotent_explicit_key_match**
- Legacy v30A would update: **13** rows
- Idempotent would update: **0** rows

## Duplicate row findings
- standards.requirement: leave_alone (7 rows)
- commercial.theme: human_review_required (3 rows)

## Rows to update/create
- Create: 0
- Update: 0

## Expected Final QA after apply
- Overall: **85** (almost_ready)

## Blockers remaining after apply
- 44 pending facts — v30B stewardship required

## Code repairs included
- brand-explorer-portfolio-ladder-mapping.js — IHG parent ladder + sibling simulation
- brand-explorer-visual-display-defect-audit.js — IHG portfolio context defect gate
- brand-explorer-ihg-family-active-profile-repair-writer.js — idempotent sort repair skip when sort=0

## Exact apply command
`npm run brand-explorer-kimpton-context-sort-idempotency-repair-writer -- --brand kimpton --apply --approve-brand-explorer-v30A-R1-kimpton-context-sort-repair --founder-reviewed-kimpton-context-copy --confirm-no-company-validation-claim`