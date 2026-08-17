# Brand Explorer Radisson Individuals Final Presentation Quality Sync v31N

- Generated: 2026-07-11T00:41:15.771Z
- Mode: **apply**
- v31N exists: **yes**

## Score mismatch root cause
- Classification: **orchestrator_using_old_scoring_function**
- Summary: Fixed: record_id Final QA resolution now maps discovery-config expansion brands to expansion_backlog scoring scope (skips active-registry gallery title-only penalties).

## Scores compared
- Final QA (slug): presentation 100, overall ready
- Final QA (record ID): presentation 100, overall ready
- Complete Build embedded: presentation 100, ready true

## overview.featured_application
- Word count before: 17
- Copy upgrade needed: yes

## Expected after fix
- Final QA: {"requiredSectionReadinessScore":100,"presentationQualityScore":100,"brandCarryoverRiskScore":100,"sourceGovernanceScore":100,"visualCompletenessScore":100,"overallNumeric":100,"overallActiveProfileReadiness":"ready"}
- Complete Build: {"finalQaScores":{"requiredSectionReadinessScore":100,"presentationQualityScore":100,"brandCarryoverRiskScore":100,"sourceGovernanceScore":100,"visualCompletenessScore":100,"overallNumeric":100,"overallActiveProfileReadiness":"ready"},"readyForActiveProfile":true,"readinessBand":"ready"}

## Apply command
```bash
npm run brand-explorer-radisson-individuals-final-presentation-quality-sync-writer -- --brand radisson-individuals-by-choice --apply --approve-brand-explorer-v31N-final-presentation-quality-sync --confirm-no-company-validation-claim --confirm-no-image-or-opening-changes
```
