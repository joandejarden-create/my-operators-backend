# Eligibility implementation details

Uses existing **Finalized Qualified Opportunity** contract (six gates):

1. Evidence-backed demand  
2. Hotel geo/commercial fit  
3. Future sourcing pathway  
4. Named commercially relevant person  
5. Person-specific professional channel  
6. Hotel-specific next action  

Competitive adds incumbent + overflow/future-cycle pathway.

## Dispositions

| Disposition | Meaning |
|-------------|---------|
| `FINALIZATION_PASS` | All FQO gates pass |
| `HOLD_RESEARCH` | Relevant but unresolved gaps (reason codes + remediation) |
| `REJECT` | Disqualified / founder-rejected |
| `HISTORICAL` | Past demand without future pathway |
| `PREVIOUSLY_APPROVED_REVIEW` | In prior STAGED/CURRENT but FQO now fails — founder attention |

Sales Ready is recorded but **not required**.
