# Market Presence Cliff Analysis

No geography weight retune. Diagnostic only.

| From | To | Elig before → after | Verdict | Detail |
| ---- | -- | ------------------- | ------- | ------ |
| Strategic Interest | Current Managed Property | Not Currently Eligible → Eligible With Conditions | Correct eligibility behavior | Strategic Interest → Current Managed Property correctly unlocks geographic eligibility. |
| Claimed Capability | Current Operating Portfolio | Not Currently Eligible → Eligible With Conditions | Correct eligibility behavior | Claimed Capability → Current Operating Portfolio correctly unlocks geographic eligibility. |
| Historical Presence | Current Managed Property | Not Currently Eligible → Eligible With Conditions | Correct eligibility behavior | Historical Presence → Current Managed Property correctly unlocks geographic eligibility. |
| Current Managed Property | Strategic Interest | Eligible With Conditions → Not Currently Eligible | Correct eligibility behavior | Loss of strong presence (Current Managed Property → Strategic Interest) correctly removes eligibility. |
| Active Development | Active Development | — → — | No cliff | No eligibility-strength transition. |

## Verdict

Observed cliffs between weak types (Strategic Interest / Historical / Claimed Capability) and strong types (Current Managed / Operating / Regional Office) represent **Correct eligibility behavior**, not scoring defects.
