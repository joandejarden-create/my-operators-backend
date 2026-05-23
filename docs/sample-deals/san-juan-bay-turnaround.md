# CALA demo sample — San Juan Bay Turnaround

**Fixture:** `fixtures/sample-deals/san-juan-bay-turnaround.example.json`
**Sample ID:** `cala-san-juan-bay-turnaround-001`

> Sample deal for product demonstration only. Reference properties are public comps for factual context only.

| Item | Value |
| --- | --- |
| **Fictional project** | San Juan Bay Turnaround |
| **Expected readiness** | Advancing |
| **Primary reference** | Hyatt Place San Juan |
| **Target list count** | 6 |

## Reference URLs

1. https://www.hyatt.com/en-US/hotel/puerto-rico/hyatt-place-san-juan-bayamon/sjuzp
2. https://www.marriott.com/en-us/hotels/sjuan-ac-hotel-san-juan-condado/overview/
3. https://www.marriott.com/en-us/hotels/sjuis-courtyard-isla-verde-beach-resort/overview/

## Intentional gaps

| Field | Reason |
| --- | --- |
| Estimated or Actual RevPAR | Performance data incomplete |
| PIP / CapEx Status | PIP/capex budget unclear |
| Plan to Self-Manage or Hire Third Party? | Operator requirements not fully defined |
| Decision Timeline for Brand/Operator | Owner decision timeline uncertain |
| Preferred Third-Party Operators (names) | No operator shortlist |

## Commands

```bash
node scripts/validate-sample-deal-fixture.mjs fixtures/sample-deals/san-juan-bay-turnaround.example.json
node scripts/print-sample-deal-airtable-map.mjs fixtures/sample-deals/san-juan-bay-turnaround.example.json
```
