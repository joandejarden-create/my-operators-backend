# CALA demo sample — Cascadas Lifestyle Hotel Component

**Fixture:** `fixtures/sample-deals/cascadas-lifestyle-hotel-component.example.json`
**Sample ID:** `cala-cascadas-lifestyle-hotel-component-001`

> Sample deal for product demonstration only. Reference properties are public comps for factual context only.

| Item | Value |
| --- | --- |
| **Fictional project** | Cascadas Lifestyle Hotel Component |
| **Expected readiness** | Ready for External Review |
| **Primary reference** | Hyatt Centric San Salvador |
| **Target list count** | 7 |

## Reference URLs

1. https://www.hyatt.com/en-US/hotel/el-salvador/hyatt-centric-san-salvador/salcc
2. https://www.marriott.com/en-us/hotels/salsv-courtyard-san-salvador/overview/
3. https://www.marriott.com/en-us/hotels/salfi-fairfield-san-salvador/overview/

## Intentional gaps

| Field | Reason |
| --- | --- |
| Ownership Structure | Ownership / management structure needs validation |
| Preferred Third-Party Operators (names) | Operator responsibilities partially undefined |
| Key Competitors | Brand support for mixed-use value needs clarification |

## Commands

```bash
node scripts/validate-sample-deal-fixture.mjs fixtures/sample-deals/cascadas-lifestyle-hotel-component.example.json
node scripts/print-sample-deal-airtable-map.mjs fixtures/sample-deals/cascadas-lifestyle-hotel-component.example.json
```
