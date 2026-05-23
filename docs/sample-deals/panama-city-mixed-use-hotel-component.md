# CALA demo sample — Panama City Mixed-Use Hotel Component

**Fixture:** `fixtures/sample-deals/panama-city-mixed-use-hotel-component.example.json`
**Sample ID:** `cala-panama-city-mixed-use-hotel-component-001`

> Sample deal for product demonstration only. Reference properties are public comps for factual context only.

| Item | Value |
| --- | --- |
| **Fictional project** | Panama City Mixed-Use Hotel Component |
| **Expected readiness** | Advancing |
| **Primary reference** | W Panama |
| **Target list count** | 7 |

## Reference URLs

1. https://www.marriott.com/en-us/hotels/ptywh-w-panama/overview/
2. https://www.marriott.com/en-us/hotels/ptyjw-jw-marriott-panama/overview/
3. https://www.marriott.com/en-us/hotels/ptyri-residence-inn-panama-city/overview/

## Intentional gaps

| Field | Reason |
| --- | --- |
| Current Form of Site Control | Site control and phasing need validation |
| Zoning Status | Residential/retail integration assumptions incomplete |
| Plan to Self-Manage or Hire Third Party? | Operator role not fully confirmed |
| Key Competitors | Brand RE value discussion incomplete |

## Commands

```bash
node scripts/validate-sample-deal-fixture.mjs fixtures/sample-deals/panama-city-mixed-use-hotel-component.example.json
node scripts/print-sample-deal-airtable-map.mjs fixtures/sample-deals/panama-city-mixed-use-hotel-component.example.json
```
