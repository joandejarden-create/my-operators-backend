# CALA demo sample — Andean Business Hotel Reflag

**Fixture:** `fixtures/sample-deals/andean-business-hotel-reflag.example.json`
**Sample ID:** `cala-andean-business-hotel-reflag-001`

> Sample deal for product demonstration only. Reference properties are public comps for factual context only.

| Item | Value |
| --- | --- |
| **Fictional project** | Andean Business Hotel Reflag |
| **Expected readiness** | Ready for External Review |
| **Primary reference** | Courtyard Bogotá Airport |
| **Target list count** | 7 |

## Reference URLs

1. https://www.marriott.com/en-us/hotels/bogcy-courtyard-bogota-airport/overview/
2. https://www.hilton.com/en/hotels/bogapgi-hilton-garden-inn-bogota-airport/hotel-info/
3. https://www.marriott.com/en-us/hotels/limac-ac-hotel-lima-miraflores/overview/

## Intentional gaps

| Field | Reason |
| --- | --- |
| Has there ever been a franchise, branded management, affiliation or similar agreement pertaining to the proposed hotel or site? | Brand agreement history needs validation |
| Primary Demand Drivers Other | Corporate account detail incomplete |
| PIP Budget Range (if conversion) | PIP budget partially defined only |

## Commands

```bash
node scripts/validate-sample-deal-fixture.mjs fixtures/sample-deals/andean-business-hotel-reflag.example.json
node scripts/print-sample-deal-airtable-map.mjs fixtures/sample-deals/andean-business-hotel-reflag.example.json
```
