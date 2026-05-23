# CALA demo sample — Mérida Centro Select-Service

**Fixture:** `fixtures/sample-deals/merida-centro-select-service.example.json`
**Sample ID:** `cala-merida-centro-select-service-001`

> Sample deal for product demonstration only. Reference properties are public comps for factual context only.

| Item | Value |
| --- | --- |
| **Fictional project** | Mérida Centro Select-Service |
| **Expected readiness** | Ready for External Review |
| **Primary reference** | Courtyard Mérida Downtown |
| **Target list count** | 7 |

## Reference URLs

1. https://www.marriott.com/en-us/hotels/midcy-courtyard-merida-downtown/overview/
2. https://www.hilton.com/en/hotels/midyagi-hilton-garden-inn-merida/hotel-info/
3. https://www.marriott.com/en-us/hotels/midcp-city-express-plus-merida/overview/

## Intentional gaps

| Field | Reason |
| --- | --- |
| Estimated or Actual RevPAR | Market demand support partially incomplete |
| Key Competitors | Comp set not fully modeled on deal record |

## Commands

```bash
node scripts/validate-sample-deal-fixture.mjs fixtures/sample-deals/merida-centro-select-service.example.json
node scripts/print-sample-deal-airtable-map.mjs fixtures/sample-deals/merida-centro-select-service.example.json
```
