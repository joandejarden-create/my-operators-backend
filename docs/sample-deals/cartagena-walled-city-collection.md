# CALA demo sample — Cartagena Walled City Collection

**Fixture:** `fixtures/sample-deals/cartagena-walled-city-collection.example.json`
**Sample ID:** `cala-cartagena-walled-city-collection-001`

> Sample deal for product demonstration only. Reference properties are public comps for factual context only.

| Item | Value |
| --- | --- |
| **Fictional project** | Cartagena Walled City Collection |
| **Expected readiness** | Ready for External Review |
| **Primary reference** | Hotel Casa San Agustin |
| **Target list count** | 7 |

## Reference URLs

1. https://www.hotelcasasanagustin.com/
2. https://www.sofitellegendsantaclara.com/
3. https://www.charlestonhotels.com.co/hoteles/charleston-santa-teresa/

## Intentional gaps

| Field | Reason |
| --- | --- |
| PIP Budget Range (if conversion) | Capex/PIP range needs validation |
| Key Competitors | Competitive set incomplete |
| Estimated or Actual RevPAR | Performance data partial |

## Commands

```bash
node scripts/validate-sample-deal-fixture.mjs fixtures/sample-deals/cartagena-walled-city-collection.example.json
node scripts/print-sample-deal-airtable-map.mjs fixtures/sample-deals/cartagena-walled-city-collection.example.json
```
