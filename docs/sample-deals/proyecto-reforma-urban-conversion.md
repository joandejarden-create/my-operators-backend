# CALA demo sample — Proyecto Reforma Urban Conversion

**Fixture:** `fixtures/sample-deals/proyecto-reforma-urban-conversion.example.json`
**Sample ID:** `cala-proyecto-reforma-urban-conversion-001`

> Sample deal for product demonstration only. Reference properties are public comps for factual context only.

| Item | Value |
| --- | --- |
| **Fictional project** | Proyecto Reforma Urban Conversion |
| **Expected readiness** | Advancing |
| **Primary reference** | Downtown Mexico, a Member of Design Hotels |
| **Target list count** | 7 |

## Reference URLs

1. https://www.hilton.com/en/hotels/mexubqq-umbral-curio-collection/
2. https://www.circulomexicano.com/
3. https://www.designhotels.com/hotels/2284-downtown-mexico

## Intentional gaps

| Field | Reason |
| --- | --- |
| PIP / CapEx Status | Capex/PIP budget incomplete |
| PIP Budget Range (if conversion) | Conversion budget range not finalized |
| Soft vs Hard Brand Preference | Strategic preference still open |
| Key Competitors | Competitive set incomplete |

## Commands

```bash
node scripts/validate-sample-deal-fixture.mjs fixtures/sample-deals/proyecto-reforma-urban-conversion.example.json
node scripts/print-sample-deal-airtable-map.mjs fixtures/sample-deals/proyecto-reforma-urban-conversion.example.json
```
