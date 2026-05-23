# CALA demo sample — Colonial City Lifestyle Conversion

**Fixture:** `fixtures/sample-deals/colonial-city-lifestyle-conversion.example.json`
**Sample ID:** `cala-colonial-city-lifestyle-conversion-001`

> Sample deal for product demonstration only. Reference properties are public comps for factual context only.

| Item | Value |
| --- | --- |
| **Fictional project** | Colonial City Lifestyle Conversion |
| **Expected readiness** | Ready for External Review |
| **Primary reference** | Kimpton Las Mercedes Santo Domingo |
| **Target list count** | 7 |

## Reference URLs

1. https://www.ihg.com/kimpton/hotels/us/en/santo-domingo/sdqkm/hoteldetail
2. https://www.billinihotel.com/
3. https://www.hodelpa.com/en/hotels/nicolas-de-ovando/

## Intentional gaps

| Field | Reason |
| --- | --- |
| Soft vs Hard Brand Preference | Soft/hard tolerance unclear |
| F&B Program Type | Public space/F&B program partially incomplete |
| Proposal Deadline | Approval timeline needs validation |

## Commands

```bash
node scripts/validate-sample-deal-fixture.mjs fixtures/sample-deals/colonial-city-lifestyle-conversion.example.json
node scripts/print-sample-deal-airtable-map.mjs fixtures/sample-deals/colonial-city-lifestyle-conversion.example.json
```
