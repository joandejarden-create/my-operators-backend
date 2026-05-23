# CALA demo sample — Cusco Heritage Palace Hotel

**Fixture:** `fixtures/sample-deals/cusco-heritage-palace-hotel.example.json`
**Sample ID:** `cala-cusco-heritage-palace-hotel-001`

> Sample deal for product demonstration only. Reference properties are public comps for factual context only.

| Item | Value |
| --- | --- |
| **Fictional project** | Cusco Heritage Palace Hotel |
| **Expected readiness** | Advancing |
| **Primary reference** | Palacio del Inka, a Luxury Collection Hotel |
| **Target list count** | 6 |

## Reference URLs

1. https://www.marriott.com/en-us/hotels/cuzlc-palacio-del-inka-a-luxury-collection-hotel/overview/
2. https://www.marriott.com/en-us/hotels/cuzjw-jw-marriott-el-convento-cusco/overview/
3. https://www.belmond.com/hotels/south-america/peru/cusco/belmond-hotel-monasterio/

## Intentional gaps

| Field | Reason |
| --- | --- |
| PIP / CapEx Status | Capex/PIP range incomplete |
| PIP Budget Range (if conversion) | PIP range not finalized |
| Plan to Self-Manage or Hire Third Party? | Operating model needs validation |
| Regulatory or Permitting Issues? | Heritage approvals not fully documented |

## Commands

```bash
node scripts/validate-sample-deal-fixture.mjs fixtures/sample-deals/cusco-heritage-palace-hotel.example.json
node scripts/print-sample-deal-airtable-map.mjs fixtures/sample-deals/cusco-heritage-palace-hotel.example.json
```
