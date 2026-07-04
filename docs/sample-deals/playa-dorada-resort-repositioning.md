# CALA demo sample — Playa Dorada Resort Repositioning

**Fixture:** `fixtures/sample-deals/playa-dorada-resort-repositioning.example.json`
**Sample ID:** `cala-playa-dorada-resort-repositioning-001`

> Sample deal for product demonstration only. Reference properties are public comps for factual context only.

| Item | Value |
| --- | --- |
| **Fictional project** | Playa Dorada Resort Repositioning |
| **Expected readiness** | Ready for External Review |
| **Primary reference** | W Punta Cana, Adult All-Inclusive |
| **Target list count** | 7 |

## Reference URLs

1. https://www.marriott.com/en-us/hotels/pujwh-w-punta-cana-adult-all-inclusive-resort/overview/
2. https://www.hyatt.com/en-US/hotel/dominican-republic/hyatt-ziva-cap-cana/pujif
3. https://www.lopesan.com/en/hotels/dominican-republic/costa-bavaro/

## Intentional gaps

| Field | Reason |
| --- | --- |
| Plan to Self-Manage or Hire Third Party? | All-inclusive operator role needs clarification |
| PIP / CapEx Status | PIP scope needs validation |
| Soft vs Hard Brand Preference | Brand standards tolerance unclear |
| Group vs Transient Mix | AI model mix assumptions incomplete |

## Commands

```bash
node scripts/validate-sample-deal-fixture.mjs fixtures/sample-deals/playa-dorada-resort-repositioning.example.json
node scripts/print-sample-deal-airtable-map.mjs fixtures/sample-deals/playa-dorada-resort-repositioning.example.json
```
