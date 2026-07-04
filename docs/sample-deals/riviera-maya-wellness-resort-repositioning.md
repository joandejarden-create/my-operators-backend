# CALA demo sample — Riviera Maya Wellness Resort Repositioning

**Fixture:** `fixtures/sample-deals/riviera-maya-wellness-resort-repositioning.example.json`
**Sample ID:** `cala-riviera-maya-wellness-resort-repositioning-001`

> Sample deal for product demonstration only. Reference properties are public comps for factual context only.

| Item | Value |
| --- | --- |
| **Fictional project** | Riviera Maya Wellness Resort Repositioning |
| **Expected readiness** | Advancing |
| **Primary reference** | Nômade Tulum |
| **Target list count** | 7 |

## Reference URLs

1. https://www.nomade-tulum.com/
2. https://www.betulum.com/
3. https://www.ourhabitas.com/tulum/

## Intentional gaps

| Field | Reason |
| --- | --- |
| Estimated or Actual RevPAR | Current operating performance incomplete |
| PIP / CapEx Status | Capex/PIP budget incomplete |
| Soft vs Hard Brand Preference | Brand standardization tolerance unclear |
| Plan to Self-Manage or Hire Third Party? | Operator role needs validation |

## Commands

```bash
node scripts/validate-sample-deal-fixture.mjs fixtures/sample-deals/riviera-maya-wellness-resort-repositioning.example.json
node scripts/print-sample-deal-airtable-map.mjs fixtures/sample-deals/riviera-maya-wellness-resort-repositioning.example.json
```
