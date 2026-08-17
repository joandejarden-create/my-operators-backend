# Operator Intelligence — Airtable Schema Dry Run

**Applied:** `false`  
**Machine-readable:** `reports/operator-intelligence-airtable-schema-dry-run.json`

## Group A — Existing fields safe to populate (after approval)

| Op | Table | Field | Purpose |
| -- | ----- | ----- | ------- |
| populate | Platform | Active Countries | Arbor, GHL, Playa, Aimbridge structured geo |
| populate | Commercial | Management Structures Supported | GHL, Playa, Aimbridge |

## Group B — Normalization

| Op | Operator | Issue |
| -- | -------- | ----- |
| normalize | Cenote Azul | Multi-country Active Countries unsupported by public research |

## Group C — New claim/evidence architecture

Claims table · PI Source Library links · Case Study comparable metadata · Brand relationship verification fields.

**Do not execute.** Founder approval required before any create/populate.
