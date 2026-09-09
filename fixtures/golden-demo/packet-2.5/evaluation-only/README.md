# Evaluation-only — Packet 2.5 / 2.5B gold sets

These files are **frozen evaluation truth** for claim extraction / entity resolution / relationship reconciliation.

| File | Packet | Purpose |
|------|--------|---------|
| `gold-claims-v1.json` | 2.5 / 2.5A | Expected accepted claims |
| `gold-entity-mappings-v1.json` | 2.5B | Expected entity mention → canonical id mappings |

**Pipeline execution MUST NOT load, import, or read these files.**

Unlock only in:
- `scripts/test-packet-2.5-claim-engine.mjs`
- `scripts/test-packet-2.5a-precision.mjs`
- `scripts/test-packet-2.5b-entity-resolution.mjs`
