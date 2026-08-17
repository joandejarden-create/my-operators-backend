# Verified Census State Machine (V2.3)

## States
- `DISCOVERED`
- `IDENTITY VERIFIED`
- `VERIFIED — MATERIAL GAPS`
- `VERIFIED — ROOMS PENDING`
- `VERIFIED — FIRST-PARTY VALIDATION PENDING`
- `VERIFIED — GOLDEN COMPLETE`
- `RESEARCH ESCALATION`
- `INACTIVE / HISTORICAL`
- `IDENTITY CONFLICT`

## Transitions (conceptual)
DISCOVERED → IDENTITY VERIFIED (Exact/High independent existence)
IDENTITY VERIFIED → VERIFIED — MATERIAL GAPS | VERIFIED — ROOMS PENDING | VERIFIED — FIRST-PARTY VALIDATION PENDING | VERIFIED — GOLDEN COMPLETE
Any VERIFIED* → RESEARCH ESCALATION (contradiction / blocked)
Any → INACTIVE / HISTORICAL (closure)
Any → IDENTITY CONFLICT

## Rules
1. **Rooms is NOT required** for VERIFIED physical property status.
2. **Golden Complete** (≥95% Priority including Rooms) is independent of Verified existence.
3. Scores:
   - IDENTITY / EXISTENCE CONFIDENCE
   - GOLDEN CENSUS COMPLETENESS
4. Minimum identity gate: Exact or High independent confirmation + durable property_identity_id + country + non-Cvent provenance.

## V2.2 evidence
238 hotels otherwise Golden except Rooms → `VERIFIED — ROOMS PENDING` is operationally necessary.
