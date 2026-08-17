# Operator Explorer → Fit Data Gap (Diagnostic Only)

## Fit Data Ready diagnostic rule (unchanged)

`asg >= 6 && marketPresenceRows >= 3 && brandRelationships >= 2` → Fit Data Ready

Explorer Strong uses `asg >= 5 && countries >= 2 && brands >= 2`.

## Why Fit Data Ready stays at 4

Normalized Assignments / Presence / Brand Relationships **do reach** the Fit diagnostic (same counts power OE readiness).

The bottleneck is primarily **methodology/threshold mismatch**, not missing OE intelligence:

1. Fit Ready requires **asg ≥ 6** while Strong only needs **≥ 5** — 9 Strong profiles miss Fit Ready largely on this + BR/MP row thresholds
2. Fit Ready uses **Market Presence row count** and **Brand Relationship row count**, not the same country/brand-name diversity Strong uses
3. Several Fit factor domains (Ownership/Governance, Regional Resources, Commercial Differentiators) are **not fully mapped** from the new assignment spine into Fit inputs

### Strong but not Fit Ready (examples)

- **Hilton (Managed)**: asg=5, mpRows=2, brRows=3 → Conditional
- **Remington Hospitality**: asg=5, mpRows=2, brRows=2 → Conditional
- **IHG Hotels & Resorts (Managed)**: asg=5, mpRows=4, brRows=2 → Conditional
- **Accor (Managed)**: asg=5, mpRows=3, brRows=3 → Conditional
- **Aimbridge Hospitality (LATAM)**: asg=8, mpRows=2, brRows=6 → Conditional
- **Marriott International (Managed)**: asg=5, mpRows=3, brRows=3 → Conditional
- **Driftwood Hospitality Management**: asg=5, mpRows=4, brRows=2 → Conditional
- **Highgate**: asg=6, mpRows=5, brRows=1 → Conditional
- **Grupo Iberostar**: asg=5, mpRows=2, brRows=2 → Conditional

### Fit Data Ready operators

- Playa Hotels & Resorts (asg=6, mp=3, br=4)
- Arbor Lodging (CALA) (asg=11, mp=5, br=4)
- Hotel Equities (CALA) (asg=10, mp=10, br=4)
- GHL Hoteles (GHL Holding) (asg=6, mp=7, br=5)

## Answer

Fit Data Ready remains ~4 because **Fit readiness requirements/mappings are stricter and partially disconnected** from Explorer Strong semantics — not because Operator Explorer lacks operator intelligence.

This supports **Path A — Resume Operator Fit v2.1 targeted refinement** (remap Fit diagnostics to normalized OE entities; do not lower Explorer gates).

No Fit weights, geography scoring, CRI, or ranking changes in this phase.
