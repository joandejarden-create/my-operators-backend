# Operator Fit Internal Pilot — Ranking Stability

Deal: **Deal A**

## Baseline ranks

1. Highgate — alignment 40.6 · Strong
2. GHL Hoteles (GHL Holding) — alignment 37.4 · Strong

## Controlled tests

### unknown_becomes_verified_positive

- Expected: Modest alignment/confidence uplift; rank change only if near peer
- Observed / note: Document for founder — no automatic weight retune
- Flag: None

### positive_becomes_unsupported

- Expected: Confidence drop; possible eligibility loss if Market Presence weakened
- Observed / note: Market Presence eligibility is binary for geography — material impact expected
- Flag: Watch disproportionate eligibility cliffs on presence type changes

### source_becomes_stale

- Expected: Confidence / publication labels degrade; score may hold if factors unchanged
- Observed / note: Confidence channel should move before raw alignment
- Flag: None

### geography_relationship_changes

- Expected: Eligibility change can remove from Ranking Ready entirely
- Observed / note: Proportional for gate; high rank volatility if only 2 candidates
- Flag: Thin universes amplify relative rank volatility

### brand_relationship_verified

- Expected: Alignment uplift on brand factors; rarely flips eligibility alone
- Observed / note: Limited today due to brand-approval depth gap
- Flag: Under-responsive until brand-operator relationships enriched

### management_structure_known

- Expected: Eligibility unlock when structures were missing
- Observed / note: Material for Conditionally Rankable → Ranking Ready
- Flag: None

- Do not auto-tune weights from pilot optics.
- Eligibility cliffs (Market Presence) are intentional — document, don’t soften for pilot.
