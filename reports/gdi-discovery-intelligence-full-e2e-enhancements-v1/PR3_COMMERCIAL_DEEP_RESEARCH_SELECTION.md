# PR3 — Commercial Deep-Research Selection

## Delivered

- Transparent multi-dimension selection scores (weights documented; combined with existing `scoreDeepResearchInvestment`)
- Two-pass allocation: fair segment floor → commercial fill
- Research tier routing (Tier 2 vs Tier 3 targeted)
- Budget-aware expected-cost gating
- Selection register answering why candidate N was selected vs deferred
- Persisted as `deep-research-selection-register.json` from the original chain

## Modules

- `lib/.../research/gdi-deep-research-selection-register-v1.js`
- Chain wiring in `original-full-e2e-research-chain-v1.js`
- `scripts/test-gdi-deep-research-selection-register-v1.mjs`

## Notes vs Bethesda live baseline

Deterministic ranking improvements are certified here. Live yield improvement requires a separate founder-authorized Exa comparison run — not claimed in this PR.

## Acceptance

| Criterion | Status |
|---|---|
| Ranking deterministic | Yes |
| Fair coverage where eligible exist | Yes |
| Budget limits respected (expected) | Yes |
| Reasons recorded | Yes |
| No paid research | Yes |
| Fingerprint unchanged | Yes |
