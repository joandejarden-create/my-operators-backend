# Certification Gap

Original period was CERTIFIED while:

- raw competitor mentions ≫ 0
- customer competitive universe = 0
- top displacement named in e2e pack (analytical path) but customer displacement blank
- `inGovernedRegistry: false` already disclosed in monthly-review eligibility

## New invariants added
1. `competitive_universe_integrity` — FAIL if bindable raw competitors > 0 but customer universe empty; FAIL if raw mentions high but zero bindable (missing registry)
2. `displacement_top_alternative_parity` — FAIL if universe has non-subject rows but top-alt/displacement missing when expected

These are shared gates in `run-property-certification.js`.
