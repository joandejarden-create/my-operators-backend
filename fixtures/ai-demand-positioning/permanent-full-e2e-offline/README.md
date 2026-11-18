# Permanent Full E2E — Layer A offline fixtures

Synthetic, sanitized fixtures for CI. **Not production proof.**

- No live Railway volume, Airtable, or paid API dependency.
- Positive: measurement reconciliation from original response text.
- Negative: baked scenarioPresence mismatch, legacy 78 contamination, missing response text.
- Missing-artifact behavior is covered by the Layer B wrapper (EXIT 2 when durable volume absent).
