# 4. Bethesda 67-opportunity replay matrix (summary)

Full rows: `BETHESDA_67_REPLAY_PARITY_MATRIX.json`

| Metric | Value |
|--------|------:|
| Original IDs reconciled | **67** |
| New candidate count | **20** |
| Hotel-wide PASS dispositions | 28 |
| PREVIOUSLY_APPROVED_REVIEW | 38 |
| HOLD_RESEARCH | 20 |
| FQO pass on gold presentation (raw) | **0** |
| FQO pass after URL rehydration probe | **14** |
| PARITY_PASS | 0 |
| IMPROVED (retained w/ diffs) | 19 |
| TECHNICAL_REGRESSION | **46** |
| RESEARCH_COMPLETION_REQUIRED | 2 |
| GENUINE_COMMERCIAL_CHANGE | 0 |
| Primary lost in candidate | 0 |
| Secondary lost in candidate | 0 |

**Verdict:** Current Full E2E + FQO does **not** reproduce the approved Bethesda 67 commercial inventory. Failures are overwhelmingly technical (evidence serialization + historical compatibility + channel strictness), not proven commercial expirations.
