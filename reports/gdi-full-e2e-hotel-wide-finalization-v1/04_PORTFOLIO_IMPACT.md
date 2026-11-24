# Portfolio impact assessment

- Every future `runCanonicalFullE2eResearch` run now fails closed if hotel-wide reconciliation is incomplete.
- Hotels without STAGED continue to show empty local finalized inventory until founder activates an FQO candidate.
- Historical STAGED/CURRENT pointers are never mutated by this stage.
- Research-store PASS counts are an audit funnel; commercial revalidation / founder review still gate activation.
- Stacks with FQO presentation policy (PR #593).
