# Minimal integration recovery plan

## Ship now (this PR)

1. Hydrate contact names onto `chain.processed` rows.
2. Defensive primaryContact attach from eligible classified people.
3. Scorecard counts hydrated names + salesReadyContacts.
4. Deterministic fixture tests (positive recover / negative reject).

## Next (founder-gated, no broad rediscovery)

1. Re-render PR #382 scorecard from preserved state (`$0`) after merge.
2. Optionally wire `gdi-workflow.ps1` Research/FullRun to canonical entry (retire parallel legacy path).
3. Evaluate Scaling Companies buyer-intel hook only for `corporate_accounts_scaling` if generic restore pass remains thin.
4. Separate paid Exa wave only after named entry points are visible and demand-thesis quality is the remaining bottleneck.

## Explicitly out of scope

- Rebuilding contact modules that already exist
- Re-enabling Surfe inside Full E2E
- Weakening sales-ready gates
- Recurring Airtable persistence
