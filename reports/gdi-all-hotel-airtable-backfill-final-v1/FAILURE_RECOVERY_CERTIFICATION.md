# Failure recovery certification

- Per-row results recorded in BACKFILL_EXECUTION_RESULTS.json
- Composite key dedupe prevents duplicate creates on retry
- Failed rows do not mark overall success
- Re-run is safe: UNCHANGED for already-correct rows; CREATE only for missing
