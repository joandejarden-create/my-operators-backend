# Remaining recovery actions

## Done in this PR (no data mutation)

- Shared Research Preview filter bypass
- Integrity diagnostic + reconciliation reports
- Deterministic regression gate

## Not required (data already present)

- No research-store restore / rewrite
- No STAGED/CURRENT pointer rewrite
- No Airtable backfill
- No re-research

## Optional founder follow-ups

1. Soft-refresh local GDI UI (hard reload) on Research Preview for Guadalajara — expect **43**, not 6.
2. Use Staged Preview explicitly to review Guadalajara STAGED **38**.
3. Decide later whether any customer-facing promotion of currently non-facing research rows is desired (separate publication workflow — not part of this recovery).
4. Consider extending integrity warnings into the founder UI chrome (source + count + “N hidden by customer-facing rules if applied”) — optional UX polish.
