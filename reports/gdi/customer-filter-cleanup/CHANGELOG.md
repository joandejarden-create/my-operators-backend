# CHANGELOG — Customer GDI filter cleanup

- Removed pursuit-management chips from customer-facing GDI workflow nav: Active Pursuits, Follow-Up Due, Hotel Selection, Closed.
- Restored top-level filters to All / Ready / Watching only (`workflowPresetHtml`).
- Coerce stale pursuit workflow filter values to All (`normalizeCustomerWorkflowFilter`).
- Restyle workflow row to shared `chain-scale-legend` language; add `.gdi-workflow-filter-row` spacing.
- Cache-bust GDI HTML assets (`gdi-customer-filter-cleanup-v1`).
- Pursuit model, APIs, records, panel, and Start/View Pursuit actions unchanged.
- Ready / Watch qualification thresholds unchanged.
