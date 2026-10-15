# AI Demand Admin table parity (My Deals pattern)

## Result

Shared `DealalityAiAdminTable` pattern applied across AI Demand Admin tabs without redesigning My Deals or customer GDI cards.

| Capability | Reviews | ADP Action Plan | GDI Reports | Report Archive |
|------------|---------|-----------------|-------------|----------------|
| Sortable headers (typed sort) | Yes | Hotel picker sort preserved | Yes | Yes |
| Location column | Yes | — | Yes | Yes |
| Reset View | Yes | — | Yes | Yes |
| Result count | Yes | Counts tiles | Yes | Yes (versions) |
| Row selection | Yes | — | Yes | — |
| Compact sticky Actions | Yes | — | Yes (fixes clip) | Yes |
| Safe bulk | Selection UI | — | Missing PDFs + CSV export | — |

## Metric semantics (GDI)

Ready / Action Set / Watch = **current GDI report-data executive summary** counts from `getGdiPdfReportData`, reconciled with table rows via `normalizeGdiAdminReportRow`. Last Generated = PDF store time. Not renamed to imply live list inventory equivalence.

## Planner discovery (research path)

Extended LMC (`v1.2`) with professional planner specialties, relationship status (existing / previously pursued / not yet confirmed), influence roles, and DG-not-PE-venue persist. Directory/top-list alone fails. Deduped under `PROFESSIONAL_PLANNERS_INTERMEDIARIES` subsegment. No separate pipeline.

## Tests

- `node scripts/test-admin-ai-demand-table-v1.mjs` — PASS
- `node scripts/test-gdi-local-meetings-catering-research-v1.mjs` — PASS (includes planner)

## Local / production

See `VERIFICATION.md` for :8080 and production auth checks after deploy.

## Unavailable backend capability

- “Select all filtered results across pages” as a distinct server-side operation — UI retains page selection vs filtered set; GDI catalog is typically one page.
- Live published-prospect count column separate from report-data Ready — not dual-wired in this pass; metric note documents the report-data source instead of fabricating a second count.
