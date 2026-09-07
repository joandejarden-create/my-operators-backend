# 04 — Data Source Map

Primary application truth = **machine JSON**, not Markdown.

| UI field | Canonical source |
|---|---|
| This week paragraph | `reports/helena-cmo-manual-week-01/helena-cmo-manual-week-01.json` → `weeklyPack.sections.1_executive_cmo_summary` |
| Top 3 | same → `top3ThisWeek` |
| Commercial Tier 1/2 | same → `commercial_outcomes` + explicit DATA_GAP/UNKNOWN fillers |
| What is UNKNOWN | same → `unknowns` |
| Helena recommends | same → `threeThingsIfOnlyThree` |
| Measurement / attribution | same → `measurement_state`, `attribution_state` |
| ADP candidates | same → `adpPipeline.candidates` (classes only) |
| Live pilots | empty until OS/local instances exist |
| Safety meta | hard-coded OFF + optional `helena-cmo-operating-law-v1.json` version |
| Decision/approval status overrides | `data/helena-cmo/founder-console-actions.json` (local; gitignored) |
| Markdown reports | Evidence tab provenance only |

## Explicit non-sources

- Do not invent named hotels/CRM accounts
- Do not treat GA4 sessions / LinkedIn likes as Tier 1 success
- Do not duplicate Marketing OS into a second truth store (local log is temporary UI state only)
