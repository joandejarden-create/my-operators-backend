# GDI Executive Summary — Research Receipt Funnel Metric Reconciliation

**Hotel:** Wyndham San Diego Bayside (`recnPB3Q02NO8Ma1o`)  
**STAGED tip:** `gdi_hotel_wide_fqo_recnPB3Q02NO8Ma1o_20261127_6e217b8a`  
**Serving commit:** post-merge tip for this PR

## Research productivity (119 → 85)

| Item | Value | Authority |
|---|---:|---|
| Prior uncertified stacked hours | 119 | Pre–overlap-prevention stack (discovery + deep + qualification double-count risk) |
| Reconciled estimated hours saved | **85** | Same benchmark contract; deep replaces discovery/qualification; pursuit excluded; 20% residual |
| Equivalent working days | **10.5** | 85 ÷ 8 hours/day, rounded to nearest half day |
| Benchmark methodology change | None | `gdi-research-effort-benchmark-contract-v1` unchanged |
| Customer label | Estimated | Replaces prominent orange Provisional Estimate copy; internal status remains `PROVISIONAL_NOT_CERTIFIED` |

## Research funnel (stage grain ≠ qualified inventory)

| Stage | Count | Source | Counting unit |
|---|---:|---|---|
| Potential Opportunities Identified | **161** | Fourteen-segment discovery matrix `summary.totalExtractedCandidates` | Unique discovery candidates |
| Opportunities Researched | **121** | `02-deep-research.json` `metrics.dossierCount` | Unique deep-research dossiers |
| Opportunities Validated | **omitted** | Commercial viability precheck equals researched (121/121 PROCEED) — not an independent milestone | — |
| Qualified Opportunities Delivered | **26** | Authorized STAGED publication inventory | Cumulative qualified opportunities |

## Supporting metrics

| Metric | Count | Grain |
|---|---:|---|
| Evidence Sources Reviewed | 22 | Unique evidence URLs on qualified rows |
| Commercial Contacts Identified | 40 | Unique verified people |
| Verified Contact Channels | 46 | Evidenced email + phone + LinkedIn channels across people |
| Opportunities Screened Out | omitted | No durable screening disposition ledger (not computed as 161 − 26) |
| New / Updated This Period | omitted when weekly comparison unavailable | Weekly delta markers |

## Non-equivalence note

161, 121, 26, 22, 40, and 46 are **not** interchangeable. Discovery candidates, researched dossiers, qualified inventory, evidence URLs, unique people, and contact channels are separate counting units.
