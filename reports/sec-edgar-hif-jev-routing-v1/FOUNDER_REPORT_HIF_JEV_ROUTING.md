# FOUNDER REPORT — HIF Jev Research Routing Evaluation

Generated: 2026-11-21T01:42:42.795Z

## 1. PR / implementation

- Extends **PR #503** (`feat/hotel-intelligence-factory-v5`)
- Reuses GDI TypeSafe System One stack:
  - `lib/group-demand-intelligence/jev/jev-client.js`
  - `lib/group-demand-intelligence/jev/jev-decision-service.js`
  - `lib/group-demand-intelligence/jev/jev-config.js` (auth: `JEZ_API_KEY` | `JEV_API_KEY` | `TYPESAFE_API_KEY`)
- New HIF wrapper only: `lib/hotel-intelligence/sec-edgar/hif-jev-research-routing-v1.js`
- Shared decision type added: `HIF_CONTRACT_RESEARCH_ESCALATION`
- Optional factory stage: `JEV_RESEARCH_ROUTING` (not in default high-volume sequences)
- **No duplicate provider adapter**

## 2. Hard forbids (enforced)

Jev must not and does not in this design:

- write contractual facts
- declare `AGREEMENT_EXISTENCE` / `CONTRACT_TERMS` / `CURRENT_STATUS` verified
- authorize HPC creates/updates
- publish customer intelligence

Default mode remains **shadow** (`GDI_JEV_MODE` default). Apply routing requires explicit `forceApplyRouting` + founder approval.

## 3. Typed actions evaluated

`ACCEPT_EXISTING_EVIDENCE` · `SEARCH_ADDITIONAL_SOURCES` · `INVESTIGATE_WITH_EXA_AGENT` · `VERIFY_CONTRACT_DATE` · `VERIFY_RENEWAL_STATUS` · `VERIFY_OWNER_OPERATOR` · `HOLD_INSUFFICIENT_EVIDENCE` · `STOP_LOW_VALUE_RESEARCH`

## 4. Live shadow results (30-case Agent pilot cohort)

| Router | Oracle match % | Agent escalations | Precision % | Recall % | Est. Agent $ if followed |
|---|---:|---:|---:|---:|---:|
| alwaysAgent | 3.3 | 30 | 6.7 | 100 | 0.75 |
| deterministic | 26.7 | 22 | 9.1 | 100 | 0.55 |
| jevShadow | 16.7 | 0 | n/a | 0 | 0 |

- Jev eval spend: **~$0.0008** (30 System One calls under $1 ceiling)
- Jev choice mix: VERIFY_RENEWAL_STATUS (10), VERIFY_OWNER_OPERATOR (9), VERIFY_CONTRACT_DATE (5), SEARCH_ADDITIONAL_SOURCES (4), null/fallback (2)
- **Jev never selected `INVESTIGATE_WITH_EXA_AGENT`** — it preferred cheaper VERIFY_*/SEARCH first
- For the one high-value Agent win (Hampton Inn Joplin), Jev selected `VERIFY_RENEWAL_STATUS` (conf ~0.81), not Agent

## 5. Interpretation vs commercial objective

Commercial objective = independently verified Brand Conversion / Management Contract targets with credible termination dates and notice periods.

- **Always-Agent** wastes ~$0.75 Agent budget for 6.7% precision.
- **Deterministic** cuts Agent calls to 22 and improves oracle match to 26.7%, still over-escalates Cohort A to Agent.
- **Jev shadow** avoids Agent entirely in this sample (good for cost) but **under-recalls** Agent-needed wins (0% Agent recall) and has **worse oracle match than deterministic** (16.7% vs 26.7%).
- Jev’s cheaper-first bias is directionally useful as an *advisory* hint, but on this measured sample it does **not** increase qualified targets per dollar versus deterministic gating of Agent.

## 6. Recommendation

- **Policy: `DETERMINISTIC_ONLY`**
- **Auto-enable Jev apply routing: false**
- Keep Jev wired as **optional shadow comparator** for HIF escalation (reuse GDI client)
- Default production path: deterministic router → Search/EDGAR verify → Agent only when deterministic says `INVESTIGATE_WITH_EXA_AGENT` after cheap paths / unresolved high-value status
- Revisit Jev apply only if a later sample shows Jev improving Agent precision **and** retaining recall of termination/notice wins at acceptable System One cost

## 7. Approvals required before Jev apply routing

- Founder override despite insufficient measured benefit on this pilot
- Explicit HIF Jev budget ceiling (separate from Exa Agent $15 and V5 Search $25)
- Confirmation write/HPC/publication forbids remain in place
- IC merge of PR #503 (product chat does not merge)

## 8. Artifacts

- `reports/sec-edgar-hif-jev-routing-v1/ROUTING_CONTRACT.json`
- `reports/sec-edgar-hif-jev-routing-v1/ROUTING_EVAL_ROWS.json`
- `reports/sec-edgar-hif-jev-routing-v1/ROUTING_EVAL_SUMMARY.json`
- `reports/sec-edgar-hif-jev-routing-v1/FOUNDER_REPORT_HIF_JEV_ROUTING.md`
