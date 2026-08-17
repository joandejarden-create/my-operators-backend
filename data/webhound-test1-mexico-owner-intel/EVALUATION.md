# Webhound Test 1 Benchmark Evaluation — Mexico Owner Intelligence

**Session:** `551e2dd9-73c6-4319-90cf-210b47c86fba`  
**URL:** https://webhound.ai/session/551e2dd9-73c6-4319-90cf-210b47c86fba  
**Status:** completed (`budget_complete`)  
**Spend:** $4.88 of $5.00 (included free-run reserved/consumed)  
**Duration:** ~29 minutes (started ~14:37Z, last activity ~15:06Z)  
**Rows:** 6 · Expected fields: 143 · Fill rate: ~32%  
**Alert:** `dataset_low_fill_rate`  
**Governance:** Candidate intelligence only. Not ingested into Airtable / fixtures / Brand Explorer / Operator Explorer.

## Executive verdict

**Promising, But Needs Refinement**

Webhound demonstrated a useful discovery pattern (SEMARNAT MIA–linked hotel projects without public brand/operator), but failed the commercial actionability test for Dealality: no ownership chains (SPV → parent → ultimate), no verified contacts, sparse Tier 1 enrichment, and one clearly stale “Class 1” (Tulum 2020).

## Independent reclassification summary

| ID | Webhound | Independent | Disposition |
|----|----------|-------------|-------------|
| OLEUM-RIVM-ECOT-001 | Class 1 | Class 1 conditional | Research Further |
| RPV-QROO-VENADO-001 | Class 1 | Class 1 conditional | Research Further |
| HMP-QROO-PAST-001 | Class 1 | Class 1 fragile (CONANP) | Research Further |
| BPCS-BCS-LAPAZ-001 | Class 1 | Class 2 / Track | Track / Monitor |
| AFFP-BCS-CABO-001 | Class 2 | Class 2 on hold | Owner Relationship Opportunity |
| CBQR-TULM-INTL-001 | Class 1 | Not Class 1 | Too Late |

## Cost / value (actual $4.88)

- Cost / record: **$0.81**
- Cost / assessed credible Class 1 (3): **$1.63**
- Cost / Class 2: **$4.88**
- Cost / opportunity with relevant decision-maker + verified contact: **N/A (0)**
- **Cost per actionable pre-decision owner opportunity: failed this run (0)**

## Test 2 recommendation (do not launch)

**F + C hybrid:** Revised Mexico methodology focused on SEMARNAT cluster enrichment (Oleum + Venado ± Pastizales): ownership/SPV tracing + contacts + too-late disconfirmation. Budget $5–$10. Geography still Mexico (Playa del Carmen / Punta Venado corridor). No new run without explicit approval.

## Artifacts preserved

- `data/webhound-test1-mexico-owner-intel/session-meta.json`
- `data/webhound-test1-mexico-owner-intel/rows.jsonl`
- `data/webhound-test1-mexico-owner-intel/rows-compact.json`
- `data/webhound-test1-mexico-owner-intel/sources-summary.json`
