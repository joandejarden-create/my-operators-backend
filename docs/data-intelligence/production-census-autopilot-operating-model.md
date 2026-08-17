# Production Census Autopilot — Operating Model

## Founder input

1. Region (CALA) + optional country  
2. `--scope active-brand-setup` (preferred) or `--parent-company`  
3. `--strategy fastest-safe`  
4. `--run-until-complete` + `--batch-size`  
5. Mode: plan → controlled → apply  
6. Optional `--queue <id>` to target **one** queue only  

## Layers (source of truth)

| Layer | Role |
| --- | --- |
| Brand Setup Active/Live | Read-only **Brand Setup active control list** |
| **Hotel Property Census** (`tbl9aY5ijiuIzzWam`) | Production property data; **only Autopilot write target** |
| VIC source claims | Evidence / claim lineage only — **never write** |
| legacy Census | Not production — do not use unless migration/audit mode |
| Brand Explorer | Untouched |

Canonical config: `lib/research-engine-v2/production-census-source-of-truth.js`  
Doc: `docs/data-intelligence/production-census-source-of-truth.md`

Match summaries must say: **Matched Active / Live Brand Setup brands to production Hotel Property Census records.**

## Principles

1. Process full active-brand scope unattended  
2. Fastest-safe queue order (explainable scores)  
3. **No `--queue` ⇒ execute all eligible queues in fastest-safe order** (orchestrator)  
4. **`--queue X` ⇒ that queue only**  
5. High-confidence allowlisted **Hotel Property Census** writes only  
6. Checkpoint every batch; resume without chat copy-paste  
7. Exhausted / soft-deferred queues do **not** stop the run  
8. Hard cases route; do not stop whole run for provider soft-blocks  
9. Webhound = learning candidates only  
10. Controlled mode writes **one multi-queue approval bundle** and **never** Airtable  
11. Apply fails closed with `blocked_wrong_census_target` if write target ≠ Hotel Property Census  
12. `--queue source_discovery` discovers missing CALA properties (Mexico Hilton/Choice adapters ready), matches Hotel Property Census, and emits **insert** approval bundles only — no fuzzy auto-insert  

## Discovery mode

See `docs/data-intelligence/production-census-cala-discovery-mode.md`.  
Status: `production_census_cala_discovery_mode_ready_needs_source_adapter` (Marriott/IHG + non-Mexico CALA adapters pending).

## Queue orchestration (v1)

Module: `lib/research-engine-v2/census-autopilot-queue-orchestrator.js`

Default controlled path builds scope → match → score → execute adapters → skip exhausted → soft-defer geocode → emit:

- `queue-execution-report.{md,json}`
- `approval-bundle.json` (proposed_writes + proposed_writes_by_queue)
- `dry-run.json`

Apply consumes the bundle via `loadMultiQueueApprovalBundleProposals` (approval-bundle-bound).

## Runtime targets (design)

- Plan: &lt;2 min parent/region  
- Cached dry-run: &lt;10 min / 1k  
- Apply: 2–8 min / 100  
- Large parents: overnight OK if checkpoint/resume works  

## Completion statuses

`complete` · `partial_complete_resume_available` · `blocked_provider_decision` · `blocked_source_access` · `blocked_schema_needed` · `blocked_safety_failure` · `blocked_wrong_census_target`
