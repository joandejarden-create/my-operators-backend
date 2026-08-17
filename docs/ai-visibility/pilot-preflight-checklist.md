# Brand AI Visibility — Pilot Preflight Checklist

Use before **client access** and again immediately before any **MANUAL_GOVERNED** monitoring RUN.

Capability: **PASS** = can verify with current tooling · **BLOCKED** = must resolve before proceed · **N/A** = not required for read-only pilot day.

| # | Check | Capability | How to verify |
|---|--------|------------|---------------|
| 1 | Entitlements correct for pilot company | PASS | `/api/me` + Brand AI Visibility portfolio; entitled brand IDs match showcase / Company Profile links |
| 2 | Portfolio correct (Marriott or IHG) | PASS | Demo switch only for founder/demo; production clients: Company Profile links only |
| 3 | Brands correct | PASS | Executive “Your Brands” = entitled set; no unexpected peers as subjects |
| 4 | Prompt cohort correct | PASS | Peer set `peers_uu_collection_lifestyle_owner_decision_v2` |
| 5 | Geography correct | PASS | Start with **CALA** filter; confirm Not Monitored geos show Not Monitored (not 0%) |
| 6 | Language correct | PASS | EN selected; ES available when completed; no silent cross-language mix |
| 7 | Provider credentials | PASS* | `npm run ai-visibility:phase3b2-live-env` or `preflightAllProviderCredentials()` — names/status only |
| 8 | Cost estimate reviewed | PASS | See [monitoring-runbook.md](./monitoring-runbook.md) bounded cohort |
| 9 | Cost cap set | PASS* | Env: `AI_VISIBILITY_MAX_BATCH_COST_USD` / provider baseline hard caps; confirm before RUN |
| 10 | Storage writable | PASS | Federated store under `data/ai-visibility/runtime/` (wave1-showcase + provider-baselines) |
| 11 | Evidence path healthy | PASS | Detail evidence drawer opens for a known Presence fact |
| 12 | No duplicate run | PASS | Check existing batch summaries for same provider × geo × language × purpose before execute |
| 13 | Discoverability baseline available | PASS | Marriott + IHG priority brands = `BASELINE_MEASURED` |
| 14 | Current client-visible snapshot captured | PASS | Note federated root dirs + latest batch IDs / `phase3c2_latest.json` path before any write |
| 15 | Scheduler disabled | PASS | `SCHEDULER_ENABLED = false` · `RECURRING_CADENCE.SCHEDULER_ENABLED = false` |
| 16 | Demo override off for real client | PASS | Production user cannot use `x-dealality-demo-brand-portfolio` |
| 17 | Claude execution (if Claude in wave) | PASS* | `CLAUDE_EXECUTION_READY` must be `true`; else run without Claude and mark PARTIAL |

\*Confirm on **target deploy** env, not only local.

## Preflight verdict

- **Read-only client pilot (existing measured baseline):** checks 1–6, 11, 13–16 must PASS.
- **New monitoring RUN:** all applicable checks PASS; Claude blocked ⇒ do not substitute; accept PARTIAL or defer Claude wave.
