# Hotel Explorer — Canonical HPC Population Workflow (v1)

**Status:** Binding design for HE research → HPC/HI → Hotel Explorer  
**Pilot:** Eight Mullen Real Estate Capital portfolio *candidates* (not verified holdings)  
**Canonical base:** `appCCUsuGsE1ifoLk`  
**HPC:** `tbl9aY5ijiuIzzWam` · **Hotel Census (legacy read):** `tblgj2qEwxjTcg6q0`  
**Apply policy:** Default dry-run (`applyAuthorized: false`). Production Airtable apply requires explicit founder authorization per batch.

## Phase 0 — Reuse map (do not recreate)

| Capability | Canonical module / surface | Role |
| --- | --- | --- |
| HPC field map | `lib/hotel-intelligence/map_hotel_intelligence_fields.js` | Property field names |
| HPC SoT + write policy | `lib/research-engine-v2/production-census-source-of-truth.js` | Autopilot / write targets |
| HPC full onboarding stages | `lib/hotel-census/hpc-full-onboarding-contract-v1.js` | Min identity + ADP essentials |
| HPC shared upsert / null-fill | `lib/hotel-census/hpc-shared-upsert-v1.js` | Idempotent dry-run → apply |
| Live identity collision | `lib/hotel-census/hpc-live-identity-collision-v1.js` | MATCH vs NEW vs AMBIGUOUS |
| Authorized create/reuse | `lib/hotel-census/hpc-authorized-create-reuse-v1.js` | Founder-gated CREATE |
| Property table ownership | `lib/hotel-intelligence/property-census-identity-v1.js` | HPC vs Hotel Census by RECORD_ID |
| HI Organizations / Relationships / Evidence / Runs | `lib/hotel-intelligence/research-engine-v1/airtable-graph-contract-v1.js` | Graph SoT (not HPC free-text) |
| Graph upsert planner | `lib/hotel-intelligence/research-engine-v1/airtable-graph-upsert-planner.js` | Idempotent edge plans |
| Owner portfolio UI model | `lib/hotel-intelligence/ownership/owner-portfolio-operator-view-v1.js` | Owned/Controlled + Who Manages |
| Staged Mullen portfolio | `reports/hotel-explorer/phase4-quality/HE-BETH-MULLEN-OWNER-PORTFOLIO-STAGED-V1.json` | 8 candidates + Bethesda match |
| Founder Preview overlay | `api/hotel-explorer-founder-preview.js` | Proposed vs live |
| HE population shell | `public/js/hotel-explorer.js` + rooms/HDP/HI workspace | Shared tabs — no per-hotel UI |

**Forbidden:** Parallel hotel DBs, Autopilot writes to legacy Hotel Census, copying ownership graphs into HPC free-text, paid research without incremental spend approval.

## Workflow

```
DISCOVER → MATCH → RESEARCH → VERIFY → DRY-RUN → APPROVED APPLY → RE-READ → VALIDATE → HE PREVIEW
```

1. **DISCOVER / MATCH** — `scripts/he-mullen-pilot-hpc-identity-resolution-v1.mjs` (read-only). Outcomes: `MATCHED_HPC` | `MATCHED_HOTEL_CENSUS` | `MATCHED_EXISTING_ALIAS` | `NEW_HPC_CANDIDATE` | `AMBIGUOUS_MATCH` | `IDENTITY_UNRESOLVED`.
2. **RESEARCH / VERIFY** — Minimum verified fields from `HPC_MINIMUM_IDENTITY_FIELDS` + ADP essentials; field-level provenance; never overwrite stronger verified facts with weaker sources.
3. **DRY-RUN** — Per-mutation packet: table, record id or CREATE, field, before/after, evidence, verification state, conflict disposition.
4. **APPROVED APPLY** — Founder-explicit only. Then re-read + certify.
5. **HE** — Deep-link Founder Preview for canonical HPC ids only; shared components populate all tabs.

**STOP** before the initial production Airtable apply until the dry-run CREATE/UPDATE packet is founder-approved.

## Pilot cohort module

`lib/hotel-intelligence/hpc-population/mullen-pilot-cohort-v1.js`

## Airtable persistence classification

Until founder-approved apply + re-read: at most `PERSISTENCE_DESIGNED` / staged dry-run. Not `FULL_E2E_VALIDATED`.

## HE 5.0 Full E2E supervisor (extends this workflow)

`lib/hotel-intelligence/he-5-0-full-e2e/` — eight-hotel free-first research + shared $50 budget ledger + 12-tab field gap matrix + dry-run package.

```bash
npm run he:5-0-full-e2e-pilot
npm run he:5-0-autonomous-reconciliation
npm run test:he-5-0-full-e2e-pilot-v1
npm run test:he-5-0-autonomous-reconciliation-v1
```

Reports: `reports/hotel-explorer/he-5-0-pilot/`. Autonomous reconciliation (HE-QL-040) investigates routine conflicts without founder prompts. **STOP** only before production Airtable apply (`applyAuthorized: false`).
