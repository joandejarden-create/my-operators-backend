# GDI Parallel provider integration — implementation report

**Date:** 2026-10-14  
**Property (controlled rollout):** Wyndham San Diego Bayside `recnPB3Q02NO8Ma1o`  
**Status:** Implemented + verified locally. **Not deployed / not activated / not customer-published.**

---

## What changed (reused vs new)

### Reused (baseline)

- `scripts/gdi-research-to-page-workflow-v1.mjs` — stage coordinator
- `lib/group-demand-intelligence/workflow/parallel-discovery-adapter-v1.js` — discover_gaps entry
- `targeted-contact-plan-v1.js`, `independent-hotel-fit-v1.js`, `reuse-prior-intelligence-v1.js`
- `prospect-publication-eligibility-v1.js`, customer visibility / readiness gates
- `lib/surfe/client.js` + market-alerts Surfe adapter (fallback gate only; not invoked)
- Existing opportunity persistence / Airtable store (read-only in this step)
- Hotel config `config/group-demand-intelligence/hotels/recnPB3Q02NO8Ma1o.json` + ADP fixture path

### New modules (`lib/group-demand-intelligence/parallel/`)

| Module | Role |
|--------|------|
| `feature-flag.js` | `GDI_PARALLEL_ENABLED` default **OFF**; San Diego allowlist; kill switch; Surfe/Jev advisory flags |
| `fixture-pack-v1.js` | In-repo SD fixtures (no lab dependency in prod) |
| `claim-evidence-v1.js` | Claim-level fact vs inference; URL≠excerpt |
| `identity-dedupe-v1.js` | Account/trip/occurrence; ASCB/EMBO alias; shared housing pathways |
| `contact-channel-policy-v1.js` | Email **or** phone enough; routing vs usable; Surfe eligibility; per-channel verification |
| `budget-ledger-v1.js` | Atomic reserve/commit/release; estimated ≠ billed |
| `job-store-v1.js` | Durable job + provider run ID; no resubmit; uncertain handling |
| `san-diego-reconcile-v1.js` | Dry-run baseline reconcile + recovery queue |

### Fixtures

`fixtures/gdi-parallel-san-diego/` — 15 frozen cards + review package (copied from lab; originals preserved in lab).

### Scripts

- `scripts/test-gdi-parallel-provider-integration-v1.mjs`
- `scripts/gdi-parallel-san-diego-preview.mjs`
- Updated `gdi-workflow-v2-verify-local.mjs` for fixtureSource

---

## Tests run

| Suite | Result |
|-------|--------|
| `node scripts/test-gdi-parallel-provider-integration-v1.mjs` | **11/11 PASS** (flag OFF, scoping, dedupe, channels, Surfe eligibility, budget idempotency, job resume/uncertain, kill switch, fixture replay, SD reconcile, publication boundary) |
| `GDI_V2_SKIP_AIRTABLE=1 node scripts/gdi-workflow-v2-verify-local.mjs` | PASS (after fixtureSource assert) |
| `node scripts/gdi-parallel-san-diego-preview.mjs` | OK — preview artifacts written |

No paid Parallel or Surfe calls. No production opportunity writes.

---

## San Diego reconciliation (dry-run)

| Metric | Count |
|--------|------:|
| Fixture candidates | 15 |
| After account+trip dedupe | 15 |
| Existing baseline matches | 0 (baseline empty / no SD store rows) |
| Proposed new (internal research) | 15 |
| **Customer-visible proposed** | **0** |
| Confirmed open demand | **0** |
| Usable email or phone (policy) | 7 |
| Neither / quarantined (`not_found` / unsupported variants) | 8 |
| Shared housing pathways marked | ASCB/EMBO, IESNA/Maritz, WEST/SPARGO cohorts |

**Contact policy note:** Lab card summary used routing_fallback buckets; integrated policy **quarantines `not_found` / generated variants** and does not treat them as usable — so routing vs quarantine counts differ by design.

### Local recovery (queued, not executed)

| Org | Plan | Surfe |
|-----|------|-------|
| Sol-Ark | Local channel present — verify email variants only | Not queued |
| SPARGO | Local channel present — verify only | Not queued |
| Esri | Local channel present — verify only | Not queued |

Surfe queue remains empty until named person + org + **no** usable channel + `GDI_PARALLEL_SURFE_FALLBACK_ENABLED` + `--authorize-surfe`.

Artifacts: `reports/gdi-parallel-integration/PUBLICATION-PREVIEW.md`, `san-diego-reconciliation.json`, `publication-preview.json`.

---

## Configuration / deployment requirements

| Variable | Default | Purpose |
|----------|---------|---------|
| `GDI_PARALLEL_ENABLED` | OFF | Master Parallel capability |
| `GDI_PARALLEL_HOTEL_IDS` | `recnPB3Q02NO8Ma1o` | Property allowlist |
| `GDI_PARALLEL_KILL_SWITCH` | OFF | Block new submissions; keep results |
| `GDI_PARALLEL_SURFE_FALLBACK_ENABLED` | OFF | Surfe fallback gate |
| `GDI_JEV_ADVISORY_ENABLED` | OFF | Optional advisory only |
| `GDI_PARALLEL_BUDGET_USD` | 10 | Estimated estimate cap (not lab remainder) |
| `SURFE_API_KEY` | — | Existing server secret (report missing name only) |
| Parallel API key | — | Required only for live (not this step) |

Missing for live later: Parallel API credential env name as configured in deployment (do not invent; wire when activating).

Durable stores under `GDI_DATA_ROOT` (or defaults):

- `parallel-budget/ledger.json`
- `parallel-jobs/*.json`

Multi-instance production should prefer a shared lock store beyond mkdir locks (documented in budget module).

---

## Activation / rollback

**Activate (after explicit approval):**

1. Deploy code with flags still OFF.
2. Set `GDI_PARALLEL_ENABLED=1` and confirm hotel allowlist = San Diego only.
3. Run fixture replay / dry-run reconcile on staging.
4. Only then authorize bounded live discovery with spend caps.
5. Customer publication still requires `--authorize-publish` / reviewed-publication path — **not** automatic from Parallel output.

**Rollback:**

1. `GDI_PARALLEL_KILL_SWITCH=1` or `GDI_PARALLEL_ENABLED=0`.
2. No customer cards were written by this integration; Bethesda/Mayflower untouched.
3. Job store retains known `providerRunId`s — do not resubmit.

---

## Material unresolved blockers

1. **Baseline novelty** for San Diego was UNKNOWN in lab; local baseline is empty/readable → proposed-new but **auto-publication blocked** until explicit publish approval.
2. **Railway / production deploy** not part of this step.
3. **Multi-worker lock** for budget ledger is mkdir-based (single-writer safe); shared DB/Airtable lock recommended before multi-instance paid runs.
4. Re-read **current Parallel list prices** before any paid execution (estimates frozen 2026-10-14 for planning only).

---

## Jev

Remains advisory / OFF by default (`GDI_JEV_ADVISORY_ENABLED`). Not an eligibility or publication gate.
