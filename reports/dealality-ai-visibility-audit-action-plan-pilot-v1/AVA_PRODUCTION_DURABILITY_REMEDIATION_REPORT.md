# AI Visibility Audit — Production Durability & Deployment Remediation

**Date:** 2026-11-05  
**Prior status:** `PREFLIGHT_FAIL`  
**This remediation:** Independently verified locally  
**Production deploy:** **NOT executed** — awaiting explicit founder authorization  

Artifacts:

- `AVA_DURABILITY_VERIFICATION.json` — **ok: true**
- `AVA_MIGRATION_PARITY.json` — **ok: true**
- `config/ava-production-deployment-manifest-v1.json`
- Lean baselines: `data/ai-visibility-audit-action-plan/certified-baselines/`
- Production store: `data/ai-visibility-audit-action-plan/production/store/`
- PDF archive: `reports/ai-visibility-audit-action-plan/archive/`

---

## 1. Root causes resolved

| Blocker from preflight | Resolution |
|---|---|
| AVA store/PDFs excluded by `.railwayignore` | Explicit `RAILWAY_LEAN_AVA_V1` allowlists for lean baselines, production store, and archive PDFs |
| Full staged ADP ~299MB not deployable | Lean pinned baselines (~76 KB total JSON) replace staged dependency for AVA |
| Code largely untracked | AVA modules/UI/API/data artifacts staged into git index (not committed/pushed) |
| Ephemeral pilot PDF paths | Six audits migrated to `reports/ai-visibility-audit-action-plan/archive/{propertyId}/current.pdf` |

---

## 2. Storage architecture selected (and why)

**Selected: Deploy-packaged filesystem SoT matching ADP monthly-review archive.**

Investigation found Dealality production already persists customer/admin report artifacts by:

1. Writing to known repo paths.
2. Shipping them through **explicit `.railwayignore` allowlists** on `npm run deploy:production` / Railway CLI upload.
3. Treating that uploaded tree as the durable SoT until the next redeploy.

Examples already in production:

- `reports/ai-demand-positioning/monthly-review/archive/**`
- `data/ai-demand-positioning/published/**`
- Selected `data/ai-demand-positioning/runtime/*.json`

**Why this over a new storage service**

- Reuses proven Dealality infrastructure and deploy guards.
- Avoids introducing S3/volume/Airtable schemas without need.
- Keeps AVA admin-staged (not customer-published) in the same trust model as ADP archives.
- Keeps Cloudflare upload size manageable (~**3.96 MB** AVA pack vs ~299 MB staged).

**What this is not**

- Application upload of the whole `reports/**` tree.
- Runtime-only local disk without redeploy packaging.
- Multi-replica shared disk (single Railway service model, same as ADP archive today).

**Survivability**

| Event | Behavior |
|---|---|
| Process restart | Persists on instance disk |
| Redeploy | Persists **if** production/store + archive + baselines are in the uploaded tree |
| Multi-instance | Not shared; Dealality web service is single-instance today |

Runtime decisions created **after** a deploy must be included in the next deploy package (or backed up) — identical operational rule as ADP monthly-review archive feedback/artifacts.

---

## 3. Lean baseline design

**Contract:** `AVA_CERTIFIED_ADP_BASELINE_V1`  
**Index:** `data/ai-visibility-audit-action-plan/certified-baselines/index.json` (**38** hotels)

Each hotel file is pinned by `{propertyId}/{periodId}.json` and includes:

- Identity (name, market, website, domains)
- Certified `periodId` + execution provenance
- AI Consideration / Scenario Presence / Certified Peer Position
- Competitive peer labels
- Observation references (demand capture / ADP action count)
- `sourceSha256` of the originating staged ADP report
- `baselineSha256` of the lean extract
- `immutablePin: true` — newer ADP periods do **not** overwrite an existing period file

Loader preference (`adp-baseline-loader-v1.js`):

1. Pinned lean baseline (`periodId` explicit or index head)
2. Local staged fallback for build-time only (`STAGED_FALLBACK_DEV`)

Audits store `adpBaselinePeriodId` + `adpBaselineSha256` so historical reports keep provenance even if a new lean period is added later.

**No CORE remeasurement. No certified ADP mutation.**

---

## 4. Migration results

Six audits migrated idempotently: `scripts/ava-migrate-production-archive-v1.mjs`

| Hotel | Status preserved | Versions | PDF | Customer ready | Founder-for-customer |
|---|---|---:|---|---|---|
| Spice Island | `FOUNDER_REVIEWED_STAGED` | 1 | archive `current.pdf` | No | No |
| Wyndham | `AUDIT_VALIDATED` | 3 | yes | No | No |
| Waterstone | `AUDIT_VALIDATED` | 3 | yes | No | No |
| Bethesda | `AUDIT_VALIDATED` | 8 | yes | No | No |
| Castillo | `AUDIT_VALIDATED` | 3 | yes | No | No |
| Sheraton | `AUDIT_VALIDATED` | 3 | yes | No | No |

Parity (`AVA_MIGRATION_PARITY.json`): status match, version counts match, founder flags unchanged, publication still blocked, PDFs present — **ok: true**.

Legacy pilot store retained under `data/ai-visibility-audit-action-plan/store/` (Railway-excluded) as recoverable backup.

---

## 5. Exact code changes

| Path | Change |
|---|---|
| `lib/.../certified-baseline-paths-v1.js` | Path constants for baselines/archive/production store |
| `lib/.../adp-baseline-loader-v1.js` | Lean-first loader + pinned period support |
| `lib/.../audit-store-v1.js` | Prefer `production/store` once migrated |
| `api/admin-ai-visibility-audit.js` | PDF generate writes to archive; prior evidence-status fix retained |
| `scripts/ava-build-lean-certified-baselines-v1.mjs` | Build lean pins from staged (local) |
| `scripts/ava-migrate-production-archive-v1.mjs` | Six-hotel migration + checksums |
| `scripts/ava-durability-verification-v1.mjs` | Redeploy simulation + pack checks |
| `.railwayignore` | `RAILWAY_LEAN_AVA_V1` allowlists |
| `config/production-required-assets.json` | `AI_VISIBILITY_AUDIT` surface |
| `config/ava-production-deployment-manifest-v1.json` | Deploy/rollback manifest |

---

## 6. Deployment package contents

See `config/ava-production-deployment-manifest-v1.json`.

**Include**

- AVA API/lib/UI/PDF render assets
- `certified-baselines/**` (~76 KB)
- `production/store/**` (~0.7 MB metadata/versions)
- `archive/**` PDFs (~2.4 MB current PDFs + named copies)
- Updated `.railwayignore` + production-required-assets

**Exclude**

- Full `data/ai-demand-positioning/staged/**`
- Pilot `reports/dealality-ai-visibility-audit-action-plan-pilot-v1/**` page-previews
- Legacy `data/ai-visibility-audit-action-plan/store/**`

**Pack size verified:** **~3.96 MB** (`AVA_DURABILITY_VERIFICATION.json`)

Git: AVA package files are **indexed for commit** (`git add`); **no commit / no push** performed.

---

## 7. Authentication verification

| Check | Result |
|---|---|
| `NODE_ENV=production` ⇒ DEV bypass inert | Pass |
| Unauthenticated AVA endpoints | Previously verified **401** |
| Live admin hotels (local) | **200 / 38** |
| PDF preview (auth) | **200 application/pdf** |
| `generate-pdf` + `publish:true` | **403 PUBLICATION_BLOCKED** |
| ADP monthly-reviews regression | **200** |

Gates preserved:

`AUDIT_VALIDATED` ≠ `FOUNDER_APPROVED_FOR_CUSTOMER` ≠ `PUBLISHED` / `CUSTOMER_READY`

Staged PDFs remain behind admin auth (no public static expose of archive).

---

## 8. Persistence and restart / redeploy simulation

| Test | Result |
|---|---|
| Lean universe 38 without staged dependency in loader preference | Pass |
| Six audits + PDFs resolve from production store/archive | Pass |
| Copy allowlisted trees into `_redeploy-sim` and re-resolve lean+PDF | Pass |
| Pack < 10 MB | Pass (3.96 MB) |
| Idempotent baseline rebuild / migration re-run | Pass (unchanged pins) |

Application process restart was covered by reloading modules against disk SoT after migration. Full Railway upload was **not** executed (deploy prohibited).

---

## 9. Existing ADP regression results

| Probe | Result |
|---|---|
| `GET /api/admin/ai-demand-positioning/monthly-reviews` | **200** |
| AVA hotels list | **200 / 38** |
| Publication still blocked | **403** |
| No ADP baseline files modified | Lean extracts are new files; staged ADP untouched |

---

## 10. Remaining risks and blockers

| Item | Severity | Notes |
|---|---|---|
| Founder has not authorized production deploy | **Blocker (process)** | Required next step |
| Git commit not created | Process | Files staged; founder/CI must commit on deploy branch |
| Post-deploy runtime writes need next upload or backup | Residual (same as ADP archive) | Documented in rollback |
| Multi-instance shared disk | Residual | Not required for current single Railway web service |
| Playwright Chromium on Railway | Residual low | Already used by ADP monthly-review PDF generation |

No unpaid/paid research, Airtable, or customer-publication blockers remain for **admin-staged** AVA enablement.

---

## 11. Final deployment recommendation

### **PASS — ready to request founder production deploy authorization**

Conditions already met locally:

1. Durable allowlisted storage model implemented.
2. Lean certified baselines for all 38 hotels.
3. Six audits migrated with checksum/version parity.
4. Auth/publication gates intact.
5. Redeploy simulation preserves reports/approvals/baselines.
6. Pack size safe for Railway CLI.

**Still required before any deploy**

1. Explicit founder **production deploy** authorization (this is not per-hotel `FOUNDER_APPROVED_FOR_CUSTOMER`).
2. Commit the staged AVA package on the normal deploy branch.
3. Run `npm run deploy:production` only after that authorization.
4. Post-deploy smoke: 38 hotels, six PDFs, publish still blocked, ADP reviews healthy.

**STOP.** No production deployment performed in this task.
