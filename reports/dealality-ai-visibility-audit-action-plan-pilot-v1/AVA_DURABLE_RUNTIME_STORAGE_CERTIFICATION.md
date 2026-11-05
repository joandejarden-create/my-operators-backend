# AVA — Durable Runtime Storage Certification

**Contract:** `AVA_DURABLE_RUNTIME_STORAGE_V1`  
**Date:** 2026-11-05  
**Production deploy / Railway config change:** **NOT performed**  
**Verdict:** Implementation + isolated volume certification **PASS**  
**Production deployment readiness:** **PASS_WITH_FOUNDER_VOLUME_AUTH** (blocked until founder authorizes Railway Volume + env)

---

## 1. Architecture implemented

Single configurable storage root (`AVA_STORAGE_ROOT`), recommended Railway mount **`/app/ava-data`**:

| Subdir | Contents |
|---|---|
| `audits/{propertyId}/` | `AUDIT_META.json`, `versions/*.json`, `events/*.json` |
| `pdfs/{propertyId}/` | `current.pdf`, `{versionId}.pdf`, `checksums.json` |
| `backups/` | Staging / recoverable snapshots |

**Modes (`AVA_STORAGE_MODE`):**

| Mode | Behavior |
|---|---|
| `off` | Legacy repo paths (`production/store` + `reports/.../archive`) — ephemeral on Railway without a volume |
| `local` | Write to `AVA_STORAGE_ROOT` (default `data/ava-data`) — fail closed if unwritable |
| `required` | Same as local; intended production posture once volume is attached |

**Fail closed:** when mode is `local` or `required`, AVA writes do **not** silently fall back to the application filesystem. Missing/unwritable root → `AVA_STORAGE_UNAVAILABLE` (HTTP 503 on admin write routes).

**Seed vs runtime:**

- **Artifact (git / Railway upload allowlists):** 38 lean certified ADP baselines + packaged six-audit seed under `data/.../production/store` and `reports/.../archive`.
- **Volume:** all new audits, PDFs, recommendation decisions, founder approval events.
- Migration **never overwrites newer volume records** with older packaged seed.

**Version history correction (forward-looking):**

- Report/PDF generation → `versions/` + `kind: REPORT_VERSION`.
- Recommendation decisions → `events/` (`RECOMMENDATION_DECISION`) + in-place payload update; **does not** create a new PDF/report version.
- Founder / validation status → `events/` (`FOUNDER_APPROVAL` / `AUDIT_VALIDATION`) without a new PDF version.
- Bethesda’s historical `*_decision` version rows are **preserved** and labeled legacy — not destructively rewritten.

---

## 2. Files changed

| Path | Role |
|---|---|
| `lib/ai-visibility-audit-action-plan/resolve-ava-storage-root-v1.js` | Root/mode resolution, fail-closed ensure |
| `lib/ai-visibility-audit-action-plan/audit-store-v1.js` | Durable audits + atomic JSON + decision/approval events |
| `lib/ai-visibility-audit-action-plan/ava-pdf-archive-v1.js` | Durable PDF staging + checksums |
| `api/admin-ai-visibility-audit.js` | Decisions/events, PDF resolve, storage-status, 503 on volume miss |
| `server.js` | `/storage-status` behind `adpMonthlyReviewAdminAuth` |
| `scripts/ava-migrate-durable-storage-v1.mjs` | Idempotent six-audit migration |
| `scripts/ava-durable-runtime-persistence-certification-v1.mjs` | Mounted-volume persistence proof |
| `scripts/test-ava-durable-storage-gates-v1.mjs` | Auth / publication / fail-closed gates |
| `.env.example`, `.gitignore`, `package.json` | Config docs + scripts + ignore runtime volume |

---

## 3. Migration results

Command:

```bash
AVA_STORAGE_MODE=local AVA_STORAGE_ROOT=./data/ava-data npm run ava:migrate-durable-storage
```

Report: `AVA_DURABLE_STORAGE_MIGRATION.json`

| Hotel | Migrated |
|---|---|
| Spice Island Beach Resort | Yes |
| Wyndham San Diego Bayside | Yes |
| Waterstone Resort & Marina | Yes |
| Bethesda Marriott | Yes (legacy decision versions preserved) |
| Castillo Hotel Son Vida | Yes |
| Sheraton Mallorca Arabella Golf Hotel | Yes |

Policy: `neverOverwriteNewerRuntime: true`. PDF paths rewritten to `pdfs/{propertyId}/current.pdf` on the volume copy.

---

## 4. Runtime persistence test results

Script: `npm run ava:certify-durable-runtime`  
Evidence: `AVA_DURABLE_RUNTIME_PERSISTENCE_CERT.json`  
**Verdict: PASS**

| Step | Result |
|---|---|
| 1. Install six seed audits on mounted volume | PASS |
| 2–4. Create seventh audit + PDF + APPROVE/DEFER | PASS |
| 5. Simulated founder approval (isolated cert hotel only) | PASS |
| 6. Process restart (new Node workers) | PASS |
| 7. Redeploy artifact **excluding** seventh | PASS (artifact had no seventh) |
| 8. Seventh audit/PDF/decisions/approval intact on volume | PASS |
| 9. Original six unchanged (meta + PDF checksums) | PASS |
| 10. PDF checksum match pre/post | PASS |
| Bad volume root fail-closed | PASS |
| Decision did **not** create new report version | PASS (`beforeVersions === afterVersions`) |

This uses a **genuinely separate mounted directory** (`_durable-runtime-cert/mounted-volume`) that is **not** replaced when the application artifact tree is swapped — equivalent to a Railway Volume for certification purposes. It is **not** a re-upload of runtime files into the deploy pack.

---

## 5. PDF and metadata checksum verification

From certification JSON (examples):

| Property | PDF SHA-256 (survived / unchanged) |
|---|---|
| Seventh (runtime) | `10282b562105f5e35d1045d47d3c3f523e5159fe50201c84176cdd8c39622fb4` (pre==post) |
| Spice Island | unchanged across redeploy simulation |
| Bethesda / peers | unchanged (`sixUnchanged: true`) |

---

## 6. Authentication and publication-gate results

Script: `npm run test:ava-durable-storage-gates` → `AVA_DURABLE_STORAGE_GATES.json` **PASS**

| Check | Result |
|---|---|
| Admin auth on AVA routes (incl. PDF + storage-status) | PASS |
| No AVA-local DEV_AUTH_BYPASS | PASS |
| `AUDIT_VALIDATED` ≠ founder/customer approval | PASS |
| Customer publication requires explicit authorizations | PASS |
| Required mode fails closed on bad root | PASS |

Staged PDFs remain under `/api/admin/ai-visibility-audit/:propertyId/pdf` with `adpMonthlyReviewAdminAuth` — not a public route.

---

## 7. Version-history corrections

| Concern | Resolution |
|---|---|
| Bethesda eight stored versions | Historical rows kept; `*_decision` marked `DECISION_INDUCED_VERSION_LEGACY` on migrate |
| New recommendation decisions | `recordRecommendationDecision` → events + in-place latest payload; **no** new PDF version |
| Founder approval | `recordApprovalEvent`; status meta update; **no** new PDF version |
| API hotel detail | Returns `versionHistory.reportVersions` vs `decisionInducedVersionsPreserved` + `events[]` |

---

## 8. Remaining risks

1. **Railway Volume not attached in production yet** — until founder authorizes mount + `AVA_STORAGE_MODE=required`, production remains ephemeral for post-deploy writes.
2. **Single-instance assumption** — Railway volumes are typically single-node; multi-replica concurrent writers are unsupported without an external object store/lock service.
3. **Backup** — volume snapshots are operator-owned; `backups/` is application-level only.
4. **Cutover window** — setting `required` before migrate causes write outage (by design, fail closed).
5. **Seed still in deploy artifact** — intentional for cold-start; migrate prefers newer volume data so redeploys do not clobber runtime.

---

## 9. Deployment readiness

| Item | Status |
|---|---|
| Code + isolated certification | **PASS** |
| Production Railway Volume attached | **NOT DONE** (authorization required) |
| Production env `AVA_STORAGE_ROOT` / `MODE=required` | **NOT DONE** |
| Overall production readiness | **PASS_WITH_FOUNDER_VOLUME_AUTH** |

---

## 10. Exact founder authorization required

Authorize **all** of the following before any production change:

1. Create **one** Railway Volume for the AVA service.  
2. Mount it at **`/app/ava-data`**.  
3. Set env:
   - `AVA_STORAGE_ROOT=/app/ava-data`
   - `AVA_STORAGE_MODE=local` (first boot / migrate), then `required` after verify.  
4. Run migrate on the live service (or one-off job) against the volume.  
5. Confirm certification commands against production volume (read-only verify).  
6. **Do not** enable customer publication or change real-hotel founder approvals as part of storage cutover.

**Explicitly not authorized by this certification:** production deploy, Railway config mutation, customer publication, paid research, ADP baseline edits, production Airtable writes, founder approval changes on real hotels.

---

## Production configuration preparation (do not apply without authorization)

### Required mount / env

```text
Volume mount:  /app/ava-data
AVA_STORAGE_ROOT=/app/ava-data
AVA_STORAGE_MODE=required   # after migrate
```

### Migration procedure

```bash
# On a shell with volume mounted and MODE=local:
AVA_STORAGE_MODE=local AVA_STORAGE_ROOT=/app/ava-data \
  node scripts/ava-migrate-durable-storage-v1.mjs
```

### Deployment order

1. Deploy code that understands `AVA_STORAGE_*` (this change) with **MODE=off** still safe.  
2. Attach volume at `/app/ava-data` (founder auth).  
3. Set `AVA_STORAGE_ROOT` + `MODE=local`.  
4. Migrate six audits.  
5. Verify `GET /api/admin/ai-visibility-audit/storage-status`.  
6. Set `MODE=required`.  
7. Smoke: open one seeded hotel, confirm PDF; generate a **non-production** test property only if needed.

### Backup procedure

- Railway volume snapshot / provider backup before MODE=required.  
- Optional: copy `audits/` + `pdfs/` to offline object storage.  
- Application `backups/` is supplementary, not a substitute for volume snapshots.

### Rollback

1. Set `AVA_STORAGE_MODE=off` (reads fall back to packaged seed; **runtime-only** volume writes become invisible to the app).  
2. Keep the volume intact — do not delete.  
3. Re-enable `local`/`required` to resume volume reads/writes.

### Verification commands

```bash
npm run test:ava-durable-storage-gates
npm run ava:certify-durable-runtime
AVA_STORAGE_MODE=local AVA_STORAGE_ROOT=/app/ava-data npm run ava:migrate-durable-storage -- --dry-run
# After auth: curl admin storage-status with member JWT
```

---

## STOP

Independent certification complete. No production Railway resources were created or modified.
