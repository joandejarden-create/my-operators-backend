# ADP Clean Main Release — Production Activation Plan

**Contract:** `ADP_CLEAN_MAIN_RELEASE_V1`  
**Branch:** `release/adp-permanent-full-e2e-cert-main-v1`  
**Base:** current `origin/main` (includes GDI + AVA)  
**Mode preserved:** `ADP_DURABLE_PUBLISHED_MODE=prefer`  
**STOP:** Do not execute the procedures below until founder authorization.

---

## 1. Exact release SHA

Recorded at commit time in `05-RELEASE_CANDIDATE.json` as `releaseCandidateSha`.

Pre-commit tip was `8eabf043cbc350322a5aca5f985bcc8a8ff75715` (`origin/main`).  
Candidate SHA = commit on this branch that contains permanent Full E2E certification + Group B packages/PDFs.

## 2. Files changed (scope)

- Permanent S8 certification modules: `lib/ai-demand-positioning/certification/permanent-full-e2e/**`
- Full E2E OS dependency: `lib/dealality-full-e2e-os/**`
- Supporting ADP lib/orchestration/certification sync required for import coherence
- Scripts: permanent reconcile, final completion, Group B PDF generate/register, permanent cert + GDI universe + admin recovery tests
- `package.json` npm aliases for the above
- `server.js` worktree allow for RC local verification only (`ADP_ALLOW_WORKTREE_SERVER` / `*-adp-cert-main-rc`)
- Reports: permanent certification packages (17A baked evidence + 21B corrected packages), 21 corrected PDFs + manifests, final completion gate JSON/MD, clean-main release docs

**Not included:** unrelated HPC modules, GDI YOTEL test patch, AVA lean-pin data fixes, share-token registry mutation, production durable mutation, Airtable writes.

## 3. Gate results (integrated candidate)

| Gate | Result |
|------|--------|
| `assert:production-assets` | PASS (56 files) |
| Permanent ADP S8 certification | PASS |
| 38/38 measurement reconciliation | PASS (17A + 21B + 0C) |
| 17 Group A unchanged | PASS |
| 21 Group B corrected-package certification | PASS |
| 38/38 browser rendering | PASS (Playwright, base `:8081`) |
| 38/38 PDF parity | PASS (21 corrected PDFs bound) |
| Admin dashboard parity / local recovery | PASS (12/12) |
| External share compatibility | PASS via Gate 3 (38/38 share API + `/owner-ai-demand-share.html`; production share contract PASS) |
| Airtable persistence (implementation + contract; no writes) | PASS (`implementationReady`) |
| Historical reconstruction | PASS (4/4 samples) |
| Simulated staging activation (21 promote + Group A stable + rollback history) | PASS (isolated staging only) |
| GDI universe reconciliation | PASS |
| GDI hangers presentation parity | PASS |
| Production route public path resolution | PASS |
| `deploy:production:dry-run` | PASS (dry only) |
| Group B PDF register dry-run (SHA+period verify) | PASS 21/21 |

**Documented non-blocking / environmental (not introduced by this ADP integration):**

- `test:gdi-customer-publication-invariants-v1` — origin/main assertion expects YOTEL facing≥1; local filesystem mirror has facing=0 (dirty source already patched locally; not copied — unrelated GDI)
- `gdi:universal-100-percent-full-content-parity-cert` — missing Spice opportunity fixtures in this worktree
- `ava:adp-permanent-cross-report-certification` — `LEAN_PIN_UNSUPPORTED_ZERO` / `ZERO_INTEGRITY_FAILURE` (AVA lean inventory), exit 2

## 4. Durable and share backup procedure

1. Snapshot production durable volume (`ADP_DURABLE_PUBLISHED_ROOT`): `current/`, `snapshots/`, `registry/`, `full-adp-e2e-pdf/`.
2. Snapshot share capability registry / active tokens (do **not** rotate).
3. Record Railway deployment SHA and volume mount path.
4. Keep backup offline until post-activation verification completes.

## 5. Code deployment procedure

1. Founder authorizes deploy of **this** candidate SHA (not `e2ae6c7`).
2. Confirm no concurrent Railway deploy.
3. Deploy candidate; confirm `ADP_DURABLE_PUBLISHED_MODE=prefer` unchanged.
4. Confirm production durable volume remains authoritative (no lean-stage substitution).
5. Smoke: admin inventory 38, one Group A + one Group B customer share URL.

## 6. 21-package publication procedure

1. Separate founder authorization required.
2. Promote prepared Group B corrected packages as **new** current in durable.
3. Preserve prior snapshots + `registry/history` (no deletes).
4. Do **not** rotate share tokens.
5. Re-verify Admin + external share parity.

## 7. PDF registration procedure

1. Dry-run already verified: `npm run dealality:adp-register-group-b-corrected-pdfs-v1` (21/21).
2. Production register requires `--authorize-production-register=1` **and** founder publication auth.
3. Prechecks: `%PDF` magic, `newPdfArtifactSha256`, on-disk SHA, manifest SHA, periodId, reject prior-only SHA.

## 8. 38-hotel production browser verification

1. After deploy (+ optional publish), replay Gate 3 against production base URL.
2. Require 38/38 API + Playwright: not blocked, hotel present, Provider Presence grain, share token intact.

## 9. Rollback procedure

1. **App:** redeploy prior Railway SHA.
2. **Data:** re-activate prior `registry/history` entry; never delete snapshots or history.
3. **Shares:** leave tokens unchanged.

## 10. Airtable backfill plan

1. Separate founder authorization.
2. Use read-only reconciliation under `reports/dealality-adp-airtable-persistence-historical-traceability-v1/`.
3. Idempotent insert-only historical writers only; no destructive history rewrite.

---

**Explicit non-actions in this release prep:** no paid research, no new measurements, no Airtable writes, no production deploy/publish, no token rotation, no historical deletion.
