# ADP — Final Production Deployment Compatibility Check

**Assessed:** 2026-11-06  
**Release candidate (claimed):** `e2ae6c7d3b8a98dfd006d8660aba7e4764efe672`  
**Recommendation:** **NO-GO**

**STOP.** No deploy, publish, Airtable mutation, or share rotation performed.

---

## 1. SHA vs origin/main vs Railway

| Reference | SHA / status |
|-----------|----------------|
| Claimed RC | `e2ae6c7d3b8a98dfd006d8660aba7e4764efe672` |
| Local HEAD (same) | `e2ae6c7d3b8a98dfd006d8660aba7e4764efe672` on `feat/gdi-spice-published-inventory-registry-v1` |
| `origin/main` tip | `8eabf043cbc350322a5aca5f985bcc8a8ff75715` (Merge PR #153 AVA v4 deploy-route smoke) |
| Merge-base | `b9a84611f1d71f9c3354d3136074b82d53e2c3b1` |
| Left-right count RC…main | **6 ahead / 54 behind** |
| Ancestry | Neither is ancestor of the other (divergent) |
| Latest local deploy record | `reports/deployments/latest-production.json` → **SUCCESS** for `e2ae6c7` (dirty+hotfix overrides) at 2026-11-06T13:04Z |
| Railway production | Service **Online**; latest deployment `c271f567-…` **SUCCESS** (2026-11-06 ~14:02 CET); volume `adp-durable-published` mounted |

**Conclusion:** RC is **not** current `main`. Redeploying this SHA alone would **not** carry the 54 commits now on `origin/main` (AVA V4 + GDI consolidations).

---

## 2. Does RC include latest approved ADP / GDI / AVA?

| Domain | On RC `e2ae6c7`? | Notes |
|--------|------------------|-------|
| GDI Spice / one-click / inventory registry (PR #136 line) | Partial / branch tip | RC tip *is* that GDI line; main has later GDI consolidations (#139–#150) |
| GDI universal presentation + full-content parity | **Missing vs main** | Controllers/assets present on `origin/main`, absent from this worktree asset assert |
| AVA V4 canonical + Admin route smoke (PR #147 / #153) | **Missing** | On `origin/main` only |
| ADP permanent Full E2E certification (`S8_MEASUREMENT_RECONCILIATION`, `permanent-full-e2e/*`) | **Not in RC commit** | Present only as **untracked** working-tree files |
| 21 corrected Group B packages + PDFs | **Local artifacts only** | Under `reports/…`; not a git commit requirement for publish, but code to enforce gates is uncommitted |

**Conclusion:** RC does **not** include all latest approved ADP+GDI+AVA production changes.

---

## 3. Missing / conflicting changes

### Missing from RC (on `origin/main`, not in RC)

Notable merges (from `e2ae6c7..origin/main`), including:

- PR **#153** — AVA v4 deploy-route smoke / Admin path
- PR **#147** — AVA V4 canonical integration; 38-hotel current-period packages
- PR **#150** — GDI consolidate verified → main
- PR **#146** / related — GDI universal full customer presentation parity
- Multiple GDI one-click / packaging / boot fixes (#137–#144)

~**521** ADP/GDI/AVA-touched paths differ between merge-base and `origin/main`.

### Present only in working tree (not in `e2ae6c7`)

Untracked / not in RC tree:

- `lib/ai-demand-positioning/certification/permanent-full-e2e/**`
- `lib/ai-demand-positioning/orchestration/adp-autonomous-hotel-full-e2e-v1.js` (untracked)
- `scripts/run-adp-final-full-e2e-completion-v1.mjs`
- `scripts/test-adp-permanent-full-e2e-certification-v1.mjs`
- `scripts/generate-adp-group-b-corrected-pdfs-v1.mjs`
- Plus large dirty tree (GDI/ADP API/UI, `.railwayignore`, etc.)

### Conflicts / risk

- Deploying dirty hotfix of `e2ae6c7` again would **overwrite** the current Online service with a tree that fails `assert:production-assets` relative to current main expectations.
- Publishing 21 Group B packages is **independent** of this code SHA but still needs a **separate** founder auth and must not be conflated with deploy.

---

## 4. Skipped GDI regression — resolved

| Item | Result |
|------|--------|
| Prior skip | `test:adp-gdi-universe-reconciliation-v1` **missing from `package.json`** |
| Equivalent | `scripts/test-adp-gdi-universe-reconciliation-v1.mjs` **exists** |
| Run | `node scripts/test-adp-gdi-universe-reconciliation-v1.mjs` → **PASS** (`ok: true`, hotels 19) |
| Related | `npm run test:adp-gdi-webhound-independence-v1` → **PASS** |
| Related | `npm run test:adp-gdi-hpc-identity` → **FAIL** (script path missing from disk; stale `package.json` entry) |

**Action for future:** add npm script alias pointing at the existing universe file; fix or remove stale `test:adp-gdi-hpc-identity`.

---

## 5. Production asset tree

```text
npm run assert:production-assets  → FAIL
```

Missing on this worktree (present on `origin/main`):

- `[GDI] public/js/group-demand-intelligence/gdi-universal-presentation-controller.js`
- `[AI_VISIBILITY_AUDIT]` Admin JS/CSS/PDF render HTML + certified baseline/archive indexes

**Gate:** Do not `railway up` from this tree until assets pass (or deploy from a complete main-integrated commit).

---

## 6. Corrected PDF registration readiness

| Check | Result |
|-------|--------|
| Local corrected PDFs | **21/21** under `reports/dealality-adp-permanent-full-e2e-certification-v1/prepared-group-b-pdfs/` |
| PDF magic / SHA | Valid `%PDF`; hashes computable |
| Register API | `registerFullAdpE2ePdfArtifact()` available |
| npm `dealality:adp-register-38-sealed-pdfs` | **Script file missing** (`scripts/register-adp-38-sealed-pdfs-into-artifact-store-v1.mjs`) |
| Production artifact store path | Durable volume / `ADP_FULL_E2E_PDF_ROOT` (prefer mode) — registration is a **write**; not executed |

**Verdict:** PDFs are **technically registrable** via the library API with `replaceWithHistory` / new stagedVersionId rules, but the **batch register script is missing** and registration must wait for founder publication auth. Do not register into production now.

---

## 7. Shared production release coordinator

| Check | Result |
|-------|--------|
| Railway latest | **SUCCESS** / Online — no BUILDING / QUEUED conflict observed |
| Concurrent deploy | **None active** at check time |
| Policy | Use `npm run deploy:production` only (never raw `railway up`); respect hotfix/dirty guards |
| Note | Last SUCCESS was already a **dirty+hotfix** deploy of `e2ae6c7`; further hotfix without main integration increases divergence risk |

---

## 8. Durable registry + share token backup

| Asset | Local path | Backup feasible? |
|-------|------------|------------------|
| Durable published registry | `data/adp-durable-published/` (registry, current, snapshots, history) | **Yes** — filesystem copy / volume snapshot |
| Production volume | Railway volume `adp-durable-published` → `/app/data/adp-durable-published` | **Yes** — snapshot volume before publish/deploy |
| Share registry | `data/ai-demand-positioning/share-registry/active-tokens.json` | **Yes** — copy; **do not rotate** |
| Full ADP PDF store | `data/ai-demand-positioning/full-adp-e2e-pdf` (local may be empty; prod may use durable root) | Snapshot with durable volume |

**Recommended backup (read-only copy; run before any authorized publish):**

```powershell
$stamp = Get-Date -Format "yyyyMMdd-HHmmss"
$dest = "C:\Dev\dealality-backups\adp-pre-activation-$stamp"
New-Item -ItemType Directory -Force -Path $dest | Out-Null
Copy-Item -Recurse data\adp-durable-published "$dest\adp-durable-published"
Copy-Item -Recurse data\ai-demand-positioning\share-registry "$dest\share-registry"
# Optional: railway volume snapshot via Railway dashboard / CLI if available
```

---

## 9. Exact commands (prepared — **do not run** without founder auth)

### A. Preflight (safe / read-only)

```bash
git fetch origin
git rev-parse HEAD origin/main
npm run assert:production-assets
npm run deploy:production:dry-run
node scripts/test-adp-gdi-universe-reconciliation-v1.mjs
npm run test:adp-gdi-webhound-independence-v1
npm run test:adp-permanent-full-e2e-certification-v1
```

### B. After a **new** integrated commit exists (not bare `e2ae6c7`)

```bash
# Only when founder authorizes PRODUCTION CODE DEPLOYMENT
# and tree is clean, on allowed branch, assets PASS:
npm run deploy:production
```

Emergency overrides (discouraged unless founder explicitly allows):

```bash
DEALALITY_ALLOW_HOTFIX_DEPLOY=1 DEALALITY_ALLOW_DIRTY_DEPLOY=1 npm run deploy:production
```

### C. Publication of 21 corrected packages (separate auth)

```bash
# Staging already proven under:
# reports/dealality-adp-final-full-e2e-completion-v1/simulated-production-activation/durable-staging
# Production promote must use governed durable commit against production volume
# with publicationAuthorized=true — exact producer script TBD after code commit;
# do NOT reuse staging copy onto prod volume blindly.
```

### D. Verification (post-authorized deploy/publish)

```bash
# Postdeploy smoke (included in deploy:production)
# Admin + share parity:
npm run dealality:adp-38-admin-publication-customer-access-parity-audit
# Grain lock external reports (reconstruct tokens; no rotate):
npm run audit:adp-external-report-grain-lock-v1 -- --base=https://my-operators-backend-production.up.railway.app
```

### E. Rollback

```bash
# App: redeploy prior SUCCESS deployment image / prior Railway SHA
# (Railway dashboard → prior deployment → Redeploy)
# Data: re-activate prior registry/history entry; never delete snapshots or share tokens
```

---

## 10. GO / NO-GO

### **NO-GO** for production deployment of `e2ae6c7` as the ADP permanent-certification release

Blocking reasons:

1. **54 commits behind `origin/main`** — missing AVA V4 + later GDI production fixes.  
2. **Permanent certification code not in the RC SHA** (untracked). Deploying `e2ae6c7` would **not** ship `S8_MEASUREMENT_RECONCILIATION`.  
3. **`assert:production-assets` FAIL** on this tree.  
4. Dirty / uncommitted worktree — not a clean production candidate.  
5. Batch PDF register script missing (registration path incomplete for production ops).

Non-blocking (already resolved or ready offline):

- GDI universe reconciliation script **PASS** when run directly.  
- 21 corrected PDFs locally valid and registrable via library.  
- No active conflicting Railway build at check time.  
- Durable + share registries backupable.  
- 38-hotel local certification handoff remains valid as **local** work.

### Required before any GO

1. Integrate permanent ADP certification modules into a **clean commit**.  
2. Merge/rebase onto current `origin/main` (or cherry-pick AVA/GDI required commits) so asset assert passes.  
3. Re-run permanent cert + GDI/AVA regression on that SHA.  
4. Obtain **separate** founder auths for:  
   - (1) production code deploy  
   - (2) publish 21 corrected packages  
   - (3) Airtable backfill  

---

## Authorization boundary

| Action | Status |
|--------|--------|
| Deploy | **Not authorized — not executed** |
| Publish 21 packages | **Not authorized — not executed** |
| Airtable writes | **Not authorized — not executed** |
| Share rotation | **Not authorized — not executed** |
