# AI Visibility Audit — Production Deployment Preflight

**Date:** 2026-11-05  
**Package:** `AVA_DEPLOYMENT_PACKAGE.json`  
**Smoke:** `AVA_PREFLIGHT_SMOKE.json` · `PREFLIGHT_HOTEL_INVENTORY.json`  
**Deploy authorized:** **NO** — stop for founder authorization  

---

## 1. Preflight verdict

### Overall: **FAIL — not ready for production deploy**

| Gate | Result |
|---|---|
| Local functional capability (admin list, 6 audits, gates, PDF preview) | **PASS** |
| Bethesda / Waterstone reconciliations | **PASS** (documented + corrected) |
| Production-equivalent auth (bypass disabled) | **PASS** |
| AI Demand Admin regression probes | **PASS** |
| Storage durability / Railway upload survivability | **FAIL — blocker** |
| Code + artifact deployability (git track + `.railwayignore`) | **FAIL — blocker** |

**Do not run `npm run deploy:production` until the blockers in §9 are remediated and founder authorizes.**

---

## 2. Exact files and artifact identifiers

### Code (must ship)

See `AVA_DEPLOYMENT_PACKAGE.json` → `codeFiles`.

Key surfaces:

- API: `api/admin-ai-visibility-audit.js`
- Routes: `server.js` (`/api/admin/ai-visibility-audit/*` behind `adpMonthlyReviewAdminAuth`)
- UI: `public/js/admin-ai-visibility-audit.js`, `public/app/admin/ai-demand-admin.html`
- Engine: `lib/ai-visibility-audit-action-plan/**` including 6 hotel payloads
- PDF render: `public/adp-ai-visibility-audit-pdf-render.html`, AVA CSS/JS + shared Dealality report chrome

### Local staged data (present now; **not** Railway-allowlisted today)

- Store: `data/ai-visibility-audit-action-plan/store/` (6 hotels)
- PDFs: `reports/dealality-ai-visibility-audit-action-plan-pilot-v1/*/pdf-staged/*.pdf`
- Inventory snapshot: `PREFLIGHT_HOTEL_INVENTORY.json` (**38** eligible ADP hotels)

### Git status (preflight fact)

- `api/admin-ai-visibility-audit.js` — **untracked**
- `lib/ai-visibility-audit-action-plan/` — **untracked**
- `public/js/admin-ai-visibility-audit.js` — **untracked**
- `public/app/admin/ai-demand-admin.html` — modified

These must be committed on the deploy branch before a guarded production upload.

---

## 3. Authentication test results

### Production-equivalent (bypass disabled)

Ephemeral server: `PORT=8099`, `NODE_ENV=production`, `DEV_AUTH_BYPASS_EMAIL` unset.

| Request | HTTP | Error |
|---|---:|---|
| `GET /hotels` no auth | **401** | `authentication_required` |
| `GET /meta` no auth | **401** | `authentication_required` |
| `POST .../decision` no auth | **401** | `authentication_required` |
| `POST .../status` founder approve no auth | **401** | `authentication_required` |
| `POST .../generate-pdf` no auth | **401** | `authentication_required` |
| `POST .../generate-pdf` `{publish:true}` no auth | **401** | `authentication_required` |
| Invalid JWT bearer | **401** | `invalid_token` |

### Bypass cannot operate in production

Middleware condition (verified):

`localDev = Boolean(DEV_AUTH_BYPASS_EMAIL) && NODE_ENV !== "production" && isLocalDevRequest(req)`

| NODE_ENV | DEV_AUTH_BYPASS_EMAIL set | Bypass active? |
|---|---|---|
| production | yes | **No** |
| production | no | **No** |
| development | yes + local | Yes (local only) |
| development | empty | No |

No alternative auth mechanism was introduced.

### Authorized admin workflow (local current server)

With normal admin session path: hotels **200 / 38**, decisions persist, PDF preview works, publish remains blocked.

---

## 4. Existing AI Demand Admin regression results

Against local current server (`AVA_PREFLIGHT_SMOKE.json`):

| Surface | Probe | Result |
|---|---|---|
| AI Demand Reviews | `GET /api/admin/ai-demand-positioning/monthly-reviews` | **200** |
| ADP Action Plan | `GET /api/admin/ai-demand-positioning/action-plan-catalog` | **200** |
| Admin shell HTML | `GET /app/admin/ai-demand-admin.html` | **200** |
| AI Visibility Audit | hotels / recommendations / PDF / decisions | **PASS** |
| Express `req.body` POST fix | decide + status + publish-block complete without hang | **PASS** |

GDI Reports / Report Archive were not force-loaded end-to-end in browser this preflight; route registration and shared auth middleware are unchanged. No AVA change touches those handlers.

---

## 5. Six-hotel audit inventory

| Hotel | ADP period | Store status | Actions | Evidence URLs | PDF on disk | Customer ready | Founder-for-customer | Versions |
|---|---|---|---:|---:|---|---|---|---:|
| Spice Island Beach Resort | `…20261020082916_2209c8` | `FOUNDER_REVIEWED_STAGED` | 5 | 10 | Yes | No | No | 1 |
| Wyndham San Diego Bayside | `…20261013221340_775a48` | `AUDIT_VALIDATED` | 4 | 4 | Yes | No | No | 3 |
| Waterstone Resort & Marina | `…20260902231159_77334c` | `AUDIT_VALIDATED` | **3** | 7 | Yes | No | No | 3 |
| Bethesda Marriott | `…20261002142154_62428a` | `AUDIT_VALIDATED` | 4 | 4 | Yes | No | No | 6+ |
| Castillo Hotel Son Vida | `…20261020101643_17a397` | `AUDIT_VALIDATED` | 4 | 5 | Yes | No | No | 3 |
| Sheraton Mallorca Arabella Golf | `…20261020170125_534b15` | `AUDIT_VALIDATED` | 4 | 5 | Yes | No | No | 3 |

- Certified ADP baselines: **read-only** via `loadAdpBaselineForAudit` (no remeasurement).
- **Zero** hotels are `CUSTOMER_READY` / publication `AUTHORIZED`.
- Hotels without AVA store rows remain visible as `BASELINE_READY` (38 − 6 = 32).

---

## 6. Bethesda status reconciliation

**Observation:** `auditStatus = AUDIT_VALIDATED` while `evidenceReviewStatus = IN_REVIEW`.

**Cause:** `POST .../actions/:actionId/decision` always wrote `evidenceReviewStatus: "IN_REVIEW"`, which incorrectly **downgraded** an already-validated audit after Phase 8 decision tests. This was **inconsistent mapping**, not a second lifecycle.

**Correction made (dev only; no founder-approval change):**

1. Decision handler now **preserves** `AUDIT_VALIDATED` / founder evidence gates instead of downgrading.
2. Bethesda meta restored to `evidenceReviewStatus: AUDIT_VALIDATED` while keeping `status: AUDIT_VALIDATED`, `NOT_CUSTOMER_READY`, `NOT_AUTHORIZED`.

**Re-verified:** subsequent DEFER decision no longer downgrades evidence status (`AVA_PREFLIGHT_SMOKE.json`).

---

## 7. Waterstone recommendation reconciliation

| Source | Count | Meaning |
|---|---:|---|
| Early store version `ava_v1_20261105_waterstone_boca_raton` | **5** | Pre–Phase 7 draft actions (marina/dining/Eau/tech/weddings) |
| Phase 7 / Phase 8 payloads + latest store version | **3** | Certified hotel-specific actions |
| Phase 8 PDF / action overview / bridge register | **3** | Matches latest |

**Exact reason for 5 → 3:** intentional Phase 7/8 quality consolidation to three evidence-backed, commercially actionable recommendations (meeting-space SoT, canonical domain, aligned waterfront/dining/meetings facts). The earlier five-action draft remains in **version history** and was **not** silently deleted from the store.

AI Admin correctly shows **3** because it reads the **latest** version (`ava_v1_phase8_waterstone_boca_raton`).

---

## 8. Storage durability assessment

| Question | Finding |
|---|---|
| Survives application redeploy? | **No (as configured).** Railway CLI upload replaces the service filesystem; `.railwayignore` currently drops `reports/*` (except ADP allowlists) and does **not** allowlist `data/ai-visibility-audit-action-plan/**` or AVA PDFs. |
| Survives container restart without redeploy? | Local file writes persist on the instance disk until redeploy/replace. |
| Multi-instance shared store? | **No.** File store is local-disk only; multiple replicas would diverge. |
| PDFs after deploy? | **Would be missing** unless allowlisted and uploaded (or regenerated after deploy). |
| Version history / approvals? | **Would be missing** unless store is allowlisted/uploaded or moved to durable storage. |
| PDF dependencies? | Playwright Chromium — same pattern as existing ADP monthly-review PDF generation already used in production. |

**Minimum remediation before deploy (required):**

1. Add lean `.railwayignore` allowlists for:
   - `data/ai-visibility-audit-action-plan/store/**`
   - latest AVA PDFs under `reports/dealality-ai-visibility-audit-action-plan-pilot-v1/*/pdf-staged/*.pdf` (exclude page-preview PNGs; full AVA report tree ≈ 56MB)
2. Provide a **lean ADP baseline source** for the 38-hotel universe (do **not** upload full `staged/` ≈ **299MB**).
3. Commit untracked AVA code.
4. Prefer single-instance until a shared durable store exists; document that runtime decisions after deploy must be re-uploaded or moved off ephemeral disk.

Optional stronger remediation: Railway volume or object storage for AVA store/PDFs (not implemented in this preflight).

---

## 9. Production deployment requirements

Before founder can authorize deploy:

1. **Commit** AVA API/lib/UI payloads on the normal deploy branch.  
2. **Remediate `.railwayignore`** allowlists for AVA store + PDFs + lean baselines.  
3. **Confirm upload size** stays under Cloudflare ~200MB railway-up limit.  
4. **Post-deploy smoke** (authorized admin): hotels list ≥ expected, six audits open, PDF preview, publish still blocked.  
5. **Explicit founder deploy authorization** recorded (same pattern as other Dealality production deploys).  
6. Keep `DEV_AUTH_BYPASS_EMAIL` ineffective (`NODE_ENV=production`).  
7. No customer publication, emails, paid research, ADP baseline edits, Airtable AVA writes, or monthly register imports.

---

## 10. Rollback plan

1. Redeploy the previous known-good production commit/tag with `npm run deploy:production`.  
2. ADP published customer surfaces remain on existing ADP allowlisted paths and are preserved if the rollback tree includes them (standard Dealality deploy guard).  
3. AVA-only rollback risk: any approvals/PDFs created only on ephemeral production disk after a partial AVA enablement would be lost — another reason not to deploy until allowlists/volume are ready.  
4. Do **not** attempt Airtable or customer-share rollbacks for AVA (AVA is file-staged; not customer-published).

---

## 11. Remaining blockers

1. **Durability / `.railwayignore`:** AVA store + PDFs excluded from Railway upload.  
2. **Baseline universe:** full staged ADP (~299MB) not deployable; need lean baseline strategy for 38 hotels.  
3. **Untracked code:** AVA modules not yet on the deployable git tree.  
4. **No founder deploy authorization** (by design for this preflight).

Non-blockers already satisfied locally: auth gates, publication gates, Bethesda evidence mapping fix, Waterstone 3-action certification, six-hotel PDF presence, admin regression probes.

---

## Founder decision requested

**STOP.** Preflight is complete.

Please authorize **only after** blockers 1–3 are remediated, then explicitly approve production deploy (separate from `FOUNDER_APPROVED_FOR_CUSTOMER` per hotel).

Customer publication of any AVA PDF remains a **second**, later gate and is **not** requested here.
