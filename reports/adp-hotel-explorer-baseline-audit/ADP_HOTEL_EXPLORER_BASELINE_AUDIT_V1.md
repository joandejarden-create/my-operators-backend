# ADP / Hotel Explorer Baseline Audit V1

**Status:** STOP FOR FOUNDER REVIEW — no functional, deploy, Airtable, or customer-visibility changes made.  
**Audit date:** 2026-11-18 (Europe/Paris)  
**Worktree:** `/agent/repos/my-operators-backend-adp-audit`  
**Branch:** `cursor/adp-hotel-explorer-audit-1ed2`  
**Base commit:** `8533d7a4b7fb2a25e868ac444a6c15b41957619d` (`origin/main` tip: Merge PR #362 GDI Airtable purpose integrity)  
**Method:** Repository inspection + static analysis + free local gates. **No paid APIs, no live AI research, no full-hotel reruns, no Railway deploy, no visibility changes.**  
**Primary GDI checkout:** untouched at `/agent/repos/my-operators-backend` on `main` @ `c4958977`.

---

## Executive verdict

1. **ADP in this repo means AI Demand Positioning** (property AI-mention / demand-capture product), not “Asset Diligence.” Hotel diligence UI is **Hotel Explorer**, fed by **Hotel Intelligence (HI)**.
2. **Hotel Explorer’s most complete working implementation is already on `origin/main`.** Unmerged HE deploy branches are historical; do not merge PR #37.
3. **Hotel Explorer is not currently accessible as the primary surface inside Radar/Scout production hosts.** Radar often loads only the compact `HotelDetailPanel`; full `hotel-explorer.js` is wired on golden-demo / share / owner-explorer hosts.
4. **Verified hotel attributes exist** (HI schema + ADP Reality Gap + Hotel ADP Attributes) with strong client-safe redaction — but dual taxonomies and incomplete factory assembly limit generic hotel surfacing.
5. **Bethesda Marriott is first-class in ADP/GDI**, absent from Hotel Explorer public share packs. Inventory/publication gates must stay fail-closed; this audit did not change them.
6. **ADP has a serious fail-closed OS**, but delivery is not yet one-command repeatable. Most published periods remain legacy/grandfathered; several Full E2E permanence scripts are unwired from npm/CI.
7. **Deploy durability remains a top operational risk** (Railway whole-tree replace historically wiped HE or ADP).

**Recommended first implementation phase (no customer visibility change):**  
**Phase 1A — Radar ↔ Hotel Explorer host wiring (Wave 0 census shell)** + **Phase 1B — wire Full E2E permanence gates into npm/PR matrix** (zero paid cost). Details in §9–10.

---

## 1. Current-state architecture and functionality

### 1.1 Product map (do not conflate)

| Product noun | What it is | Primary paths |
|---|---|---|
| **ADP** | AI Demand Positioning — certified AI demand monitoring, owner report, share, PDF, BPP, monthly review, leak audit | `lib/ai-demand-positioning/`, `public/owner-ai-demand*.html`, `api/ai-demand-positioning.js` |
| **Hotel Explorer (HE)** | Full-screen property diligence overlay (identity, ownership, market, research archive, Radar tabs embedded) | `public/js/hotel-explorer.js`, `public/hotel-explorer-share.html`, `public/hotel-intelligence-golden-demo.html` |
| **Hotel Intelligence (HI)** | Research/dossier/factory/MCP stack that feeds HE | `lib/hotel-intelligence/`, `api/hotel-intelligence-*.js`, `mcp/hotel-intelligence/` |
| **Radar / Scout** | Market map + hotel pins + compact detail panel | `public/dealality-scout.html`, `public/radar-share.html`, `public/deal-capture-radar-*.html`, `api/dealality-scout`, `api/radar-*` |
| **GDI** | Group Demand Intelligence — adjacent; shares hotels (e.g. Bethesda) but separate OS | `lib/group-demand-intelligence/` |

### 1.2 Architecture layers

```text
Customer / share UI
  ADP owner app + signed share
  HE overlay + HE share packs
  Radar/Scout maps → (intended) HE or compact panel
        │
API (server.js)
  /api/ai-demand-positioning/*
  /api/hotel-intelligence/{dossiers,research}/*
  /api/golden-demo/ownership, owner-intelligence, contact-intelligence
  /api/dealality-scout, /api/scout/*, /api/radar-*
        │
Domain libs
  lib/ai-demand-positioning/   monitor → certify → publish
  lib/hotel-intelligence/      identity, research, dossier, factory/assembler
  lib/radar-buildout/, scout
        │
Persistence
  data/ai-demand-positioning/, data/hotel-intelligence/
  fixtures/* ; durable ADP_DURABLE_PUBLISHED_ROOT
  Airtable: ADP base + HPC census SoT (cross-base Census Record ID)
```

### 1.3 Key UI entrypoints

| Surface | Path | Notes |
|---|---|---|
| ADP owner | `public/owner-ai-demand.html` | Memberstack |
| ADP share | `public/owner-ai-demand-share.html` | Signed share capability |
| ADP admin | `public/app/admin/ai-demand-admin.html` | Publication control |
| HE golden demo | `public/hotel-intelligence-golden-demo.html` | Loads HE + detail panel + HI stack |
| HE share | `public/hotel-explorer-share.html` | Packs: mexico, sheraton-gdl, voco-cancun, kgpv, cambridge — **no Bethesda** |
| Compact Radar panel | `public/js/hotel-detail-panel.js` | Used without HE on some Radar HTML |
| Full HE | `public/js/hotel-explorer.js` | Sets `__DEALALITY_HOTEL_EXPLORER_PRIMARY`; embeds Radar tabs via `HotelDetailPanel.embed` |

### 1.4 API surface (no `/api/hotel-explorer/*`)

HE is a **client overlay** over census hotel objects + HI/ownership/contact APIs. ADP has a full REST surface under `/api/ai-demand-positioning/*` plus admin, leak-audit, AVA, and local review publication control.

### 1.5 Airtable / schema authority

| Concern | Schema / map |
|---|---|
| ADP published reports | `docs/ai-demand-positioning-airtable-fields.md`, `lib/ai-demand-positioning/airtable-field-map.js` |
| HI tables (incl. Hotel ADP Attributes) | `lib/hotel-intelligence/schema/hotel-intelligence-schema-v1.js` |
| Census SoT | Hotel Property Census via `map_hotel_intelligence_fields.js` |
| Writes | `ENABLE_HOTEL_INTELLIGENCE_AIRTABLE_WRITES` gated; ADP publish apply gated |

### 1.6 Local verification run in this audit (free)

| Gate | Result |
|---|---|
| `npm run assert:hotel-explorer-share-assets` | **PASS** (14 HI production assets) |
| `npm run test:hotel-explorer-share-research-parity` | **PASS** (Golden Four archives) |
| `npm run test:hotel-adp-attributes` | **PASS** |
| `npm run test:adp-certification-inventory-reconciliation-v1` | **PASS** |
| `npm run test:adp-global-certification-regression-framework-v1` | **PASS** |
| `npm run test:ai-demand-positioning-published-foundation` | **FAIL** — payload exceeds Airtable field limit |
| `npm run test:adp-customer-surface-contract` | **FAIL** — missing certification baseline JSON + BPP `brand` null TypeError (hit on Bethesda path) |
| `npm run test:hotel-explorer-share-drawer-visible` | **BLOCKED** — Playwright not installed in this environment (optional UI) |

Logs: `/opt/cursor/artifacts/adp-audit-*.log`

---

## 2. Historical code and branch recovery inventory

### 2.1 Verdict

**Recover Hotel Explorer from `origin/main`, not from unmerged deploy branches.**  
HE core blobs on `hotfix/opportunity-radar-share` are byte-identical to main; that branch’s large delta is GDI/ADP baggage — do not merge wholesale.

### 2.2 Branch recommendations (selected)

| Branch | Merged to main? | Recommendation |
|---|---|---|
| `deploy/mexico-hotel-explorer-share` (PR #37 OPEN) | NO | **Archive / close** — superseded by PR #40; older share shell |
| `hotfix/restore-mexico-hi-share-onto-main` | YES | Archive (historical) |
| `deploy/he-hpc-isolation-p85b-20260914` / `deploy/radar-hpc-cutover-p87-20260914` | YES | Archive |
| `hotfix/opportunity-radar-share` | NO | Keep as archaeology ref only; selective cherry-pick if needed |
| `deploy/adp-production-parity-current-published-20260909` (PR #41) | NO | Cherry-pick audit for ADP BPP / current_published parity |
| `deploy/adp-v91-bpp-rank-only-parity-20260910` (PR #44) | NO | Cherry-pick audit (V92 later merged) |
| `fix/adp-renaissance-delivery-closeout` | NO | Cherry-pick audit (Renaissance share + Airtable compaction) |
| `fix/railwayignore-lean-adp-upload` | NO | Cherry-pick if `.railwayignore` still useful |
| Most `deploy/adp-*` / `fix/adp-*` / Toronto exec | YES | Archive |

### 2.3 Ranked HE completeness

1. **`origin/main`** — full HE UI + share + HI APIs + assembler + tests (**SoT**)
2. `hotfix/opportunity-radar-share` — HE identical; unique Opportunity Radar / Hilton share archaeology
3. Merged restore/isolation branches — historical only
4. `deploy/mexico-hotel-explorer-share` — foundation, **obsolete**

### 2.4 Docs / durability packets worth keeping

- `docs/production-deploy/PRODUCTION_BRANCH_DURABILITY.md`
- `docs/production-deploy/PRODUCTION_REQUIRED_ASSETS.md`
- `reports/ownership-contact-baseline/011-HOTEL-EXPLORER-READINESS.md`
- `reports/hotel-intelligence/packet-2.8/CALA_HOTEL_EXPLORER_SCALE_ROADMAP.md`
- `reports/adp-six-hotel-e2e/CANONICAL_WORKFLOW.md`
- `reports/adp/certification-inventory-reconciliation/`

---

## 3. Hotel Explorer / Radar integration assessment

### 3.1 How integration is *designed* to work

In `public/js/hotel-detail-panel.js`, `HotelDetailPanel.open(hotel)` delegates to `HotelExplorer.open(hotel)` **only when**:

- `window.__DEALALITY_HOTEL_EXPLORER_PRIMARY` is set, and  
- `window.HotelExplorer.open` exists  

`hotel-explorer.js` sets that flag and embeds Radar tab content via `HotelDetailPanel.embed`.

### 3.2 How hosts are actually wired today

| Host | Loads `hotel-detail-panel.js`? | Loads `hotel-explorer.js`? | HE primary? |
|---|---|---|---|
| `hotel-intelligence-golden-demo.html` | Yes | Yes | **Yes** |
| `hotel-explorer-share.html` → golden-demo route | Via pack | Via demo stack | **Yes** (share mode) |
| `owner-explorer.html` | CSS present | HE CSS | Partial / routed |
| `deal-capture-radar-with-ranked-list.html` | Yes | **No** | **No** → compact panel only |
| `dealality-scout.html` | **No** | **No** | **No HE / no detail panel scripts** |
| `radar-share.html` | **No** | **No** | Map/share only |

### 3.3 What is required to make HE accessible within Radar (implementation outline — not executed)

1. **Host wiring:** Add HE CSS/JS (and required HI/ownership/contact deps already listed on golden-demo) to the Radar/Scout HTML hosts that open hotels — start with the host(s) founders treat as production Radar.
2. **Keep fail-closed share mode:** Use `share=1` / `__HOTEL_EXPLORER_SHARE_MODE` + `projectClientSafeSharePayload` / `enforceClientSafeCustomerSurfaces` for any customer-facing open.
3. **Wave 0 content:** Census + existing structured fields via `assembleHotelExplorerViewModel` — do **not** require Full HI dossiers for every pin (`CALA_HOTEL_EXPLORER_SCALE_ROADMAP.md`).
4. **No new public packs / tokens** without founder approval (Bethesda stays off public HE share packs until approved).
5. **Deploy durability:** Any Radar+HE ship must pass `assert:hotel-explorer-share-assets` / production-required-assets so ADP-only uploads cannot 404 HE again.

### 3.4 Factory / readiness gaps

- Assembler exists (`hotel-explorer-assembler-v2`) and is generic (no hotel-ID switches).
- Packet 2.8 reports: shared assembler historically **partial**; Golden Four still property-specific cohorts; Checkpoint 25 generic assembly **deferred**.
- Ownership/contact readiness (`011-HOTEL-EXPLORER-READINESS.md`): **PARTIAL** — demo fixture dependency for Golden Four.

---

## 4. Verified Hotel Intelligence attribute assessment

### 4.1 Where “verified attributes” live

| Stack | Path | Role |
|---|---|---|
| HI schema / Hotel ADP Attributes | `lib/hotel-intelligence/schema/hotel-intelligence-schema-v1.js`, `adp-attributes/*` | Structured attribute rows; ADP use types; confidence |
| Reality Gap / property attributes | `lib/ai-demand-positioning/intelligence/reality-gap.js`, `property-attributes/*` | Customer labels for AI recognition gaps |
| Source policy | `lib/data-intelligence/source-policy/v1/` | `canDisplayToCustomer` / `canUseForScoring` |
| Client-safe dossier | `lib/hotel-intelligence/dossier/client-safe/*` | Block Webhound, costs, `rec…`, Census product names, ontology codes |
| External copy contract | `docs/data-intelligence/hotel-intelligence-external-facing-report-copy.md` | Binding |

### 4.2 Safe to surface in Hotel Explorer (when verified HIGH/MEDIUM + public source)

- Hotel name, brand/affiliation, official URL  
- Address / city / Dealality Market & Submarket (not STR lineage copy)  
- Rooms/keys, meeting room count, meeting sq ft (with confidence)  
- Customer-labeled amenities from Reality Gap definitions  
- Demand nodes when source policy allows display  
- Public-safe contacts only (`PUBLIC_SAFE`; no inferred emails)

### 4.3 Must stay internal

- Airtable `rec…`, ADP property ids as raw codes, Census/Radar product names  
- Research Center / Webhound / provider costs / session UUIDs / Packet 2.x / auto_promote  
- Ontology codes; Cvent-only / DISCOVERY_ONLY / UNVERIFIED without consent framing  
- GDI research provenance, budgets, win theses  
- Admin publication / certification / fingerprint chrome  

### 4.4 Dual-taxonomy risk

HI Airtable attribute builder and ADP Reality Gap property-attributes are **parallel truths**. Unifying them is a P1 remediation (large effort) before scaling “verified attributes in HE” commercially.

---

## 5. Hotel visibility and publication assessment

### 5.1 HE public inventory

Hardcoded share packs only: **Sheraton GDL, voco Cancún, KGPV, Cambridge (+ mexico hub)**.  
**Bethesda Marriott is not in HE share packs.**

### 5.2 Bethesda Marriott (identity — do not change visibility)

| Field | Value |
|---|---|
| Census / HPC | `recLuxvwwxID7U2B8` |
| ADP property id | `adp_bethesda_marriott` |
| Marriott code | `WASBT` (foundation blocks WASBN / North confusion) |
| ADP published FS | Present under `data/ai-demand-positioning/published/adp_bethesda_marriott/` |
| GDI config | `config/group-demand-intelligence/hotels/recLuxvwwxID7U2B8.json` |
| Canonical ADP scope | Included in `canonical-adp-31-property-scope-v1.js` |
| Cert inventory class | Property QA PASS; **LEGACY_QA_PASS** (not certification-era CERTIFIED) per reconciliation report |
| HE share | **Not listed** |

### 5.3 Publication / visibility gates (keep intact)

- ADP: staged vs published registry; signed share (`adpshare.v1.*`); `ADP_REQUIRE_CERTIFICATION_BEFORE_PUBLISH`; production verification ≠ local FS proof  
- GDI: `REVIEWED_PROSPECT_HOTEL_IDS`, auth states, publication packages, feature flags (local defaults ≠ production)  
- HI: `share=1` → client-safe projection; contact publication rules  
- Brand/OE: External Display Status (separate product)

### 5.4 Local / cloud / customer discrepancies

| Issue | Risk |
|---|---|
| Local GDI flags auto-ON; production OFF unless env set | Local shows hotels customers never see |
| Research store ≠ published inventory | Serving research as customer if gate fails |
| Missing `reports/.../ADP_EXISTING_HOTEL_CERTIFICATION_BASELINE_V1.json` in this tree | Customer surface prior-attach warnings / contract test failure |
| `.railwayignore` lean allowlists + whole-tree `railway up` | Historical HE or ADP wipe |
| Airtable published payload size | `test:ai-demand-positioning-published-foundation` **FAIL** (field limit) |

**This audit made no customer-visibility changes.**

---

## 6. ADP end-to-end workflow and reliability

### 6.1 OS stages (authority)

`lib/dealality-full-e2e-os/adp-full-e2e-workflow-v1.js` (`ADP_OS_STAGES`) +  
`lib/ai-demand-positioning/orchestration/adp-autonomous-hotel-full-e2e-v1.js` +  
lifecycle state machine + permanent Full E2E certification under `certification/permanent-full-e2e/`.

Pipeline framing: identity → HPC onboarding → market registry → scenario universe cert → paid measurement → evidence → competitive resolve → Reality Gap / attributes → certify → stage → founder review → production publish → external share.

### 6.2 Repeatability verdict

| Strength | Weakness |
|---|---|
| Fail-closed certification axes | Multiple entrypoints (OS orchestrator vs per-hotel baseline scripts vs `run-ai-demand-positioning.mjs`) |
| Large gate library (~291 ADP npm scripts) | Permanent Full E2E / persistence / durability scripts **unwired** from `package.json` / CI |
| Actionability contracts (executable actions, no generic slogans) | Most hotels still **LEGACY / grandfathered** publish |
| Leak Audit isolation | Phase 1 synthetic — sales wedge, not certified ADP |
| Inventory reconciliation PASS locally | Customer surface contract FAIL; Airtable payload limit FAIL |

Committed inventory snapshot: **20** properties QA PASS; **3** certification-era CERTIFIED (Hilton, Renaissance, YOTEL); **17** grandfathered (`reports/adp/certification-inventory-reconciliation/FOUNDER_REPORT.md`). Local FS also contains additional published property folders (28 incl. cache) — treat FS listing and cert inventory as related but not identical grains.

### 6.3 CI posture

Only ADP-ish GitHub Action found for PRs: admin workspace navigation. PR matrix does **not** run measurement/Full E2E permanence.

---

## 7. Technical debt, duplication, and architectural risks

| Risk | Severity | Notes |
|---|---|---|
| Radar hosts missing HE scripts | **High** | Blocks “HE in Radar” product promise |
| Railway whole-tree wipe | **High** | Documented HE/ADP mutual wipe history |
| Dual attribute taxonomies | **High** | HI ADP attrs vs Reality Gap |
| Legacy grandfathering | **High** | Formal period comparability weak |
| Unwired Full E2E permanence tests | **Medium** | Scripts exist; npm/CI gap |
| Multiple monitoring entrypoints | **Medium** | Bespoke hotel scripts |
| Executive-read v1/v2/v3 parallel | **Medium** | Customer path ambiguity |
| Leak-audit admin JS forks | **Low–Med** | Parallel admin surfaces |
| HE vs Brand/Operator/Owner Explorer naming | **Low** | Founder confusion |
| Open superseded PR #37 | **Low** | Noise / accidental merge risk |
| Bethesda-specific code paths in GDI | **Med** | Conflicts with hotel-agnostic doctrine |
| Airtable payload size FAIL | **Med** | Blocks clean Airtable publish path |
| Missing certification baseline artifact | **Med** | Breaks prior-attach / contract tests |
| AGENTS.md silent on ADP/HE | **Low** | Operating memory gap |

---

## 8. Commercial usefulness opportunities

Grounded in existing contracts (not invented features):

1. **Radar pin → Hotel Explorer Wave 0** — instant property context from census (rooms, brand, geography) without paid research.  
2. **Verified attribute strip in HE** — rooms, meeting inventory, amenities with confidence; Reality Gap “AI does not recognize X” only after ADP certify.  
3. **Competitive positioning honesty** — CORE peers + “Benchmark not yet certified” language already contracted.  
4. **Executable action plans** — owner role, steps, definition of done, next monitoring check.  
5. **Leak Audit → paid ADP** — clear promotion contract (Phase 1 exists; consolidate admin UX).  
6. **Measurable improvement** — only after ending legacy grandfathering for commercial hotels so period-over-period is real.

---

## 9. Prioritized remediation roadmap

Effort = technical size (small / medium / large). No calendar estimates.

| Pri | Item | Effort | Dependencies | Customer visibility impact |
|---|---|---|---|---|
| **P0** | Wire HE CSS/JS (+ required deps) into chosen Radar host(s); open via `HotelExplorer.open`; Wave 0 assembler | **Medium** | Host selection; client-safe mode; asset assert | Internal/preview first; **no new public packs without approval** |
| **P0** | Wire Full E2E permanence + persistence + durability tests into `package.json` + PR matrix | **Small** | Scripts already on disk | None |
| **P0** | Restore/commit missing certification baseline artifact or fail-soft prior-attach | **Small** | Locate authoritative baseline file | None if restore-only |
| **P0** | Close/archive superseded HE PR #37 | **Small** | Founder confirm | None |
| **P1** | Fix Airtable published payload field-limit FAIL | **Medium** | Compact strategy (Renaissance closeout may help) | Improves ADP Airtable path only |
| **P1** | Unify attribute truth (HI ↔ Reality Gap) | **Large** | Schema stewardship | Enables safer HE attribute strip |
| **P1** | Single ADP Full E2E entrypoint; demote hotel-specific baselines to wrappers | **Large** | Auth ledger | None until publish |
| **P1** | End legacy grandfathering for active commercial hotels (next official period must certify) | **Medium/hotel** | Paid remeasure auth | Publish quality ↑ when approved |
| **P1** | Selective cherry-pick audit of open ADP PRs #41/#44 / Renaissance closeout | **Medium** | Diff vs main | None until merge |
| **P2** | Collapse executive-read to v3 customer path | **Medium** | Edition immutability | Presentation only when activated |
| **P2** | Leak-audit admin consolidation | **Small–Med** | Sales process | Isolated product |
| **P2** | AGENTS.md + ADP operator guide from OS stages | **Small** | Extract docs | None |
| **P3** | Bethesda HE pack / Radar deep-link | **Medium** | Founder visibility approval; client-safe data | **Requires explicit approval** |

---

## 10. Recommended first implementation phase

### Phase 1 — “HE in Radar (Wave 0) + gate wiring” (proposed)

**Goal:** Make the existing complete Hotel Explorer accessible from Radar without new research spend and without changing customer-facing hotel visibility.

**In scope**
1. Choose production Radar host(s) (founder confirm: ranked-list vs Scout vs share).  
2. Script-tag parity with golden-demo for HE open path (or shared partial loader).  
3. Wave 0 view-model from census/assembler for any pin; empty/error UI states required.  
4. Enforce share/client-safe when audience is external.  
5. Add npm aliases for permanent Full E2E / persistence / durability tests; extend PR suggest matrix.  
6. Restore missing certification baseline if the file exists elsewhere in git history.  
7. Tests: asset assert, HE research parity, new “Radar host loads HE primary flag” static assert, Full E2E permanence.

**Out of scope (needs later approval)**
- Bethesda (or any new hotel) on public HE share packs  
- Paid research / Webhound / full-hotel ADP remeasure  
- Airtable production writes / Railway deploy  
- Ending legacy grandfathering for customer metrics  
- Merging open ADP deploy PRs wholesale  

**Success criteria**
- From approved Radar host, clicking a hotel opens **Hotel Explorer** (not only compact panel) for at least Golden Four census IDs in local/dev.  
- No new hotels appear on customer share packs.  
- `assert:hotel-explorer-share-assets` + permanence gates green.  
- Primary GDI checkout remains untouched.

---

## 11. Cost controls (this audit)

| Item | Cost |
|---|---|
| Repo inspection / static analysis | $0 |
| `npm ci` in ADP worktree | $0 (registry packages only) |
| Free local test gates above | $0 |
| Paid provider APIs / AI research / full-hotel reruns | **$0 — not initiated** |
| Railway / production Airtable writes | **$0 — not initiated** |

**Future paid cost (not approved):** ADP remeasure / Webhound escalation / Parallel research — track separately per hotel under founder auth scopes before any `--apply`.

---

## 12. Unresolved questions for founder

1. Which Radar HTML is the **canonical customer/internal** host for HE integration first?  
2. Should Wave 0 HE open for **all census pins** or an allowlist (Golden Four / ADP 31 / reviewed prospects)?  
3. Is Bethesda’s next step **internal HE preview only**, ADP re-certification, GDI publication, or all three in sequence?  
4. Prefer closing PR #37 immediately?  
5. Priority between **HE↔Radar wiring** vs **ADP Full E2E factory** if only one stream can run beside GDI?  
6. Confirm no customer share-token or Railway deploy work until after Phase 1 review.

---

## Appendix A — Evidence index (absolute paths)

- `/agent/repos/my-operators-backend-adp-audit/public/js/hotel-explorer.js`
- `/agent/repos/my-operators-backend-adp-audit/public/js/hotel-detail-panel.js`
- `/agent/repos/my-operators-backend-adp-audit/public/hotel-explorer-share.html`
- `/agent/repos/my-operators-backend-adp-audit/public/hotel-intelligence-golden-demo.html`
- `/agent/repos/my-operators-backend-adp-audit/lib/hotel-intelligence/factory/hotel-explorer-assembler.js`
- `/agent/repos/my-operators-backend-adp-audit/lib/hotel-intelligence/dossier/client-safe/block-patterns.js`
- `/agent/repos/my-operators-backend-adp-audit/lib/dealality-full-e2e-os/adp-full-e2e-workflow-v1.js`
- `/agent/repos/my-operators-backend-adp-audit/lib/ai-demand-positioning/execution/bethesda-marriott-foundation-v1.js`
- `/agent/repos/my-operators-backend-adp-audit/docs/production-deploy/PRODUCTION_BRANCH_DURABILITY.md`
- `/agent/repos/my-operators-backend-adp-audit/reports/hotel-intelligence/packet-2.8/CALA_HOTEL_EXPLORER_SCALE_ROADMAP.md`
- `/agent/repos/my-operators-backend-adp-audit/reports/adp/certification-inventory-reconciliation/FOUNDER_REPORT.md`
