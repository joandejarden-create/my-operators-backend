# Operator-Side End-State Consistency Audit

**Date:** 2026-05-26  
**Type:** Read-only implementation-readiness review (no code, Airtable, scoring, PDF, or My Deals UX changes in this pass).  
**Regenerate field metrics:** `node scripts/generate-operator-setup-field-coverage-diff.mjs`

---

## 1. Executive Summary

Dealality’s **target architecture** for the operator side is clear and directionally correct: one **Operator Setup** data spine on **new-base Airtable tables**, consumed by **Operator Explorer**, **Operator Alignment Snapshot (OAS)**, **Operator Capability Snapshot (OCS)**, and the **Operator Strategy** table in My Deals, with future **Deal Operator Review Set** and outreach workflow on top.

**What is already consistent**

- **OAS and Operator Strategy share the same company-level model.** Strategy rows are built from `GET /api/operator-alignment-snapshot/:dealId/companies` (same scoring/narrative path as OAS company list).
- **OCS stays deal-scoped** (Strategic Intent / OCS P0 on the deal record), opened from Strategy CTAs; it does not score operators from Operator Setup rows.
- **OAS company scoring reads new-base only** (`loadActiveOperatorCandidatesForAlignment` → `buildPrefillObjectFromNewBaseRows`). Phase B closed **OAS-needed unmapped writer gaps to 0** (audit CSV).
- **Operator Explorer list** reads new-base Master + Profile + Platform (+ child counts) via `GET /api/third-party-operators` — same `rec…` Master ids Strategy uses for “Open Operator Profile.”
- **Operator Strategy** is table-first: cross-deal rows, native My Deals filters, sortable columns, checkboxes, bulk-actions shell, OAS/OCS/Profile CTAs, disabled review/outreach until workflow exists.
- **Phase A + Phase B** delivered an auditable field map, extended the new-base build sheet **103 → 117** rows, and validation scripts for writer coverage.

**What is partially consistent**

- **Operator Setup UI → new-base persistence:** Only **117** of **420** static form fields map to the new-base writer; **281** are Static Form Only; **26** Legacy Only (legacy writer path when flag is off). High-priority OAS/Strategy fields are mapped; most Explorer narrative (`overview_*`, many `cap_*`) is not.
- **Production write path:** `OPERATOR_SETUP_USE_NEW_BASE_WRITER=0` in `.env.example` — **legacy 9-table writer remains default**. New-base is the **read** spine for Explorer/OAS/Strategy but not yet the guaranteed **write** spine in production.
- **Operator Explorer:** List/detail **intended** to use new-base; profile UI is **gold-mock**; **51** Explorer-consumer fields still lack new-base writer mapping; `companyLogo` has no new-base save pipeline; `api/operator-explorer.js` still ships **MOCK_OPERATORS** for legacy explorer detail fallback.
- **Operator Strategy table vs original spec:** **Project Location** is a column; **Alignment Signal** is a **filter only** (not a table column). Review/outreach CTAs are disabled (⋯ menu + bulk menu).

**What is not yet built**

- **Deal Operator Review Set** (Airtable table + APIs + CTA wiring).
- **Add to Operator Review** and **Prepare Outreach** (row and bulk) as live workflow actions.
- **Operator outreach / proposal / selection** workflow end-to-end.
- **Production cutover** of Operator Setup intake to new-base writer with backfill of Active operators.

**What we can claim today (internal/product)**

- The **target operator-side architecture** is defined and largely built for **read/compute surfaces** (OAS, Strategy, Explorer list, OCS deal artifact).
- **OAS and Operator Strategy are connected** through the companies API and shared alignment scoring.
- **New-base Operator Setup is becoming the operator data spine** for those consumers, with Phase B mapping for OAS/Strategy P0 fields complete on paper and in build sheet.
- **Operator Strategy** surfaces operating companies across active deals with scores, review status, key considerations, data confidence, and snapshot/profile actions.

**What we should not claim yet**

- “Operator Setup is fully consistent across all systems.”
- “All Operator Setup UI fields persist to new-base Airtable.”
- “New-base Operator Setup is the production system of record.”
- “Operator Explorer is fully powered by enriched new-base fields.”
- “Operator outreach / proposal workflow is live.”
- “The operator workflow is end-to-end complete.”

---

## 2. Target Architecture Status Table

| Layer | Intended Role | Current Status | Evidence / Files | Can Claim Today? | Remaining Gap | Recommended Next Step |
|-------|---------------|----------------|------------------|------------------|---------------|------------------------|
| **Operator Setup UI** | 13-tab capture for operator profile, footprint, services, diligence, OAS inject fields | **Partial** — 420 static fields; many do not persist to new-base | `public/third-party-operator-setup-new-two.html`, `scripts/he-cala-form-inventory.json`, `public/js/oas-inject-form-fields.js` | **Partial** | 281 Static Form Only; 26 Legacy Only; logo upload not on new-base writer | Keep UI; extend writer in phases; staging saves with flag `1` |
| **New-base Operator Setup Airtables** | System of record (8 tables) | **Partial** — schema + read path live; write path optional via env flag | `api/lib/operator-setup-new-base-read.js`, `reports/operator-alignment-5b-schema-backup-2026-05-25.json` | **Partial** | Production still writes legacy by default; incomplete field population | Staging test + backfill Active masters |
| **New-base Writer** | Map form → new-base columns on save | **Partial** — 117 build-sheet rows + Master admin; Phase B complete | `api/lib/operator-setup-new-base-writer.js`, `api/lib/operator-setup-new-base-build-sheet-rows.json`, `api/lib/operator-setup-new-base-phase-b-fields.json`, `docs/operator-setup-new-base-writer-extension-phase-b.md` | **Partial** | 51 Explorer gaps; `companyLogo` skipped; 26 Legacy Only | Staging `OPERATOR_SETUP_USE_NEW_BASE_WRITER=1`; Phase D Explorer fields |
| **Legacy Writer** | Transitional 9-table persistence | **Active (default)** | `api/lib/operator-setup-write-plan.js`, `OPERATOR_SETUP_USE_NEW_BASE_WRITER=0` in `.env.example` | **Yes (as default)** | Dual truth vs new-base; invisible to OAS if only legacy populated | Freeze legacy additions; plan cutover |
| **Operator Explorer** | Discovery directory (Active operators) | **Partial** — list from new-base; profile gold-mock; mock API fallback | `public/operator-explorer.html`, `public/js/operator-explorer.js`, `api/third-party-operators-list.js`, `api/operator-explorer.js` (MOCK_OPERATORS) | **Partial** | 51 unmapped Explorer fields; overview/cap narrative; logo save | **Phase D** — Explorer integration |
| **Operator Capability Snapshot** | Deal-only capability themes (not operator matching) | **Built** — separate from OAS/Setup spine | `lib/operator-capability-inputs.js`, `public/js/operator-capability-snapshot.js`, `GET /api/deals/:dealId/operator-capability-snapshot` | **Yes** | Must not conflate with OAS scoring | Keep separate; no scoring changes |
| **Operator Alignment Snapshot** | Deal × operator alignment book + companies API | **Built** — company scoring, narrative, PDF/book | `lib/operator-alignment-company-utils.js`, `public/js/operator-alignment-snapshot.js`, `/api/operator-alignment-snapshot/:dealId/profile` + `/companies` | **Yes** | Quality depends on Setup completeness + `dataConfidenceLevel` | Staging validation after new-base saves |
| **Operator Strategy table (My Deals)** | Cross-deal operator-company work queue | **Built** — table-first UX | `public/js/operator-strategy-my-deals.js`, `docs/operator-strategy-my-deals-tab.md`, `scripts/validate-operator-strategy-my-deals-tab.mjs` | **Yes** | N+1 API load; review/outreach disabled; P1 sandbox row model verified, **UI pending auth** ([operator-setup-p1-ui-qa-before-phase-e.md](./operator-setup-p1-ui-qa-before-phase-e.md)) | Authenticated Strategy QA; then Phase E review set |
| **Future Deal Operator Review Set** | Per-deal operator shortlist state | **Not built** | Documented in `docs/operator-alignment-snapshot-phase-4.md`, `docs/operator-side-system-comparison.md` §2.9 | **No** | No table/API; CTAs disabled | Product schema decision → Phase E |
| **Operator outreach / proposal workflow** | Outreach after shortlist | **Not built** | Disabled CTAs in Strategy | **No** | Depends on review set + comms product | After Phase E |

---

## 3. Data Flow Verification

### A. Operator Setup UI → New-base Operator Setup Airtables

**High-value fields now on new-base writer (Phase B + existing build sheet)**

| Area | Examples | Writer path |
|------|----------|-------------|
| Master | `company_name`, `Data Confidence Level`, `Source Type`, `Last Updated Date` | `createOrUpdateOperatorMaster` + `adminMap` |
| Profile | `companyDescription`, `website`, `headquarters`, `primaryServiceModel`, `yearEstablished`, `yearsInBusiness` | Build sheet |
| Platform | `chainScale`, `specificMarkets`, `totalProperties`, `totalRooms`, `brandsPortfolioDetail`; form `regions` → `specificMarkets` | Build sheet + writer coercion |
| OAS inject (Platform/Commercial/Governance) | `activeCountries`, `serviceModelsSupported`, `managementStructuresSupported`, `Offered Services`, `preOpeningSupportCapability`, `brandFamiliesOperated`, `bf_*`, `brand_signal_*`, `cap_kpi_*`, etc. | Already in build sheet (pre–Phase B inventory) |

**Evidence:** `api/lib/operator-setup-new-base-phase-b-fields.json`, `api/lib/operator-setup-new-base-build-sheet-rows.json` (117 rows), `docs/operator-setup-new-base-writer-extension-phase-b.md`.

**Still legacy-only or not on new-base writer (representative)**

| Category | Count (Phase A CSV) | Examples |
|----------|---------------------|----------|
| Legacy Only | 26 | Commercial/Governance `company_name` (blocked — not on live Airtable), tagline/mission, some contact fields |
| Static Form Only | 281 | Most `overview_*`, Owner Value scalars, many deal-term scalars |
| Skipped (Phase B) | 3 | `companyLogo` (no attachment pipeline), `dealTermsOptIn`, `diligenceQaOptIn` (Needs Decision) |

**Fields that still save only through legacy writer when `OPERATOR_SETUP_USE_NEW_BASE_WRITER=0`**

- Any field in the **26 Legacy Only** set and legacy write plan bindings (`api/lib/operator-setup-write-plan.js`, `third-party-operator-new-two-field-bindings.json`).
- Default intake POST path uses legacy until flag is `1`.

**`OPERATOR_SETUP_USE_NEW_BASE_WRITER=1` safety**

| Environment | Assessment |
|-------------|------------|
| **Staging** | **Safe to test** on a disposable test operator: save intake → `GET /api/third-party-operators?activeOnly=1` → OAS `/companies` → Operator Strategy tab. Use `node scripts/test-operator-setup-new-base-save-coverage.mjs` (dry-run); `--apply` only on test data. Optional shadow: `OPERATOR_SETUP_NEW_BASE_SHADOW_WRITE`. |
| **Production** | **Not safe to claim as SOT yet.** Flag remains `0` in `.env.example`. Requires staging QA, Active-operator backfill (`scripts/backfill-operator-setup-alignment-fields.mjs`), and sign-off before cutover. |

**Post–Phase B audit metrics** (`docs/operator-setup-field-coverage-diff.md`):

| Metric | Value |
|--------|------:|
| Build sheet rows | 117 |
| OAS-needed, not new-base mapped | **0** |
| Strategy-needed, not new-base mapped | **2** (Commercial/Governance `company_name` in schema backup only — not live columns) |
| Explorer-needed, not new-base mapped | **51** |

---

### B. New-base Operator Setup Airtables → Operator Alignment Snapshot

| Check | Result |
|-------|--------|
| OAS required fields fully mapped (writer) | **Yes** — audit count **0** OAS-needed gaps |
| OAS reads enriched new-base fields | **Yes** — `loadActiveOperatorCandidatesForAlignment` loads Master/Profile/Platform/Commercial/Governance; `buildPrefillObjectFromNewBaseRows` + granular service prefill |
| Company scores depend on new-base | **Yes** — scoring uses prefill from new-base rows; deal context from deal record |
| `regionsSupported` | **Computed** from `geo_*` totals on read (`operator-setup-new-base-read.js`); form `regions` also maps to `specificMarkets` on write |

**Risks**

- **Stale/empty new-base cells** for Active masters if operators were saved only via legacy writer — scores and narratives degrade until backfill or re-save with flag `1`.
- **`dataConfidenceLevel` / `sourceType` / `lastUpdatedDate`** now mapped on write but must exist on records to surface in Strategy/OAS copy.
- **Scoring weights** unchanged (out of scope) — product must not imply “recommendation” language.

**Evidence:** `lib/operator-alignment-company-utils.js`, `lib/operator-alignment-profile-utils.js`, `docs/operator-alignment-snapshot-implementation-checklist.md`.

---

### C. New-base Operator Setup Airtables → Operator Strategy table

| Check | Result |
|-------|--------|
| Rows use OAS companies endpoint | **Yes** — `GET /api/operator-alignment-snapshot/:dealId/companies` per deal (max 40 deals, concurrency 4) |
| Project / Deal | **Yes** |
| Operating Company | **Yes** (`companyName`, optional `parentCompany`) |
| Project Location | **Yes** (dedicated column; location no longer under deal name) |
| Alignment Signal | **Filter only** — not a table column (band still in row model for filter) |
| Score | **Yes** (`alignmentScoreOptional`) |
| Review Status | **Yes** (`reviewStatusLabel`) |
| Key Consideration | **Yes** |
| Data Confidence | **Yes** (`dataConfidenceLevel`) |
| CTAs | **Yes** — OAS, OCS, Profile (live `rec…`); More menu with disabled Add to Review / Prepare Outreach |

**Strategy-needed gaps (2)**

- Backup-schema-only `company_name` on Commercial/Governance — **not live Airtable**; OAS/Strategy use Master + Profile `company_name`.

**Evidence:** `public/js/operator-strategy-my-deals.js`, `docs/operator-strategy-my-deals-tab.md`.

---

### D. New-base Operator Setup Airtables → Operator Explorer

| Check | Result |
|-------|--------|
| Same new-base read module as OAS list path | **Yes** — `api/third-party-operators-list.js` → `buildNewBaseListRow` |
| Detail prefill | **Yes** — `GET /api/intake/third-party-operators/:id` → `loadNewBaseOperatorBundle` + `buildPrefillObjectFromNewBaseRows` |
| Product UI | **gold-mock** (`operator-explorer-gold-mock.html`) + `operator-explorer-new-base-profile.js` |
| Profile sections / badges from new-base keys | **Yes (Phase D)** — snapshot rail A–G, hero badges, explicit prefill merge |
| Deal-aware alignment panel | **Yes (Phase D)** — `?dealId=` → OAS `/companies` match by `operatorId` |
| Mock fallback | **Dev-only** — `MOCK_OPERATORS` requires `OPERATOR_EXPLORER_ALLOW_MOCKS=1`; live UI opens `rec…` only; no-id page labeled **Sample operator profile** |
| Fully aligned with Phase B enriched fields | **Partial** — display when prefill populated; **51** writer gaps remain on save path |

**Can Explorer be claimed as fully aligned today?** **Partially** — live profile UX is aligned with OAS/Strategy read spine; writer/backfill gaps remain.

**Docs:** [operator-explorer-new-base-integration.md](./operator-explorer-new-base-integration.md) | [operator-setup-to-explorer-field-mapping-audit.md](./operator-setup-to-explorer-field-mapping-audit.md) (pre-Phase E tab/field matrix)

---

## 4. Claims We Can Make Today

Approved internal/product language:

1. **“Our target operator-side architecture is Operator Setup (new-base) → Explorer → OAS → Operator Strategy, with OCS as a separate deal capability artifact and future review/outreach on top.”**
2. **“Operator Alignment Snapshot and the Operator Strategy table share the same company-level operator consideration model via the OAS companies API.”**
3. **“The Operator Strategy tab surfaces operating companies across active deals with alignment scores, review status, key considerations, data confidence, and actions to open OAS, OCS, and Operator Profile.”**
4. **“Operator Capability Snapshot remains deal-focused operating capability screening—not operator matching or scoring.”**
5. **“Phase B extended the new-base Operator Setup writer for high-priority OAS, Explorer list, and Strategy fields; OAS-needed writer mapping gaps are closed in the audit.”**
6. **“Operator Explorer’s published list reads Active operators from new-base Operator Setup tables using the same Master record ids as OAS and Strategy.”**
7. **“Operator Explorer live profiles surface Operator Setup fields used by OAS and Operator Strategy (market presence, operating profile, governance, data confidence) and show deal alignment context when opened with `dealId`.”** (Phase D — not a recommendation or ranking UI)

---

## 5. Claims We Should Not Make Yet

Avoid:

1. “Operator Setup is fully consistent across all systems.”
2. “Every field on the Operator Setup form saves to new-base Airtable.”
3. “New-base Operator Setup is the production system of record.” (flag default is `0`.)
4. “Operator Explorer is fully powered by new-base enriched data.” (51 writer gaps; overview/cap; logo.)
5. “Operator Explorer never uses mock data.” (mock module remains for dev behind `OPERATOR_EXPLORER_ALLOW_MOCKS=1`; sample page without id is labeled.)
6. “Add to Operator Review” or “Prepare Outreach” are live.
7. “The operator workflow is complete end-to-end.”
8. “Alignment Signal appears as a column in Operator Strategy.” (It is a **filter**; location is the column.)

---

## 6. Operator Explorer Gap Review

### Current source files / APIs

| Surface | Path |
|---------|------|
| List UI | `public/operator-explorer.html`, `public/js/operator-explorer.js` |
| Profile UI | `public/operator-explorer-gold-mock.html`, `public/js/operator-explorer-gold-mock-data.js` |
| List API | `GET /api/third-party-operators` (`api/third-party-operators-list.js`) |
| Detail / prefill API | `GET /api/intake/third-party-operators/:id` (`api/third-party-operator-intake.js`, `loadNewBaseOperatorBundle`) |
| Legacy explorer API | `api/operator-explorer.js` (list delegates to third-party-operators; detail **MOCK_OPERATORS** fallback) |

### Current data source

- **List:** New-base Master (Active) + Profile + Platform + case study / diligence counts.
- **Detail:** New-base bundle + `buildPrefillObjectFromNewBaseRows` (same spine as OAS prefill).
- **Strategy Profile CTA:** Same gold-mock embed with `id=rec…`.

### Mock / fallback behavior

- `MOCK_OPERATORS` in `api/operator-explorer.js` for `GET /api/operator-explorer/operator/:id` when no Airtable match — **not** used by primary list (`third-party-operators-list`).
- Gold-mock may show loading/placeholder UX until intake payload loads (product QA item).

### Field verification (Explorer-critical)

| Field | Read from new-base today? | New-base writer today? | Gap |
|-------|---------------------------|-------------------------|-----|
| `company_name` | Yes (Master/Profile) | Yes (Phase B) | — |
| `companyDescription` | Yes | Yes (Phase B) | — |
| `website` | Yes | Yes (Phase B) | — |
| `headquarters` | Yes | Yes (Phase B) | — |
| `companyLogo` | Yes **if** attachment on record | **No** (skipped — multipart) | **High** — Phase C+ pipeline |
| Active Countries | Yes (Platform columns / prefill) | Yes (build sheet / OAS inject) | Populate via save + backfill |
| Active Markets / Cities | Yes (`activeMarkets`, `specificMarkets`, computed regions) | Partial (`specificMarkets`, `regions`→`specificMarkets`) | `geo_*` grid still mostly Static Form Only |
| Market Presence Type | Yes if column populated | In build sheet (verify row) | May be empty without save |
| Service Models Supported | Yes | In build sheet | Same |
| Chain Scales Supported | Yes (`chainScale`) | Yes (Phase B) | Same |
| Management Structures Supported | Yes | In build sheet | Same |
| Offered Services | Yes (governance granular + aggregate) | In build sheet | Same |
| New-Build Opening Experience | Partial (prefill aliases) | Often legacy / static only | Explorer gap row likely |
| Pre-Opening Support Capability | Yes if populated | In build sheet | Same |
| Owner Reporting Level | Partial | Often `cap_*` / governance — many unmapped | Explorer gap |
| F&B Capability Level | Partial | Granular governance columns | Explorer gap |
| Revenue Management Capability | Partial | Granular governance | Explorer gap |
| Sales Platform | Partial | Granular governance | Explorer gap |
| Governance Cadence | Partial | May be legacy or static | Explorer gap |
| Brand Families Operated | Yes if populated | In build sheet | Same |
| Data Confidence Level | Yes (Master) | Yes (Phase B) | Same |
| Source Type | Yes (Master) | Yes (Phase B) | Same |
| Last Updated Date | Yes (Master) | Yes (Phase B) | Same |
| `overview_*` (headlines/stories) | Read if column has data | **Static Form Only** (281 class) | **Major Explorer narrative gap** |

**Phase A / B summary for Explorer:** **51** rows flagged “Explorer consumer, not new-base mapped” (excluding child JSON). Phase B did not target `overview_*` or full `cap_*` grids.

### Recommended Phase D integration plan

1. **Writer:** Add highest-traffic Explorer detail fields to build sheet (prioritize `overview_*` block + `companyLogo` attachment pipeline).
2. **Read:** Confirm `buildPrefillObjectFromNewBaseRows` and gold-mock bind the same keys as list/OAS.
3. **Remove product dependence on mock:** Ensure all production links use `third-party-operators` + intake detail; deprecate `operator-explorer.js` mock path for published operators.
4. **Staging:** Save test operator with `OPERATOR_SETUP_USE_NEW_BASE_WRITER=1` → verify Explorer list + modal match OAS company row.
5. **Docs:** Update Explorer copy to state data source = Operator Setup new-base.

---

## 7. Operator Strategy Gap Review

Validated against `docs/operator-strategy-my-deals-tab.md` and `scripts/validate-operator-strategy-my-deals-tab.mjs`.

| Requirement | Status |
|-------------|--------|
| No deal selector | **Met** |
| No Switch Deal | **Met** |
| No Deal Actions block | **Met** |
| No Operating Pathways section | **Met** |
| No unnecessary header subcopy / in-panel title | **Met** (`aria-label` only) |
| Project / Deal column | **Met** |
| Project Location column | **Met** (single-line styling) |
| CTA column | **Met** |
| OAS / OCS / Profile icons | **Met** |
| Review / Outreach CTAs disabled | **Met** (⋯ More + Bulk Actions menu) |
| Alignment Signal as table column | **Not present** — filter only (documented product drift from early spec) |
| Checkboxes + Bulk Actions | **Met** (bulk items disabled) |

**Conclusion:** Table-first implementation is **aligned** with intended My Deals structure; minor spec drift on Alignment Signal column vs filter.

---

## 8. Future Workflow Gap Review

### Deal Operator Review Set

- **Status:** Not implemented (no Airtable table in repo, no API).
- **Purpose:** Persist per-deal operator shortlist (analogous to brand Target List / contacted pairs).
- **Blocks:** “Add to Operator Review” row CTA, bulk add, outreach eligibility, reporting.

**Schema recommendation (document only):** New table e.g. `Deal Operator Review Set` with links to **Deal** + **Operator Setup Master** (`rec…`), status, added-at, source (Strategy/OAS), optional notes — **requires product sign-off** before implementation.

### Add to Operator Review / Prepare Outreach CTAs

- UI placeholders exist (disabled) in Strategy row ⋯ menu and bulk dropdown.
- No write path.

### Operator outreach / proposal / selection workflow

- Not built; depends on review set + communications product (brand outreach patterns may be reused conceptually).
- OCS and deal Strategic Intent inform **deal** readiness, not operator outreach state.

---

## 9. Recommended Next Sequence

| Order | Step | Rationale |
|------:|------|-----------|
| 1 | **Staging test** `OPERATOR_SETUP_USE_NEW_BASE_WRITER=1` on test operator; run list → OAS companies → Strategy | **Done (P1 proof)** — see [operator-setup-p1-staging-proof.md](./operator-setup-p1-staging-proof.md); sandbox `recBVEgtm8cS96mu7` |
| 2 | **Backfill** Active masters (alignment fields script + selective re-save) | Reduces empty OAS/Strategy rows |
| 3 | **Phase D — Operator Explorer integration** (writer + UI binding for `overview_*`, logo, cap/governance display fields) | Weakest consumer link (51 gaps) |
| 4 | **Validate** Strategy → Profile (gold-mock) → OAS modal on same `rec…` | Confirms id consistency |
| 5 | **Product decision** on Deal Operator Review Set schema | Unblocks Phase E |
| 6 | **Phase E** — Enable Add to Operator Review CTA + persistence | Workflow state |
| 7 | **Phase F** — Operator outreach / proposal workflow | After review set |
| 8 | **Production cutover** new-base writer + retire legacy | Only after 1–4 pass QA |

---

## 10. Final Verdict

| Question | Answer |
|----------|--------|
| Can we claim the architecture as the **target end state**? | **Yes.** |
| Can we claim it is **fully implemented** today? | **No.** |
| Can we claim **OAS and Operator Strategy are connected** today? | **Yes.** |
| Can we claim **Operator Explorer is fully aligned** today? | **Not yet.** |
| Can we claim **new-base Operator Setup is the production source of truth**? | **Not yet** (`OPERATOR_SETUP_USE_NEW_BASE_WRITER=0` default). |
| Can we claim **OCS is separate from operator matching**? | **Yes.** |

**Implementation-readiness score (qualitative):** **~70%** on read/compute UX (OAS, Strategy, OCS, Explorer list); **~35%** on write-path completeness (117/420 form fields, legacy default); **~0%** on review/outreach workflow.

---

## 11. Next Cursor Prompt — Phase D

Use this prompt for the next implementation phase (when approved):

---

**Phase D — Operator Explorer integration using new-base Operator Setup fields**

**Goal:** Align Operator Explorer list and gold-mock profile with the same enriched new-base fields OAS and Operator Strategy depend on, without changing scoring weights, BAS/OCS/OAS PDF layouts, Airtable schema, or Operator Strategy My Deals UX.

**Scope IN:**
- Extend new-base build sheet + writer for Explorer-priority fields (start with `overview_*` block, governance/cap display fields from Phase A CSV “Explorer consumer = Yes”, and `companyLogo` attachment save if feasible).
- Ensure `buildNewBaseListRow` and `buildPrefillObjectFromNewBaseRows` expose the same keys the gold-mock UI renders.
- Remove or gate production reliance on `MOCK_OPERATORS` in `api/operator-explorer.js` for Active `rec…` operators.
- Staging validation script: save test operator with `OPERATOR_SETUP_USE_NEW_BASE_WRITER=1` → list API → detail prefill → compare to OAS company prefill for same Master id.
- Update `docs/operator-setup-field-coverage-diff.md` (regenerate CSV) and Explorer product doc.

**Scope OUT:**
- Scoring weight changes
- OAS / OCS / BAS PDF layout changes
- Operator Strategy table UX changes
- Deal Operator Review Set table creation
- Production `OPERATOR_SETUP_USE_NEW_BASE_WRITER=1` enablement (staging only unless explicitly requested)
- Airtable schema changes

**Acceptance:**
- Explorer list + profile show populated values for Phase D field set on test operator after new-base save.
- Explorer-needed unmapped count reduced materially (target: P0 Explorer list + profile fields).
- No mock operator shown for published Active `rec…` ids on primary Explorer path.
- Regenerated field coverage diff committed.

---

## Audit confirmation

This review made **no** code, Airtable, scoring, PDF, or My Deals UX changes—documentation only.

**Deliverable:** `docs/operator-side-end-state-consistency-audit.md`

**Summary — can claim:** Target architecture; OAS ↔ Strategy connection; OCS deal-only role; Phase B OAS mapping complete; Strategy table-first UX live.

**Summary — cannot yet claim:** Full Setup consistency; new-base production SOT; Explorer fully aligned; review/outreach workflow; end-to-end completeness.

**Recommended next phase:** Phase D (Operator Explorer new-base integration), after staging new-base writer test.
