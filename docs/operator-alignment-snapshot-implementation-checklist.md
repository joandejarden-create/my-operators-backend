# Operator Alignment Snapshot — Implementation Checklist

**Audit date:** 2026-05-25  
**Full audit:** [operator-alignment-snapshot-audit.md](./operator-alignment-snapshot-audit.md)  
**Status:** Phase 0 complete (read-only). Do not start implementation until product approves §K open questions.

---

## Phase 0 — Audit (done)

- [x] Inventory repo files (Operator Explorer, Setup, OCS, BAS, scoring, intake, My Deals)
- [x] Inventory Airtable tables/fields from code + build sheet + record counts
- [x] Map reusable logic (`scoreOperatorMatchForDeal`, BAS, OCS, readiness)
- [x] Document gaps, risks, score model, UI placement
- [x] Publish audit + this checklist

---

## Phase 1 — Data model & field mapping (no app behavior yet)

### Product sign-off

- [ ] Approve operator **profile category** taxonomy (vs. current `Preferred Third-Party Operator Profile` options)
- [ ] Decide persist scores vs. compute on demand
- [ ] Decide operator shortlist table (Target List analogue) — yes/no and when
- [ ] Confirm OAS vs. OCS entry points on My Deals (separate vs. combined)

### Airtable review (read-only first, then change request doc)

- [x] Run `node scripts/generate-operator-setup-build-sheet-rows.mjs` against live base (2026-05-25 Phase 5B)
- [ ] Confirm legacy `3rd Party Operator - *` table usage (empty / deprecated?)
- [ ] List automations touching: `submission_status`, `readyForInvestorPublication`, deal operator fields
- [x] Phase 5B fields added — see `docs/operator-alignment-phase-5b-schema-implementation.md`

### Mapping artifacts to create

- [x] `fixtures/operator-profile-archetypes.json` — profile layer definitions
- [x] `docs/operator-alignment-field-matrix.md` — deal field ↔ operator field ↔ score factor
- [x] `docs/operator-alignment-recommended-airtable-fields.md` + live schema (Phase 5B)

## Phase 5G — Operator Strategy tab (My Deals) (done 2026-05-25)

- [x] My Deals tab **Operator Strategy** added (after Matched Brands)
- [x] **Table-first UX** — cross-deal pipeline (Project / Deal column)
- [x] Deal selector / switch deal / summary cards / Deal Actions / Operating Pathways **removed** from tab
- [x] Operating Companies for Consideration table + CTA icon column (OAS, OCS, profile, disabled review/outreach)
- [x] Search + alignment + quick filters + Refresh
- [x] Deep link `dealId` → filter chip (not dropdown)
- [x] Client fan-out: `/companies` per deal (capped batch, concurrent)
- [x] `docs/operator-strategy-my-deals-tab.md`
- [x] `scripts/validate-operator-strategy-my-deals-tab.mjs`
- [x] Additive API fields from 5G: `operatingPathLabel`, `dataConfidenceLevel` (no schema / weight / PDF changes)

## Phase 5F — Narrative differentiation polish (done 2026-05-25)

- [x] `lib/operator-alignment-company-narratives.js` — structured Operator Setup → owner-facing narrative pack
- [x] `lib/operator-alignment-operator-narrative-meta.js` — demo archetype labels (narrative only, not scoring)
- [x] `buildCompanyAlignmentResult` exposes rationale, supports, validation, key consideration, questions
- [x] `public/js/operator-alignment-snapshot.js` — prefers server narrative fields for cards + tables
- [x] `scripts/validate-operator-alignment-narrative-diversity.mjs`
- [x] Post-5F audit — `reports/operator-alignment-scoring-phase5f-recIeGRZP21udmTnt.json` (scores match 5C)
- [x] Final narrative prioritization pass — distinctive signals, 4 support bullets max, updated diversity validation
- [x] Final audit — `reports/operator-alignment-scoring-phase5f-final-recIeGRZP21udmTnt.json`
- [x] Executive summary parity — `lib/operator-alignment-executive-summary.js` (6-paragraph summary, Brand Alignment pattern)
- [x] `docs/operator-alignment-phase-5f-narrative-polish.md`
- [x] **No** weight / BAS / OCS / PDF layout / Airtable schema changes

## Phase 5C — Operator Setup backfill (done 2026-05-25)

- [x] `scripts/backfill-operator-setup-alignment-fields.mjs` (dry-run default, `--apply`, `--overwrite`)
- [x] `lib/operator-alignment-operator-backfill-plans.js` — differentiated per-operator plans
- [x] Live option validation before write
- [x] 10 active operators backfilled — `reports/operator-setup-alignment-backfill-2026-05-25T184332.json`
- [x] `scripts/validate-operator-setup-alignment-backfill.mjs`
- [x] Score comparison — `docs/operator-alignment-phase-5c-operator-backfill-results.md`
- [x] Post-5C audit — `reports/operator-alignment-scoring-phase5c-recIeGRZP21udmTnt.json`
- [x] **No** weight / BAS / OCS / PDF changes

## Phase 5E Option Validation / Airtable Field Option Sync (done 2026-05-25)

- [x] Live options export — `reports/operator-alignment-live-airtable-options.json` + `.csv`
- [x] Options audit doc — `docs/operator-alignment-airtable-options-audit.md`
- [x] `lib/operator-alignment-airtable-option-aliases.js` + normalize helpers
- [x] Backfill validates against live options before write
- [x] Scoring uses canonical categories + live option labels
- [x] `node scripts/validate-operator-alignment-airtable-options.mjs`
- [x] Post-sync scoring audit — `reports/operator-alignment-scoring-phase5e-options-validated-recIeGRZP21udmTnt.json`

## Phase 5E — Scoring input wiring (done 2026-05-25)

- [x] `normalizeOperatorAlignmentDealInputs` — structured SI/Deals first, legacy fallback
- [x] Structure factor: brand agreement vs operating model vs management path (not MP Franchise Only alone)
- [x] Service factor: Required / Must-Have Operator Services + scope
- [x] Geography: Market Presence Requirement + Active Countries/Markets
- [x] Stage/pre-opening: Opening Timeline, Pre-Opening Support Needed
- [x] Reporting: Owner Reporting Expectations vs operator reporting level
- [x] Missing data → exclude / Needs Validation (not fake low scores)
- [x] Audit script field-source output — `reports/operator-alignment-scoring-phase5e-recIeGRZP21udmTnt.json`
- [x] `docs/operator-alignment-phase-5e-score-wiring-results.md`
- [x] Narrative: breakdown rationales + reduced OAS default bullets
- [x] **No** weight / BAS / OCS / PDF / Airtable schema changes

## Phase C — Operator Strategy table-first My Deals (done 2026-05-25)

- [x] Native My Deals chrome: title + search + alignment + Reset only (no subcopy, section H3, deal dropdown, refresh button)
- [x] Reload on tab open; deep-link `dealId` → filter chip only
- [x] Removed deal selector, Switch Deal, Deal Actions, Operating Pathways, summary cards
- [x] Cross-deal table: Project Location column; alignment band via filter only
- [x] Columns: Project/Deal, Operating Company, Project Location, Score, Review Status, Key Consideration, Data Confidence, CTA
- [x] Filters: search, alignment signal; deep-link deal chip (no deal dropdown / refresh)
- [x] Deep link `dealId` → filter chip (not selector)
- [x] CTAs: OAS, OCS, Profile active; More (⋯) menu with disabled Add to Review / Prepare Outreach
- [x] `scripts/validate-operator-strategy-my-deals-tab.mjs` updated
- [x] `docs/operator-strategy-my-deals-tab.md` updated
- [x] **No** Airtable schema / scoring / BAS / OCS / OAS PDF changes

## Phase B — New-base writer extension (done 2026-05-25)

- [x] Live Meta API confirmed Master P0 columns (`Data Confidence Level`, `Source Type`, `Last Updated Date`)
- [x] `api/lib/operator-setup-new-base-phase-b-fields.json` + build sheet merge (+14 rows → **117** total)
- [x] Writer: `regions`→`specificMarkets`; Master admin `coerceFieldValue`
- [x] `scripts/validate-operator-setup-new-base-writer-coverage.mjs`
- [x] `scripts/test-operator-setup-new-base-save-coverage.mjs` (dry-run default)
- [x] Regenerated `reports/operator-setup-field-coverage-diff.csv` — OAS unmapped **0**
- [x] `docs/operator-setup-new-base-writer-extension-phase-b.md`
- [x] **No** Airtable schema / scoring / BAS / OCS / OAS PDF / Strategy UX / production writer flag changes
- [x] Staging P1 proof: `node scripts/run-operator-setup-p1-staging-proof.mjs --write` — sandbox `recBVEgtm8cS96mu7` (see [operator-setup-p1-staging-proof.md](./operator-setup-p1-staging-proof.md))
- [x] Human QA (partial 2026-05-26): Explorer gold-mock profile rail **pass**; OAS companies data **pass**; Strategy table UI **pending auth** — [operator-setup-p1-ui-qa-before-phase-e.md](./operator-setup-p1-ui-qa-before-phase-e.md)
- [ ] Authenticated sign-off: My Deals Operator Strategy row + CTAs + embedded Alignment Context
- [ ] Staging: backfill remaining Active masters (beyond P1 sandbox)

## Phase 5B — Schema + mapping + intake (done 2026-05-25)

See [operator-alignment-phase-5b-schema-implementation.md](./operator-alignment-phase-5b-schema-implementation.md).

- [x] Airtable backup + 40 new fields (no renames/deletes)
- [x] Reused chainScalesSupported, Services Required, Operator Strategy Status, Must-Haves, Preferred Deal Structure, bf_selected_deal_structures, specificMarkets
- [x] `lib/operator-alignment-field-options.js`, `lib/operator-alignment-prefill-map.js`
- [x] Deal Setup field lists + Operator Setup bindings/build sheet
- [x] Deal Intake + Operator Setup UI inject (`oas-inject-form-fields.js`)
- [x] Backfill CSV templates (sample operators + deal example)
- [x] `scripts/validate-operator-alignment-phase-5b.mjs`
- [x] Scoring weights unchanged; read-path prefill keys only

### Deal intake gaps (prioritized)

- [ ] Map or add: owner involvement / retain control, institutional reporting, repositioning complexity
- [ ] Normalize `Preferred Third-Party Operator Profile` options OR add parallel `Operator Profile Categories` field
- [ ] Align `Operator Strategy Status` with OAS visibility rules

---

## Phase 1 — Profile-level API foundation (done 2026-05-25)

See [operator-alignment-snapshot-phase-1.md](./operator-alignment-snapshot-phase-1.md).

### API

- [x] `api/operator-alignment-snapshot.js` — `GET /api/operator-alignment-snapshot/:dealId/profile`
- [x] Register route in **`server.js` and `server.upload-ready.js`**
- [x] Load deal via `fetchDealScoringContext` / merged linked records
- [x] `fixtures/operator-profile-archetypes.json` + `lib/operator-alignment-profile-utils.js`
- [x] Return: deal context, profile cards, review considerations, questions, data gaps, methodology note
- [x] **No** specific-operator scoring (`alignmentScoreOptional: null`)

### Copy / compliance

- [x] Archetype fixture copy — neutral tone (validated by `scripts/validate-operator-profile-archetypes.mjs`)
- [x] Alignment labels: Strong / Moderate / Conditional / Limited / Insufficient Data

## Phase 2 — Standalone print page (done 2026-05-25)

See [operator-alignment-snapshot-phase-2.md](./operator-alignment-snapshot-phase-2.md).

- [x] `public/operator-alignment-snapshot.html`
- [x] `public/js/operator-alignment-snapshot.js` + print via `#bas-print-host`
- [x] `public/css/operator-alignment-snapshot.css`
- [x] Server HTML routes (`server.js`, `server.upload-ready.js`)
- [x] `?dealId=` and `?print=1`
- [x] `scripts/test-operator-alignment-snapshot-page.mjs`

### Phase 2 polish pass (done 2026-05-25)

- [x] Deal Context layout — no left clip; light content page; OAS-owned grid styles
- [x] Humanized relevance signal keys (chips, no raw `code` keys)
- [x] Operator Review Signal — level label + rationale + chips
- [x] Profile card hierarchy — hero title/band, matched/conditional chips, section lists
- [x] Strip repeated “Suggested workflow action:” bullet prefixes (renderer only)
- [x] Print/PDF margins, page breaks, band/title prominence
- [x] Extended `scripts/test-operator-alignment-snapshot-page.mjs` checks

---

## Phase 3 — My Deals action / modal (done 2026-05-25)

See [operator-alignment-snapshot-phase-3.md](./operator-alignment-snapshot-phase-3.md).

- [x] `public/my-deals.html` — `data-action="operator-alignment"` + preview modal
- [x] `OperatorAlignmentSnapshot.renderMyDealsPreview` in `public/js/operator-alignment-snapshot.js`
- [x] `public/css/operator-alignment-my-deals-preview.css`
- [x] Top 3 profiles by band + `sortPriority`
- [x] Humanized signal chips in modal
- [x] Open full snapshot + Print links
- [x] Operating Companies for Consideration placeholder (modal only)
- [x] `scripts/test-operator-alignment-snapshot-page.mjs` My Deals checks
- [ ] Optional embed: `?embed=1` on standalone page (deferred)
- [ ] `lib/operator-alignment-rationale.js` (optional dynamic layer — Phase 4+)

---

## Phase 4 — Operating Companies for Consideration (done 2026-05-25)

See [operator-alignment-snapshot-phase-4.md](./operator-alignment-snapshot-phase-4.md).

- [x] Read-only audit of Operator Setup / Explorer / scoring paths
- [x] `lib/operator-alignment-company-utils.js` — wrap `scoreOperatorMatchForDeal`, completeness gate
- [x] `GET /api/operator-alignment-snapshot/:dealId/companies`
- [x] Export `scoreOperatorMatchForDeal` from `api/my-deals.js` (no weight changes)
- [x] My Deals modal: top 3 company cards or gated message
- [x] Standalone snapshot: Operating Companies section or gated copy
- [x] No mock operators; Active `rec…` only
- [x] `scripts/validate-operator-alignment-companies.mjs`
- [x] Phase 4 QA / visibility pass — always show Operating Companies section; dynamic footer; `attachCompaniesSnapshot`; `scripts/qa-oas-companies-sample-deal.mjs`; docs QA subsection
- [x] Phase 4 route/browser/PDF fix — stale server 404 → restart + status-aware errors + `fetchCompaniesApiPack` + OAS startup route log + JS cache bust
- [x] Phase 4 formatting / BAS-parity pass — summary grid, compact cards, 5 print companies, market chips, common gaps, humanized copy, print CSS (`?v=phase4-bas-parity`)
- [x] Phase 4 formatting / BAS-parity pass 2 — light summary cards, 2-col grids, company name fix, capped follow-ups, header-first cards (`?v=phase4-bas-parity-2`)
- [x] Brand Assessment structure parity — 3-page book (cover + narrative + detail), tables + operator-by-operator cards, My Deals stays compact (`?v=brand-assessment-structure`)
- [x] Final Brand Assessment parity polish — one-page cover print, company name before score on detail cards, owner-facing rationale, raw scoring-label cleanup, Current Review Status logic (`?v=bas-parity-polish`)
- [x] Print cover and detail card header fix — fixed-height cover sheet, BAS A4 margins, `oas-operator-detail-title` before meta line (`?v=print-cover-detail-header`)
- [x] BAS print parity — remove cover clip, reset flex viewport for print, logo eager load + print wait (`?v=bas-print-parity`)
- [x] OAS print flat sheets + in-flow cover footer (disclaimer + logo band) (`?v=oas-print-flat`)
- [x] OAS print compact pagination — relax keep rules, cover sheet min-height, no trailing blank page (`?v=oas-print-compact`)
- [x] OAS print reverted to BAS parity — same cover HTML, same `printSnapshot`, no OAS print overrides (`?v=oas-bas-parity`)
- [ ] Per-operator Alignment Detail drawer (`operator-match-score-breakdown`) — deferred
- [ ] Operator shortlist / review set table — Phase 5
- [ ] Extend scoring weights per audit §F — product approval

---

## Phase D — Operator Explorer new-base profile (2026-05-25)

- [x] Audit documented in `docs/operator-explorer-new-base-integration.md`
- [x] `operator-explorer-new-base-profile.js` — field map, badges, snapshot sections A–G
- [x] Gold-mock profile loads intake prefill + merges OAS/Strategy camelCase keys
- [x] Deal-aware **Alignment Context** via `GET /api/operator-alignment-snapshot/:dealId/companies`
- [x] `MOCK_OPERATORS` gated — `OPERATOR_EXPLORER_ALLOW_MOCKS=1` (dev only); sample meta when served
- [x] Live Explorer popup / Strategy CTA — `rec…` only; `dealId` on profile URL
- [x] Demo page labeled **Sample operator profile** (no id)
- [x] `scripts/validate-operator-explorer-new-base-integration.mjs`

## Phase 5 — Operator Explorer integration (follow-on)

- [x] Remove production reliance on `MOCK_OPERATORS` for published records (404 unless dev flag)
- [ ] List card badges: alignment label (when `?dealId=` on Explorer list page), completeness %
- [ ] Link from Explorer list row → OAS with deal context (profile link already opens OAS from alignment panel)
- [ ] Per-operator Alignment Detail drawer — deferred

### Operator Setup form

- [ ] Add/normalize P0 fields identified in audit §I (countries, experience tags, min keys)
- [ ] `setup_completion_score` display for operator admin
- [ ] Required fields before `readyForInvestorPublication`

---

## Phase 6 — PDF / export & sharing (extended)

- [ ] Print CSS parity test (clone `scripts/test-ocs-pdf-export-copy.mjs` pattern)
- [ ] Optional: server-side HTML export for email
- [ ] Deal room / advisor share link (if product requires)

---

## Dependency flags — do not change without full grep

| Field / area | Used by |
|--------------|---------|
| `Current Operating Model`, `Preferred Future Operating Model`, `Operator Strategy Status`, `Operator Capability Priorities` | OCS, deal-setup, OCS intake JS, scoring context |
| `Preferred Third-Party Operator Profile` | deal-setup, readiness, my-deals PATCH, sample deals |
| `submission_status`, `readyForInvestorPublication` | Explorer list, third-party-operators-list |
| `Operator` link on Setup tables | All new-base read/write |
| `bf_*`, `cap_*`, `mkt_*`, `infra_*` | Explorer behavior, prefill, scoring token collectors |
| `Explorer Profile JSON` | Legacy Explorer prefill |
| `Deal Brand Cache` / `Target List` fields | Brand only — pattern reference for operator shortlist |

---

## Server route parity checklist

| Route | `server.js` | `server.upload-ready.js` |
|-------|:-----------:|:------------------------:|
| OCS GET/POST | ✓ | **Fix — add** |
| OAS GET profile | ✓ | ✓ |
| OAS GET companies | ✓ | ✓ |
| OAS HTML page | ✓ | ✓ |
| `operator-match-score-breakdown` | ✓ | ✓ |

---

## Naming consistency (enforce in PR review)

| Use | Do not use for this feature |
|-----|----------------------------|
| Operator Alignment Snapshot | Operator Match, Operator Fit, Operator Recommendation |
| Alignment Signal / Review Consideration | Best operator, recommended path, strongest path |
| Operator Profiles for Review | Operating model recommendation |
| Operator Capability Snapshot (separate product) | Conflate with alignment scoring in UI |

---

## Quick reference — existing assets to clone

| Pattern | Source file |
|---------|-------------|
| Document API | `api/brand-alignment-snapshot.js` |
| Document UI | `public/js/brand-alignment-snapshot.js` |
| My Deals modal | `my-deals.html` → `data-action="brand-alignment"` |
| Scoring engine | `api/my-deals.js` → `scoreOperatorMatchForDeal` |
| Operator load | `api/lib/operator-setup-new-base-read.js` |
| Disclaimers | `lib/operator-capability-copy.js` |
| Deal P0 fields | `lib/operator-capability-inputs.js` |

---

## Recommended next Cursor prompt

After you approve Phase 1 taxonomy and Phase 2 scope, paste the **Recommended next Cursor prompt** at the end of [operator-alignment-snapshot-audit.md](./operator-alignment-snapshot-audit.md).
