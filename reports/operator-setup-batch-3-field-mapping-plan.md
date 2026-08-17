# Operator Setup Batch 3 Field Mapping Plan (Implementation Plan Only)

Generated: 2026-06-02  
Scope: Batch 3 planning only (no implementation)  
Status: Proposed

## Objective

Resolve unresolved My Operator field mapping contracts end-to-end:

`UI field/key -> request payload key -> API transform -> Airtable table/field -> readback key -> downstream usage`

## Constraints (explicitly preserved)

- No Airtable schema changes yet.
- No field renames/deletes.
- No select/multi-select normalization rollout.
- No scoring methodology changes.
- No Batch 4 work.
- No removal of legacy alias/remap compatibility logic in this batch.

## Evidence used (current-first model)

Current code + runtime-relevant files:
- `public/third-party-operator-setup-new-two.html`
- `public/js/operator-setup-explorer-behavior.js`
- `api/third-party-operator-intake.js`
- `api/lib/operator-setup-new-base-writer.js`
- `api/lib/operator-setup-new-base-build-sheet-rows.json`
- `api/lib/operator-setup-new-base-read.js`
- `api/third-party-operator-detail.js`
- `public/js/operator-explorer-new-base-profile.js`
- `lib/operator-alignment-company-utils.js`
- `api/operator-capability-snapshot.js`
- `api/operator-alignment-snapshot.js`

Live-schema-backed references (clues, not assumed truth):
- `reports/operator-setup-field-inventory.json` (`liveSchema: ok`)
- `reports/my-operator-input-to-airtable-map.json`
- `reports/operator-field-usage-matrix.json`
- `reports/operator-source-of-truth-gaps.json`

---

## Field/Issue Contract Matrix

### P0 — Required before go-live

### B3-P0-01 — `companyLogo` canonical write gap (displayed but not written)
- **Field/concept:** Company logo attachment contract
- **Affected My Operator section:** Profile & Positioning / Operator Quick Facts
- **Frontend key:** `companyLogo` (file input)
- **Request payload key:** multipart file + body payload
- **API file/function:** `api/third-party-operator-intake.js` -> `handleThirdPartyOperatorIntake()` + `submitThirdPartyOperator()`; canonical path calls `writeOperatorSetupToNewBase()`
- **Airtable table/field:** Expected `Operator Setup - Profile & Positioning.companyLogo` (or equivalent attachment field)
- **Readback/detail key:** `pf.companyLogo` -> list/detail logo handling in `operator-setup-new-base-read.js`
- **Downstream affected:** Operator Explorer detail/list, owner-facing profile shell
- **Current issue:** In canonical mode, new-base writer has no attachment handling; legacy path writes `fields['Company Logo']`, canonical path does not.
- **Proposed fix:** Add explicit canonical attachment mapping path to Profile & Positioning row in new-base writer.
- **Schema change needed:** No
- **Code change needed:** Yes
- **UI change needed:** No
- **Data migration needed:** No
- **Snapshot/scoring affected:** No
- **Test needed:** Multipart save -> fetch detail/list -> logo persists after refresh.
- **Risk:** High

### B3-P0-02 — Narrative fields in UI with no confirmed canonical save mapping
- **Field/concept:** `companyHistory`, `differentiators`, `managementPhilosophy`, `companyTagline`, `missionStatement`
- **Affected section:** Profile & Positioning / Company Story & Positioning
- **Frontend key:** same as above (`name=` values confirmed in form)
- **Request payload key:** same keys posted by form
- **API file/function:** canonical `writeOperatorSetupToNewBase()` build-sheet mapping in `api/lib/operator-setup-new-base-writer.js`
- **Airtable table/field:** Expected mostly `Operator Setup - Profile & Positioning.*`
- **Readback/detail key:** `buildBasicsShapedFieldsFromNewBase()` exposes several of these for detail payload
- **Downstream affected:** Explorer detail/profile copy, owner-facing context cards, alignment company narrative context
- **Current issue:** Current committed build-sheet fallback includes `companyDescription` but misses several narrative keys that exist in UI and are read downstream.
- **Proposed fix:** Add missing keys to canonical mapping source (build-sheet rows generation artifact / mapping source), then verify round-trip readback keys.
- **Schema change needed:** No
- **Code change needed:** Yes
- **UI change needed:** No
- **Data migration needed:** Maybe (backfill from legacy if needed)
- **Snapshot/scoring affected:** Indirect (narrative completeness)
- **Test needed:** Save these fields -> read detail prefill -> render in Explorer components.
- **Risk:** High

### B3-P0-03 — Key mismatch families using alias/fallback across read paths
- **Field/concept:** `company_name`/`companyName`/`Company Name`, service model and chain-scale aliases
- **Affected section:** Multiple tabs (Quick Facts, Platform, Commercial)
- **Frontend key:** camelCase form keys
- **Request payload key:** camelCase + `explorerProfileJson` echo keys
- **API file/function:** `buildPrefillObjectFromNewBaseRows()`, `buildBasicsShapedFieldsFromNewBase()`, `public/js/operator-explorer-new-base-profile.js` (`pickField/pickList`), `lib/operator-alignment-company-utils.js`
- **Airtable table/field:** Master/Profile/Platform fields
- **Readback/detail key:** mixed camel/snake/title-case alias reads
- **Downstream affected:** Explorer, Operator Alignment company-level snapshot, score support logic
- **Current issue:** Contract works via fallback lists, but source key is ambiguous and can silently read from alternate fields.
- **Proposed fix:** Define per-field canonical read key and keep fallback keys for compatibility only (explicitly ordered and logged when fallback is used).
- **Schema change needed:** No
- **Code change needed:** Yes
- **UI change needed:** No
- **Data migration needed:** No
- **Snapshot/scoring affected:** Yes (field provenance clarity)
- **Test needed:** Contract tests asserting canonical key hit + fallback telemetry.
- **Risk:** High

### B3-P0-04 — Leadership detail contract mismatch (`role` vs `function`, summary/bio mapping)
- **Field/concept:** Leadership child-row mapping consistency
- **Affected section:** Leadership Team
- **Frontend key:** `exec_*_role`, `exec_*_summary`, `exec_*_bio`, `exec_*_headshot`
- **Request payload key:** same `exec_*` keys
- **API file/function:** `mapExecRowToAirtableChildFields` (writer), `mapNewBaseLeadershipForDetail` + legacy detail shaping
- **Airtable table/field:** `Operator Setup - Leadership Team Members` (`role`, `summary`, `bio`, `headshot`)
- **Readback/detail key:** detail emits `function`, `summary`, `bio`, `shortBio`, `experienceSummary`
- **Downstream affected:** Explorer leadership cards/details, owner diligence view context
- **Current issue:** Write/read keys differ and rely on conversion conventions; easy to regress and hard to validate.
- **Proposed fix:** Publish explicit child-row contract doc in code and add mapper-level assertions for required fields.
- **Schema change needed:** No
- **Code change needed:** Yes
- **UI change needed:** No
- **Data migration needed:** No
- **Snapshot/scoring affected:** No
- **Test needed:** Save leadership rows -> verify deterministic detail shape.
- **Risk:** High

### B3-P0-05 — `explorerProfileJson` legacy mirror contract still partially coupled
- **Field/concept:** Explorer JSON mirror vs canonical top-level fields
- **Affected section:** Explorer payload-bearing subsections (overview/cap/mkt/brand/exec)
- **Frontend key:** explorer keys (`overview_*`, `cap_*`, `mkt_*`, `brand_*`, `exec_*`)
- **Request payload key:** both top-level keys and `explorerProfileJson`
- **API file/function:** `public/js/operator-setup-explorer-behavior.js` enrichment; legacy Basics mirror branch in `api/third-party-operator-intake.js`
- **Airtable table/field:** legacy Basics JSON mirror column vs new-base split tables
- **Readback/detail key:** prefill direct keys + optional `prefill.explorerProfileJson`
- **Downstream affected:** Explorer edit prefill behavior and backward compatibility
- **Current issue:** Mixed contract can mask missing canonical mappings (JSON mirror present while structured rows are missing).
- **Proposed fix:** Keep mirror for compatibility but mark as diagnostic/legacy; require structured key round-trip as source of truth in tests.
- **Schema change needed:** No
- **Code change needed:** Yes
- **UI change needed:** No
- **Data migration needed:** No
- **Snapshot/scoring affected:** Indirect
- **Test needed:** Structured fields persist even when JSON mirror absent.
- **Risk:** High

---

### P1 — Required before external demos

### B3-P1-01 — API mapping exists but UI input missing (`brand_conversion_project_count`)
- **Field/concept:** Brand conversion project count KPI
- **Affected section:** Brand & Relationships
- **Frontend key:** missing input (label appears but no `name="brand_conversion_project_count"` control)
- **Request payload key:** expected `brand_conversion_project_count`
- **API file/function:** canonical build-sheet includes mapping (`operator-setup-new-base-build-sheet-rows.json`)
- **Airtable table/field:** `Operator Setup - Profile & Positioning.brand_conversion_project_count`
- **Readback/detail key:** expected direct prefill/read key
- **Downstream affected:** Brand relationship KPI cards in Explorer
- **Current issue:** Field can be mapped and stored but cannot be entered from current UI.
- **Proposed fix:** Add explicit input control (or remove dependency from demo sections until enabled).
- **Schema change needed:** No
- **Code change needed:** No/Low (if only UI)
- **UI change needed:** Yes
- **Data migration needed:** No
- **Snapshot/scoring affected:** Possibly informational only
- **Test needed:** Enter value -> save -> prefill -> render KPI.
- **Risk:** Medium

### B3-P1-02 — Written but not displayed field cluster
- **Field/concept:** Operational resilience / ESG fields (`emergencyResponse`, `businessContinuity`, `support24x7`, `crisisExperience`, `insuranceCoverage`, `sustainabilityPrograms`, `esgReporting`, `carbonTracking`, etc.)
- **Affected section:** Profile & Positioning extended sections
- **Frontend key:** present in payload paths
- **Request payload key:** matching camelCase keys
- **API file/function:** intake write paths + new-base writer mapping where present
- **Airtable table/field:** mostly `Operator Setup - Profile & Positioning`
- **Readback/detail key:** reader usage currently `No/Unclear` for many
- **Downstream affected:** Explorer and owner-facing pages show little/no usage
- **Current issue:** Data may save but does not appear in owner-facing outputs.
- **Proposed fix:** Classify as either demo-visible fields or deferred capture-only fields; add explicit visibility contract.
- **Schema change needed:** No
- **Code change needed:** Yes (display/read mapping)
- **UI change needed:** Maybe
- **Data migration needed:** No
- **Snapshot/scoring affected:** No
- **Test needed:** Field-level visibility matrix test.
- **Risk:** Medium

### B3-P1-03 — Downstream source-of-truth gap for Operator Capability Snapshot
- **Field/concept:** Capability Snapshot source contract
- **Affected section:** Operator Capability Snapshot
- **Frontend key:** N/A
- **Request payload key:** deal-level only (`dealId`)
- **API file/function:** `api/operator-capability-snapshot.js` + builder utils
- **Airtable table/field:** currently deal-centric; no direct Operator Setup profile pull in endpoint
- **Readback/detail key:** N/A (operator profile keys not directly consumed here)
- **Downstream affected:** Capability Snapshot expectations in owner flow
- **Current issue:** If product expects Operator Setup-backed capability content, current endpoint contract does not explicitly consume it.
- **Proposed fix:** Document current behavior as deal-derived; if operator fields are required, add explicit mapping spec for a later batch.
- **Schema change needed:** No
- **Code change needed:** Maybe (depending on product decision)
- **UI change needed:** Maybe
- **Data migration needed:** No
- **Snapshot/scoring affected:** Yes (scope definition)
- **Test needed:** Contract test proving source fields for capability output.
- **Risk:** Medium

### B3-P1-04 — Alignment/score inputs rely on partial alias fallback contracts
- **Field/concept:** Company-level alignment prefill completeness source keys
- **Affected section:** Operator Alignment Snapshot / Score Breakdown support
- **Frontend key:** Operator Setup canonical keys + aliases
- **Request payload key:** N/A (read-side)
- **API file/function:** `lib/operator-alignment-company-utils.js` (prefill alias fan-in)
- **Airtable table/field:** Master/Profile/Platform one-to-one linked rows
- **Readback/detail key:** `prefill.*` with multi-key fallback
- **Downstream affected:** Alignment company cards + score explanation confidence/completeness
- **Current issue:** Partial fallback strategy can hide missing canonical mappings and produce inconsistent completeness outcomes.
- **Proposed fix:** Lock canonical source keys for alignment inputs and emit explicit “fallback used” diagnostics.
- **Schema change needed:** No
- **Code change needed:** Yes
- **UI change needed:** No
- **Data migration needed:** No
- **Snapshot/scoring affected:** Yes (input provenance, not methodology)
- **Test needed:** Deterministic scoring-input provenance tests by field.
- **Risk:** Medium-High

---

### P2 — Cleanup after go-live

### B3-P2-01 — Airtable field exists but API/UI contract missing (`readyForInvestorPublication`)
- **Field/concept:** Publication readiness flag
- **Affected section:** Governance/publication workflow (future)
- **Frontend key:** none confirmed
- **Request payload key:** none confirmed
- **API file/function:** none confirmed in current intake/new-base writer
- **Airtable table/field:** exists in schema reports (candidate legacy/unused)
- **Readback/detail key:** none confirmed
- **Downstream affected:** none confirmed
- **Current issue:** Schema presence without active contract.
- **Proposed fix:** Keep as “Needs Review” until business workflow decides activation.
- **Schema change needed:** No
- **Code change needed:** Not now
- **UI change needed:** Not now
- **Data migration needed:** No
- **Snapshot/scoring affected:** No
- **Test needed:** Repo + data usage confirmation before activation.
- **Risk:** Low/Unknown

### B3-P2-02 — System/derived fields should remain non-editable
- **Field/concept:** `operator_id`, `submission_status`, `created_at`, `updated_at`
- **Affected section:** System fields
- **Frontend key:** should be none
- **Request payload key:** should not be user-editable
- **API file/function:** master write/read helpers
- **Airtable table/field:** `Operator Setup - Master`
- **Readback/detail key:** visible metadata only where needed
- **Downstream affected:** all read paths relying on metadata integrity
- **Current issue:** Must remain explicitly derived/system to avoid accidental UI edits.
- **Proposed fix:** Add guardrails in contract docs/tests; no UI exposure.
- **Schema change needed:** No
- **Code change needed:** Minimal docs/tests
- **UI change needed:** No
- **Data migration needed:** No
- **Snapshot/scoring affected:** No
- **Test needed:** Ensure these cannot be overwritten from intake payload.
- **Risk:** Low

---

## Coverage against required issue classes

1. Displayed but not written: `companyLogo`, narrative cluster.
2. Written but not displayed: resilience/ESG cluster.
3. Written to one key, read from another: canonical-vs-alias key families.
4. Alias/fallback keys: explorer + alignment prefill readers.
5. No confirmed save path: narrative fields missing in canonical mapping; missing UI control for mapped field.
6. Downstream no confirmed source-of-truth: capability snapshot operator-source ambiguity.
7. Airtable exists but API mapping missing: `readyForInvestorPublication` candidate.
8. API mapping exists but UI missing: `brand_conversion_project_count`.
9. Derived/non-manual fields: master system fields.
10. Runtime validation required: all P0/P1 issues require save->refresh->downstream assertions.

---

## Smallest safe implementation batch for Batch 3 (recommended)

Conservative first slice (no redesign):

1. **Fix canonical save gaps only**  
   - `companyLogo` canonical persistence  
   - narrative fields missing from canonical mapping (`companyTagline`, `missionStatement`, `companyHistory`, `differentiators`, `managementPhilosophy`)
2. **Add one missing UI control where mapping already exists**  
   - `brand_conversion_project_count`
3. **Introduce contract-level tests + diagnostics**  
   - canonical key assertions for alias-heavy families  
   - fallback-used telemetry (read side) for explorer/alignment
4. **No scoring logic changes**  
   - only provenance/contract checks around scoring inputs

This keeps Batch 3 focused on confirmed mapping gaps and contract determinism.

---

## Do Not Touch Yet

1. **Potential legacy but still referenced**
   - Any fields flagged “candidate unused” in Stage 5 without full runtime proof.
2. **Scoring-tied fields**
   - Operator alignment weighted inputs/threshold assumptions (provenance only, not methodology).
3. **Fields blocked on option normalization**
   - Select/multi-select harmonization families (defer to later batch).
4. **Business-review-required fields**
   - Publication/readiness workflow flags like `readyForInvestorPublication`.
5. **Fields likely requiring schema changes later**
   - Any missing contracts that cannot be resolved via current schema + mapping updates alone.

