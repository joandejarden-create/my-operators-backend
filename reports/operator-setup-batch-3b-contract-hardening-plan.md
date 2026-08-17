# Operator Setup Batch 3B Contract Hardening Plan (Planning Only)

Generated: 2026-06-02  
Scope: Batch 3B planning only (no implementation)

## Objective

Harden canonical-vs-fallback contracts so we can clearly tell when downstream output is sourced from:

- canonical Operator Setup keys (intended)
- legacy alias/fallback keys
- `explorerProfileJson` mirror
- transformed/derived compatibility values

without changing user-facing behavior or scoring methodology.

## Hard constraints applied

- No Airtable schema changes.
- No field renames/deletes.
- No select/multi-select normalization.
- No scoring formula/weight/threshold changes.
- No removal of alias/remap compatibility.
- No removal of `explorerProfileJson`.
- No Operator Capability Snapshot logic change.
- No user-facing output change in first implementation slice.

---

## Proposed Items (with requested fields)

### OCS3B-001
- **Priority:** P0
- **Field/concept:** Explorer company identity key provenance (`companyName` family)
- **Current canonical source key:** `companyName` (prefill), `company_name` (Airtable raw)
- **Current fallback/alias keys:** `company_name`, `Company Name`
- **Current file/function using fallback:** `public/js/operator-explorer-new-base-profile.js` -> `pickField(...["companyName","company_name","Company Name"])`
- **Airtable table/field:** `Operator Setup - Master.company_name`; mirrored in Profile row
- **Affected downstream page/function:** Operator Explorer profile model build
- **Current risk:** Silent fallback can mask canonical mapping regressions
- **Proposed diagnostic/contract hardening:** Add non-user-facing provenance hook in `pickField` returning `{value, sourceKey, sourceLayer}` in debug mode/log-only
- **Behavior changes?:** No
- **User-facing output changes?:** No
- **Scoring changes?:** No
- **Test needed:** Unit test proving canonical key selected when present; fallback logged when canonical missing
- **Risk level:** High
- **Rollback approach:** Remove provenance hook call sites; keep existing key order

### OCS3B-002
- **Priority:** P0
- **Field/concept:** Service model key provenance (`serviceModelsSupported` family)
- **Current canonical source key:** `serviceModelsSupported`
- **Current fallback/alias keys:** `serviceModels`, `service_models`, `Service Models Supported`, `Primary Service Model`
- **Current file/function using fallback:** `public/js/operator-explorer-new-base-profile.js` (`pickList` stack), `lib/operator-alignment-prefill-map.js` alias map
- **Airtable table/field:** `Operator Setup - Platform & Markets.serviceModelsSupported` / compatibility columns
- **Affected downstream page/function:** Explorer profile + Alignment company-level context
- **Current risk:** Different pages may resolve different aliases, causing inconsistent interpretation
- **Proposed diagnostic/contract hardening:** Add provenance logging + contract tests asserting canonical-first order in both Explorer and Alignment alias map
- **Behavior changes?:** No (diagnostic only)
- **User-facing output changes?:** No
- **Scoring changes?:** No
- **Test needed:** Contract test matrix for canonical present/absent/alias-only states
- **Risk level:** High
- **Rollback approach:** Remove provenance instrumentation, keep existing lookup arrays

### OCS3B-003
- **Priority:** P0
- **Field/concept:** Chain scale key provenance (`chainScalesSupported` family)
- **Current canonical source key:** `chainScalesSupported`
- **Current fallback/alias keys:** `chainScale`, `Chain Scales You Support`, `Chain Scale`
- **Current file/function using fallback:** `public/js/operator-explorer-new-base-profile.js` + alignment consumer prefill chains
- **Airtable table/field:** `Operator Setup - Profile & Positioning.chainScalesSupported`; platform chain-scale fields
- **Affected downstream page/function:** Explorer and alignment matching narratives
- **Current risk:** Canonical support level may be overridden by looser fallback fields
- **Proposed diagnostic/contract hardening:** Fallback-used trace and tests for deterministic key ordering across readers
- **Behavior changes?:** No
- **User-facing output changes?:** No
- **Scoring changes?:** No
- **Test needed:** Canonical-vs-alias provenance assertions
- **Risk level:** High
- **Rollback approach:** Remove tracing/test-only helpers

### OCS3B-004
- **Priority:** P1
- **Field/concept:** Parent/management ownership alias provenance
- **Current canonical source key:** `parentCompany` (where populated)
- **Current fallback/alias keys:** `platform`, `Platform`, `managementCompany`-style semantics
- **Current file/function using fallback:** `public/js/operator-explorer-new-base-profile.js` parent company pick stack
- **Airtable table/field:** Profile/master ownership/company fields (varies by legacy data)
- **Affected downstream page/function:** Explorer “parent company / platform” display
- **Current risk:** Ownership semantics drift; same UI label from different source concepts
- **Proposed diagnostic/contract hardening:** Add explicit `sourceField` provenance in non-user-facing debug payload; add mapping docs
- **Behavior changes?:** No
- **User-facing output changes?:** No
- **Scoring changes?:** No
- **Test needed:** Snapshot test proving provenance field for parent-company value
- **Risk level:** Medium-High
- **Rollback approach:** Disable provenance metadata emission

### OCS3B-005
- **Priority:** P0
- **Field/concept:** Market/country/region fallback ambiguity
- **Current canonical source key:** `activeCountries`, `activeMarkets`, `regions`/`regionsSupported`, `specificMarkets`
- **Current fallback/alias keys:** `Active Countries`, `Active Markets / Cities`, `active_markets`, `Regions Supported`
- **Current file/function using fallback:** `public/js/operator-explorer-new-base-profile.js` key stacks; `lib/operator-alignment-prefill-map.js` alias map
- **Airtable table/field:** `Operator Setup - Platform & Markets` geographic fields
- **Affected downstream page/function:** Explorer geography cards; alignment company-level narrative context
- **Current risk:** Regional eligibility and narrative confidence may use different sources silently
- **Proposed diagnostic/contract hardening:** Add per-field fallback-used counter/log and contract tests for geography families
- **Behavior changes?:** No
- **User-facing output changes?:** No
- **Scoring changes?:** No
- **Test needed:** Key-order/provenance tests for market/country/region resolution
- **Risk level:** High
- **Rollback approach:** Remove counters/log-only hooks

### OCS3B-006
- **Priority:** P0
- **Field/concept:** Leadership child mapping consistency provenance
- **Current canonical source key:** Airtable child fields `role`, `summary`, `bio`, `headshot`
- **Current fallback/alias keys:** Detail output compatibility keys `function`, `shortBio`, `experienceSummary`, `headshotUrl`
- **Current file/function using fallback:** `api/lib/operator-setup-new-base-read.js` -> `mapNewBaseLeadershipForDetail`
- **Airtable table/field:** `Operator Setup - Leadership Team Members.role/summary/bio/headshot`
- **Affected downstream page/function:** Explorer leadership rendering + detail payload consumers
- **Current risk:** Multiple output aliases can hide write/read contract mismatch
- **Proposed diagnostic/contract hardening:** Mapper contract tests + debug provenance object (`sourceFieldMap`) in non-user-facing mode
- **Behavior changes?:** No
- **User-facing output changes?:** No
- **Scoring changes?:** No
- **Test needed:** Save/read leadership round-trip with provenance assertions
- **Risk level:** High
- **Rollback approach:** Remove debug provenance map and tests only

### OCS3B-007
- **Priority:** P0
- **Field/concept:** `explorerProfileJson` mirror masking risk
- **Current canonical source key:** Structured top-level fields persisted through new-base mappings
- **Current fallback/alias keys:** `explorerProfileJson` merged payload mirror
- **Current file/function using fallback:** `public/js/operator-setup-explorer-behavior.js` (`enrichOperatorSetupSubmitData`, `applyExplorerProfileJsonPrefill`)
- **Airtable table/field:** Legacy Basics mirror field + structured new-base tables
- **Affected downstream page/function:** Edit prefill and Explorer rendering contracts
- **Current risk:** Mirror presence can make missing structured mappings invisible
- **Proposed diagnostic/contract hardening:** Add diagnostic comparison check (structured vs mirror key coverage diff) in non-user-facing logs
- **Behavior changes?:** No
- **User-facing output changes?:** No
- **Scoring changes?:** No
- **Test needed:** Contract test where mirror is present but structured key missing -> flagged in diagnostics
- **Risk level:** High
- **Rollback approach:** Disable comparison logger; retain mirror behavior

### OCS3B-008
- **Priority:** P1
- **Field/concept:** Alignment/score support field provenance visibility
- **Current canonical source key:** `prefill` canonical keys feeding alignment company context
- **Current fallback/alias keys:** `OAS_OPERATOR_PREFILL_KEY_ALIASES` title/snake aliases
- **Current file/function using fallback:** `lib/operator-alignment-prefill-map.js` -> `applyOperatorAlignmentPrefillAliases`
- **Airtable table/field:** Operator Setup profile/platform fields mapped via alias registry
- **Affected downstream page/function:** Operator Alignment Snapshot + score explanation context
- **Current risk:** Cannot distinguish canonical vs fallback source at runtime when diagnosing score context quality
- **Proposed diagnostic/contract hardening:** Add optional provenance map per resolved key (`canonical|alias:title`) for debug/log only
- **Behavior changes?:** No
- **User-facing output changes?:** No
- **Scoring changes?:** No
- **Test needed:** Provenance unit tests around alias resolver
- **Risk level:** Medium-High
- **Rollback approach:** Remove provenance map generation

### OCS3B-009
- **Priority:** P2
- **Field/concept:** Canonical key documentation in code
- **Current canonical source key:** Distributed across reader/writer modules
- **Current fallback/alias keys:** Multiple arrays/maps in separate files
- **Current file/function using fallback:** Explorer profile model builder, alignment alias map, new-base read prefill
- **Airtable table/field:** N/A (documentation-only)
- **Affected downstream page/function:** Engineering maintainability and incident response
- **Current risk:** Future regressions due to unclear canonical precedence
- **Proposed diagnostic/contract hardening:** Add inline “canonical-first contract” comments and short mapping tables in code docs
- **Behavior changes?:** No
- **User-facing output changes?:** No
- **Scoring changes?:** No
- **Test needed:** N/A (docs + lint/static checks only)
- **Risk level:** Low
- **Rollback approach:** Revert docs/comments only

---

## Separation by change type

### A) Safe diagnostics / telemetry only
- OCS3B-001, OCS3B-002, OCS3B-003, OCS3B-004, OCS3B-005, OCS3B-006, OCS3B-007, OCS3B-008  
  (all as debug/log/provenance metadata only; no display/scoring change)

### B) Contract tests only
- OCS3B-001 through OCS3B-008 test components

### C) Read-key ordering changes
- **Deferred unless absolutely necessary.**  
  If any ordering updates are needed, treat as separate PR after diagnostics prove mismatch (likely P1/P2).

### D) Anything that would change user-facing behavior
- **None in initial slice.**  
  Any user-visible value precedence change should be deferred.

### E) Anything that should be deferred
- Any fallback-stack reorder affecting rendered values
- Any score computation/input selection logic change
- Any option normalization or schema migration

---

## Smallest safe Batch 3B implementation slice (recommended)

Conservative first slice (matches your preference):

1. Add fallback-used diagnostics in:
   - `pickField`/`pickList` resolution path (Explorer model)
   - alignment alias resolver (`applyOperatorAlignmentPrefillAliases`)
   - leadership detail mapper output provenance (debug-only)
   - `explorerProfileJson` structured-vs-mirror coverage checks
2. Add contract/provenance tests for:
   - company/service/chain/market resolution families
   - leadership child readback field map
   - mirror masking detection
3. Add explicit canonical key documentation comments near fallback stacks.
4. No user-facing display changes; no scoring changes.

