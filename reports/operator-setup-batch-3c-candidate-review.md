# Operator Setup Batch 3C Candidate Review and Implementation Plan

Generated: 2026-06-02  
Evidence sources:
- `reports/operator-setup-staging-diagnostics-soak-results.md`
- `reports/operator-setup-staging-diagnostics-soak-results.json`
- `reports/operator-setup-staging-diagnostics-log-capture.csv`

Scope: Planning only. No code/schema/scoring/display/fallback-order/select-option changes in this step.

---

## 1) Evidence summary from soak

- Soak ran with required staging config (`NODE_ENV=production`, canonical write mode, diagnostics enabled).
- Coverage: **10 profiles**, **91 checks** total, **81 pass / 10 fail**.
- Determinism checks passed:
  - canonical write mode only
  - no shadow/fail-open behavior observed
  - non-rec mock blocking passed
  - read path was `new_base` for tested operator detail flows
- 10 OAS failures were already attributed to **deal-cohort mismatch** (operators not in `companiesForConsideration` for `recIeGRZP21udmTnt`), not a canonical write/read regression.
- Key diagnostics:
  - `serviceModelsSupported` fallback used: **13**
  - `managementStructuresSupported` fallback used: **1** (`bf_selected_deal_structures`)
  - unresolved sparse-field signals present (e.g. `marketPresenceType`, `governanceCadence`, `minimumKeyCount`, etc.) with low count and sparse-profile context
  - `mirror_prefill_applied` / `mirror_write_contract` not captured in API/module soak (browser-only path)

---

## 2) Finding classification table (Batch 3C review set)

| classification | count | notes |
|---|---:|---|
| `confirmed_canonical_mapping_miss` | 1 primary candidate | `serviceModelsSupported` fallback to `primaryServiceModel` observed repeatedly |
| `fallback_acceptable_legacy_only` | 1 candidate | `managementStructuresSupported` single fallback event; likely alias compatibility on deal side |
| `missing_data_issue` | 1 grouped candidate | sparse-profile unresolved alignment fields |
| `display_read_contract_issue` | 1 candidate (deferred check) | `explorerProfileJson` mirror masking is browser-only and not yet evidenced here |
| `deferred_cleanup` | 1 grouped candidate | non-critical legacy/diagnostic cleanup items |
| `business_review_needed` | 1 candidate | management-structure semantics may require owner/deal-side source-of-truth decision |

---

## 3) Candidate-by-candidate review

## Candidate A — `serviceModelsSupported`

- **field/concept:** `serviceModelsSupported`
- **affected file/function:** `public/js/operator-explorer-new-base-profile.js` (`pickField` resolution path for service models); contract test coverage in `scripts/validate-operator-setup-batch-3b-contracts.mjs`
- **canonical key expected:** `serviceModelsSupported`
- **fallback key used:** `primaryServiceModel`
- **count observed:** 13 fallback-used events
- **profiles affected:** mostly sparse-profile cohort records (`recZKRDG8eH2e9Tiy`, `recfQ1w5OR0a5Digr`, `recyHakOliyjuJGKT`, `recWynAfAXb6aznyb`, `recPIkCoZi9FRs8h5`, `recP78Z4fiObIhmCM`)
- **likely root cause:** canonical key not populated consistently for some records while legacy-compatible scalar `primaryServiceModel` remains populated
- **classification:** `confirmed_canonical_mapping_miss` (with some sparse-data overlap)
- **recommended action:** smallest safe Batch 3C fix to ensure canonical `serviceModelsSupported` is populated/resolved for affected records/contracts without changing displayed value logic
- **code change needed:** Yes (targeted mapping/contract closure)
- **data backfill needed:** Possibly minimal, only if records remain missing canonical field after mapping fix
- **UI change needed:** No
- **schema change needed:** No
- **scoring changes needed:** No
- **risk level:** Medium
- **test needed:** Canonical-first contract test + round-trip save/reload/detail/explorer assertion for `serviceModelsSupported`

## Candidate B — `managementStructuresSupported`

- **field/concept:** `managementStructuresSupported`
- **affected file/function:** `lib/operator-alignment-prefill-map.js` (`applyOperatorAlignmentPrefillAliases`)
- **canonical key expected:** `managementStructuresSupported`
- **fallback key used:** `bf_selected_deal_structures`
- **count observed:** 1
- **profiles affected:** not profile-specific in CSV row (alignment alias-resolution context row)
- **likely root cause:** deal-side alias bridge compatibility, potentially expected for legacy deal schema
- **classification:** `fallback_acceptable_legacy_only` **or** `business_review_needed` (pending source-of-truth decision for management-structure ownership)
- **recommended action:** do not include in smallest 3C code slice; confirm intended owner-vs-deal source contract first
- **code change needed:** Not in first 3C slice
- **data backfill needed:** No immediate
- **UI change needed:** No
- **schema change needed:** No
- **scoring changes needed:** No
- **risk level:** Low-Medium
- **test needed:** Focused alias provenance test + deal-linked validation scenario

## Candidate C — unresolved sparse-profile alignment fields

- **field/concept:** grouped unresolved fields (examples: `marketPresenceType`, `governanceCadence`, `minimumKeyCount`, plus other alignment concepts with unresolved count = 1)
- **affected file/function:** `lib/operator-alignment-prefill-map.js` alias resolution diagnostics
- **canonical key expected:** corresponding canonical prefill keys
- **fallback key used:** none (unresolved)
- **count observed:** generally 1 per concept
- **profiles affected:** sparse profiles with very low populated key counts
- **likely root cause:** missing/partial operator data, not mapping defect
- **classification:** `missing_data_issue`
- **recommended action:** treat as data-quality/backfill workstream, not Batch 3C mapping logic change
- **code change needed:** No
- **data backfill needed:** Yes (targeted data completion)
- **UI change needed:** No
- **schema change needed:** No
- **scoring changes needed:** No
- **risk level:** Low for mapping; Medium for demo completeness if left sparse
- **test needed:** Data completeness audit on chosen demo operators

## Candidate D — `explorerProfileJson` mirror masking

- **field/concept:** mirror masking diagnostics (`mirror_prefill_applied`, `mirror_write_contract`)
- **affected file/function:** `public/js/operator-setup-explorer-behavior.js`
- **canonical key expected:** structured prefill keys (concept dependent)
- **fallback key used:** `explorerProfileJson` mirror path where applicable
- **count observed:** not captured in this soak (API/module only)
- **profiles affected:** unknown until browser diagnostic pass
- **likely root cause:** missing browser-side capture in this soak mode
- **classification:** `display_read_contract_issue` (evidence pending)
- **recommended action:** browser-only diagnostic pass before deciding on any 3C change
- **code change needed:** Not yet
- **data backfill needed:** Unknown
- **UI change needed:** No
- **schema change needed:** No
- **scoring changes needed:** No
- **risk level:** Medium (visibility gap)
- **test needed:** browser diagnostics session with contract diagnostics enabled

## Candidate E — OAS deal-linked validation follow-up

- **field/concept:** OAS flow validity for deal-linked operators
- **affected file/function:** runtime validation flow using `/api/operator-alignment-snapshot/:dealId/companies` and deal-scoped cohort selection
- **canonical key expected:** operator appears in deal `companiesForConsideration` when linked
- **fallback key used:** n/a
- **count observed:** 10 flow failures, all attributed to cohort mismatch
- **profiles affected:** all 10 tested profiles in current soak
- **likely root cause:** validation cohort mismatch, not product regression
- **classification:** `deferred_cleanup` (test harness/fixture quality) + operational follow-up
- **recommended action:** run a deal-linked OAS pass (example candidate `recBVEgtm8cS96mu7` if confirmed linked)
- **code change needed:** No product code change required for this plan item
- **data backfill needed:** No
- **UI change needed:** No
- **schema change needed:** No
- **scoring changes needed:** No
- **risk level:** Medium for confidence gate, low for core product regression risk
- **test needed:** OAS deal-linked pass in authenticated staging flow

---

## 4) Proposed Batch 3C implementation scope (smallest possible)

Preferred smallest scope (only if implementation approved):

1. **Target only `serviceModelsSupported` canonical contract closure**
   - Add/repair canonical mapping/read contract so `serviceModelsSupported` is present as canonical source for affected records/flows.
   - Keep compatibility fallback in place.
   - Do not change fallback order unless absolutely required by confirmed contract gap; prioritize canonical population, not resolver reorder.

2. **Add focused contract test**
   - Assert canonical-first resolution for `serviceModelsSupported`.
   - Assert same displayed value/output behavior as current UI.
   - Assert no scoring, threshold, or recommendation impact.

3. **No other behavior changes**
   - No scoring logic changes
   - No display behavior changes
   - No schema/options changes
   - No alias/mirror removal

---

## 5) Do not implement yet

- `managementStructuresSupported` changes, until business/source-of-truth review confirms whether deal-side alias use is expected.
- Sparse-profile unresolved-field fixes as mapping code changes (treat as data completion work first).
- Any `explorerProfileJson` masking fix without browser diagnostic evidence.
- Any OAS logic changes based on current 10 failures (first run deal-linked validation with a confirmed linked operator).

---

## 6) Recommended follow-up validation plan

1. **`serviceModelsSupported` round-trip validation**
   - Save/update through My Operator
   - Reload/prefill and detail readback
   - Explorer resolution confirms canonical key present and preferred
   - Output parity check (no user-visible behavior regression)

2. **OAS deal-linked operator pass**
   - Use `dealId=recIeGRZP21udmTnt`
   - Use operator confirmed in that deal cohort (e.g. `recBVEgtm8cS96mu7` if confirmed)
   - Validate profile + companies APIs and snapshot pages

3. **Browser mirror masking diagnostics check**
   - Enable diagnostics in browser (`localStorage` or global flag)
   - Exercise My Operator prefill + submit flows
   - Capture `mirror_prefill_applied` and `mirror_write_contract` events
   - Classify as mapping issue vs acceptable compatibility behavior

---

## Recommendation

- **Proceed with Batch 3C as a single-candidate, low-blast-radius slice focused on `serviceModelsSupported` only.**
- Keep `managementStructuresSupported`, sparse-data issues, mirror masking, and deal-linked OAS follow-up outside the first code slice until their evidence gates are completed.

