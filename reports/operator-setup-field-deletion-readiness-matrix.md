# Operator Setup Field Deletion Readiness Matrix

Generated: 2026-06-02

## Bottom line

- **Safe to remove immediately:** **None**
- **Safe to deprecate (keep in schema, stop relying on actively):** limited candidates only, with monitoring/business sign-off first
- **Must keep (compatibility/canonical/scoring/demo-critical):** multiple fields still active

Current evidence shows this is **deletion readiness = not ready** for hard removals.

---

## Evidence used

- `reports/operator-setup-staging-diagnostics-soak-results.json`
  - fallback activity observed historically (`serviceModelsSupported` from `primaryServiceModel`)
  - no stop-condition regressions; diagnostics still indicate active compatibility paths
- `reports/operator-setup-batch-3c-candidate-review.json`
  - confirms compatibility and business-review candidates (e.g., `managementStructuresSupported` alias path)
- `reports/operator-setup-deal-linked-oas-validation.json`
  - canonical `serviceModelsSupported` validated, but `primaryServiceModel` still present and used as compatibility field
- `reports/operator-legacy-unused-fields.json`
  - 100 fields marked `Needs Review` with `No/Unclear` usage, but explicitly non-destructive scan and no data-presence verification

---

## Readiness buckets

## 1) Safe to deprecate now (not delete yet)

These are candidates for **deprecation workflow only** (announce, monitor, then consider removal later):

- `overview_signal_1_value`
- `overview_signal_2_value`
- `overview_signal_3_value`
- `brand_signal_audit`
- `brand_signal_reflag`
- `brand_signal_franchise_align`
- `brand_signal_soft_retention`
- `readyForInvestorPublication`

Why only deprecate:
- current scan marks them `No/Unclear` and `Needs Review`, not proven unused across all runtime contexts
- no data-presence and downstream dependency lockout proof yet

---

## 2) Keep for compatibility (do not remove)

- `primaryServiceModel`
  - compatibility fallback for `serviceModelsSupported` remains intentionally in place
- `company_name` / `Company Name` compatibility family
  - legacy/read compatibility paths still present in explorer/detail ecosystems
- Alias bridge fields used in alignment prefill contexts (example: `bf_selected_deal_structures` for management-structure compatibility)

Reason:
- active or recent compatibility usage and fallback behavior has been explicitly preserved by approved scope

---

## 3) Do not touch

- Canonical operator setup fields validated in Batches 1/2/3A/3C
- Any fields affecting scoring/snapshot/deal-alignment methodology
- Any fields under browser-only diagnostics paths pending explicit validation
- Anything tied to Batch 4 normalization planning

---

## Decision

- **Hard delete recommendation today:** **No**
- **Next safe action:** run a formal deprecation cycle, not deletion:
  1. mark candidates as deprecated in docs
  2. monitor 1-2 release cycles with diagnostics/log checks
  3. verify no data writes/no reads/no exports/no business workflows
  4. only then propose field removal batch

---

## Required before any removal batch

1. Data presence check per candidate field (non-empty record counts).
2. Read/write/export/report usage sweep in current repo and active automations.
3. Business owner sign-off per field.
4. Rollback plan for each deletion candidate.
5. Staging proof that UI/API/scoring are unchanged with candidate fields disabled.

