# Operator Setup Batch F1 — Final Scope (Reconciled)

**Generated:** 2026-06-02  
**Status:** Planning only — do not implement until approved  
**Machine-readable:** `reports/operator-setup-batch-f1-final-scope.json`

## Why the counts disagreed

| Source | Count | What it meant |
|--------|------:|---------------|
| `operator-setup-form-completeness-implementation-plan.json` → `batches.F1` | **6** | Fields with **canonical build-sheet mapping** and **missing `name=` on the My Operator HTML form** (strict automated filter). |
| `operator-setup-form-completeness-summary.md` | **12** | Broader “lowest-risk candidates” from the missing-inputs shortlist — **not** all passed F1 guardrails. |
| **This reconciliation** | **9 approved** | Current-code verified list safe to implement under your guardrails. |
| **Removed** | **4** | Excluded with explicit reasons (see below). |

The summary’s 12 list and the plan’s 6-item F1 **overlap but are not the same set**. This document is the authoritative Batch F1 scope for implementation.

---

## Final approved F1 candidate list (9 fields)

Implement in this order (lowest risk first):

| Order | Canonical key | Section |
|------:|---------------|---------|
| 1 | `minimumKeyCount` | Commercial Fit & Deal Preferences |
| 2 | `similarProjectCaseStudies` | Commercial Fit & Deal Preferences |
| 3 | `brand_soft_independent_narrative` | Brand Relationships & Portfolio Experience |
| 4 | `governanceCadence` | Owner Engagement & Reporting |
| 5 | `infra_technology_maturity_level` | Technology, Systems & Data Infrastructure |
| 6 | `newBuildOpeningExperience` | Operating Platform & Services |
| 7 | `marketPresenceType` | Markets, Segments & Asset Focus |
| 8 | `brandFamiliesOperated` | Brand Relationships & Portfolio Experience |
| 9 | `salesPlatform` | Operating Platform & Services |

**Tiers**

- **F1-A (1–3):** number / long text — no select-option risk.  
- **F1-B (4–9):** single- or multi-select — use options from `api/lib/third-party-operator-new-two-field-bindings.json`; spot-check live Airtable before demo (Batch 4 normalization still deferred).

---

## Fields removed from F1 (4)

| Field | Removed? | Reason |
|-------|----------|--------|
| `brandsPortfolioDetail` | **Yes** | **Not missing.** Form already builds this on save from the Brands Managed grid (`collectBrandsPortfolioDetailFromForm`). A separate textarea would duplicate the JSON contract. |
| `readyForInvestorPublication` | **Yes** | **Business-review / workflow.** Publication gate, not operator profile content. Batch 3 plan: do not activate without workflow sign-off. Bindings use display title “Submit as investor-ready (published profile)” while build-sheet uses `readyForInvestorPublication` — confirm one column before any UI. **Defer to F4** (admin/read-only). |
| `offeredServices` | **Yes** | **Overlaps existing UI.** Support & Services already captures granular multis (`revenueManagementServices`, `salesMarketingSupport`, …). Scoring reads `offeredServices`; adding a second aggregate control risks drift without a defined derivation rule. **Defer to F4.** |
| `brandedVsIndependentMix` | **Yes** | Was in original 6-item F1 but **not** in the summary’s 12. Score- and Explorer-visible; contract exists but score-adjacent. **Defer to F1.5** after the nine approved fields pass validation. |

---

## Per-field confirmation (approved items)

### 1. `minimumKeyCount`

| # | Detail |
|---|--------|
| Airtable table | Operator Setup - Commercial Fit & Terms |
| Airtable field | Minimum Key Count |
| Canonical key | `minimumKeyCount` |
| Writer | Mapped (build-sheet → canonical writer) |
| Readback/prefill | Mapped (`buildPrefillObjectFromNewBaseRows` + alignment aliases) |
| Downstream | Explorer snapshot row; DNA commercial fit fallback |
| Input | number, optional |
| Select options safe | N/A |
| Editable | Yes |

### 2. `similarProjectCaseStudies`

| # | Detail |
|---|--------|
| Airtable table | Operator Setup - Commercial Fit & Terms |
| Airtable field | Similar Project Case Studies |
| Canonical key | `similarProjectCaseStudies` |
| Writer / readback | Mapped |
| Downstream | Explorer snapshot |
| Input | textarea, optional |
| Editable | Yes |

### 3. `brand_soft_independent_narrative`

| # | Detail |
|---|--------|
| Airtable table | Operator Setup - Profile & Positioning |
| Airtable field | `brand_soft_independent_narrative` |
| Canonical key | same |
| Writer / readback | Mapped |
| Downstream | `operator-brand-relationships-sections.js` |
| Input | textarea, optional |
| Editable | Yes |

### 4. `governanceCadence`

| # | Detail |
|---|--------|
| Airtable table | Operator Setup - Governance, Delivery & Diligence |
| Airtable field | Governance Cadence |
| Canonical key | `governanceCadence` |
| Writer / readback | Mapped |
| Downstream | Explorer, OAS prefill, DNA owner engagement |
| Input | single-select, optional |
| Options | Bindings: Monthly, Quarterly, Asset-management style, … |
| Editable | Yes |

### 5. `infra_technology_maturity_level`

| # | Detail |
|---|--------|
| Airtable table | Operator Setup - Governance, Delivery & Diligence |
| Airtable field | `infra_technology_maturity_level` |
| Canonical key | same |
| Writer / readback | Mapped |
| Downstream | Infrastructure section (peer `infra_*` fields already on form) |
| Input | single-select, optional |
| Options | Basic, Structured, Integrated, Advanced |
| Editable | Yes |

### 6. `newBuildOpeningExperience`

| # | Detail |
|---|--------|
| Airtable table | Operator Setup - Commercial Fit & Terms |
| Airtable field | New-Build Opening Experience |
| Canonical key | `newBuildOpeningExperience` |
| Writer / readback | Mapped |
| Downstream | Explorer, OAS; **scoring reads this field** (UI-only F1; no formula change) |
| Input | single-select, optional |
| Options | Strong, Moderate, Limited, None documented, Unknown |
| Editable | Yes |

### 7. `marketPresenceType`

| # | Detail |
|---|--------|
| Airtable table | Operator Setup - Platform & Markets |
| Airtable field | Market Presence Type |
| Canonical key | `marketPresenceType` |
| Writer / readback | Mapped |
| Downstream | Explorer badges/snapshot, OAS |
| Input | multi-select, optional |
| Options | Align with `OAS_MARKET_PRESENCE_TYPE_OPTIONS` / bindings |
| Editable | Yes |

### 8. `brandFamiliesOperated`

| # | Detail |
|---|--------|
| Airtable table | Operator Setup - Profile & Positioning |
| Airtable field | Brand Families Operated |
| Canonical key | `brandFamiliesOperated` |
| Writer / readback | Mapped |
| Downstream | Explorer, DNA brand sections |
| Input | multi-select, optional |
| Options | Marriott, Hilton, …, Other (bindings) |
| Editable | Yes |

### 9. `salesPlatform`

| # | Detail |
|---|--------|
| Airtable table | Operator Setup - Governance, Delivery & Diligence |
| Airtable field | Sales Platform |
| Canonical key | `salesPlatform` |
| Writer / readback | Mapped |
| Downstream | Explorer snapshot; scoring/backfill context |
| Input | multi-select, optional |
| Options | Local sales, Regional sales, … (bindings) |
| Editable | Yes |

---

## Validation plan

**Environment:** `OPERATOR_SETUP_WRITE_MODE=canonical`, `NODE_ENV=production`, diagnostics off for demo parity.

**Per approved field**

1. Save via My Operator canonical intake.  
2. Reload form — prefill shows saved value.  
3. Detail API — canonical prefill key present.  
4. Explorer — displays when applicable (live `rec…` only).  
5. **`newBuildOpeningExperience` only:** two score breakdown runs on a deal-linked operator — scores must be identical (proves UI-only change).

**Regression**

- Brand grid still writes `brandsPortfolioDetail` JSON without a new field.  
- Granular Support & Services unchanged.  
- No accidental write to `submission_status` / `readyForInvestorPublication`.

**Files to touch (when implemented)**

- `public/third-party-operator-setup-new-two.html` — form controls only  
- Optional: focused script `scripts/validate-operator-setup-batch-f1-form-fields.mjs`  
- No changes to scoring modules, schema, or fallback order

---

## Summary answers

1. **Final approved F1:** 9 fields (table above).  
2. **Removed from F1:** `brandsPortfolioDetail`, `readyForInvestorPublication`, `offeredServices`, `brandedVsIndependentMix`.  
3. **Reasons:** see “Fields removed from F1”.  
4. **Implementation order:** 1–9 in approved table.  
5. **Validation:** save → prefill → detail → explorer (+ score stability for `newBuildOpeningExperience`).

**Pause here.** No implementation until you approve this scope.
