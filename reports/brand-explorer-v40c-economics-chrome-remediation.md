# v40C Economics Chrome + Residual Owner Copy Remediation

Generated: 2026-07-22T09:20:31.208Z

Dry-run only. Renderer chrome patched in code. Presentation residual patches planned — not applied.

## Summary

- Brands: 1
- Chrome inventory items patched in code: 9
- Residual Presentation patches planned: 3
- Records patched: 0
- Apply errors: 0
- Internal preview clean (projected): 1/1
- Founder visual review ready (projected): 0
- Incomplete control pass: **no**
- Unlock: **no** · Active approval: **no**

## Economics chrome inventory

### econ_fee_support_item7_loi
- file: `public/js/brand-explorer-atelier-from-api.js` · fn: `econFeeSupportFromApi`
- tab/section: atelier-economics / Typical Economics at a Glance · KPI support notes
- hardcoded=true BrandSetup=true Presentation=false
- internalPreview=true external=false
- status: **patched_in_v40c**
- replacement: Confirm participation costs and timing directly during brand engagement and legal review.

### econ_glance_hint_fdd_item7
- file: `public/js/brand-explorer-atelier-from-api.js` · fn: `renderAtelierEconomicsObligations`
- tab/section: atelier-economics / Typical Economics at a Glance · section hint
- hardcoded=true BrandSetup=false Presentation=false
- internalPreview=true external=false
- status: **patched_in_v40c**
- replacement: Typical ranges and fee-schedule notes from Brand Setup—confirm participation costs, operating obligations, and agreement terms directly during brand engagement and legal review

### econ_confirm_section_fdd_loi
- file: `public/js/brand-explorer-atelier-from-api.js` · fn: `renderAtelierEconomicsObligations`
- tab/section: atelier-economics / Confirm With Brand / Legal Counsel
- hardcoded=true BrandSetup=false Presentation=false
- internalPreview=true external=false
- status: **patched_in_v40c**
- replacement: Confirm With Brand / Legal Counsel + owner-safe diligence body

### econ_disclaimer_fdd_loi
- file: `public/js/brand-explorer-atelier-from-api.js` · fn: `renderAtelierEconomicsObligations`
- tab/section: atelier-economics / How to use this tab
- hardcoded=true BrandSetup=false Presentation=true
- internalPreview=true external=false
- status: **patched_in_v40c**
- replacement: agreement review with the brand and your advisors

### econ_fee_bucket_footnotes
- file: `public/js/brand-explorer-atelier-from-api.js` · fn: `econFeeBucketProofHtml / ECON_FEE_BUCKET_DEFS`
- tab/section: atelier-economics / Fee buckets To Join / To Operate
- hardcoded=true BrandSetup=false Presentation=true
- internalPreview=true external=false
- status: **patched_in_v40c**
- replacement: owner-safe participation / program-cost diligence footnotes

### econ_fee_card_default_body
- file: `public/js/brand-explorer-atelier-from-api.js` · fn: `econFeeCardBodyFromApi`
- tab/section: atelier-economics / Fee type cards from Brand Setup
- hardcoded=true BrandSetup=true Presentation=false
- internalPreview=true external=false
- status: **patched_in_v40c**
- replacement: confirm basis and timing directly with the brand and legal counsel

### econ_cash_steady_fee_stack
- file: `public/js/brand-explorer-atelier-from-api.js` · fn: `ECON_CASH_PHASE_DEFS.steadystate`
- tab/section: atelier-economics / Cash & Capital Rhythm · Steady State
- hardcoded=true BrandSetup=false Presentation=true
- internalPreview=true external=false
- status: **patched_in_v40c**
- replacement: full set of recurring participation costs

### loyalty_net_contribution_default
- file: `public/js/brand-explorer-atelier-from-api.js` · fn: `renderAtelierLoyalty (implPnl default)`
- tab/section: atelier-loyalty / Loyalty implications · P&L
- hardcoded=true BrandSetup=false Presentation=true
- internalPreview=true external=false
- status: **patched_in_v40c**
- replacement: contribution after program costs

### gold_detail_fdd_disclaimer
- file: `public/js/brand-explorer-gold-detail.js` · fn: `working-sample disclaimer`
- tab/section: gold-detail panels (when attached) / Working sample note
- hardcoded=true BrandSetup=false Presentation=false
- internalPreview=true external=false
- status: **patched_in_v40c**
- replacement: publicly available brand materials

## hotel-indigo
- decision: **not_owner_ready**
- internal forbidden 0 → 0
- residual patches: 3

## Designed apply
```
npm run brand-explorer-v40c-economics-chrome-remediation -- \
  --brands hotel-indigo \
  --apply \
  --approve-brand-explorer-v40C-economics-chrome-remediation \
  --confirm-no-company-validation-claim \
  --confirm-no-active-profile-approval \
  --confirm-no-source-library-changes \
  --confirm-no-registry-changes \
  --confirm-no-image-field-changes \
  --confirm-external-profiles-remain-locked \
  --confirm-internal-preview-owner-copy-clean \
  --confirm-brand-only
```
