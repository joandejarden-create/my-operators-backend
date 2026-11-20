# BE_LOCAL_PRODUCT_PARITY_GATE (permanent)

**Contract:** `BE_LOCAL_PRODUCT_PARITY_GATE_V1`  
**Module:** `lib/brand-explorer/be-local-product-parity-gate-v1.js`  
**Orchestrator:** `lib/brand-explorer/be-founder-approval-publication-v1.js` (step `LOCAL_PRODUCT_PARITY`)  
**Config:** `configs/brand-explorer-local-product-parity-gate-contract-v1.json`  
**Audit store:** `reports/brand-explorer/comparison-studio/local-product-parity/`

## Rule

No Brand Explorer profile may be published (production Airtable writeback or customer promotion) unless the **exact approved immutable candidate** has **PASS**ed full content, presentation, and functional parity between:

1. Brand Explorer development / Comparison Studio preview  
2. Canonical local Dealality application  

This applies to initial population, updates, and future automated refreshes.

## Permanent pre-publication product preview

Every brand and refresh must support:

**Research → Stage → Validate → Compare → Preview in Actual Product → Founder Approve → Airtable Writeback → Publish → Verify**

Founder-only modes inside the real `/app#/brand-explorer-combined` product (requires `beInternalPreview=1`):

- **CURRENT SAVED** — canonical Airtable Presentation  
- **PROPOSED UPDATE** — latest cumulative staged candidate via the same product atelier renderer (must match Comparison Studio RIGHT for the same brand / candidate version / payload)

No profile may be published without validating that the proposed content renders correctly inside the actual product — Comparison Studio alone is not sufficient. Proposed preview is read-only (no Airtable writes). Customer sessions without internal preview continue to load CURRENT SAVED until publication is approved.

Wiring gate: `npm run test:brand-explorer-founder-product-preview-v1`  
Lesson: `BE-QL-FOUNDER-PRODUCT-PREVIEW-001`

## Insufficient proof

- HTTP 200 alone  
- Matching Git commits alone  
- Unit-test PASS alone  

## Fail closed when

- Canonical tabs / required subsections missing  
- Material content lost or unexpectedly altered  
- Cards, images, tables, or KPIs differ unexpectedly  
- Style / layout materially regresses  
- Navigation / interactions fail  
- Local app serves outdated or mismatched renderer/assets  
- Preview and local app use different payload versions  
- HPC/Airtable source differences are unreconciled  
- Parity report is missing, stale, or for another candidate hash  

## Preserved gates

Gate 1, Gate 2, research quality, scorecards, founder brand approval, and publication activation remain fail-closed. This gate does **not** create a bypass pathway.

## Tests

```bash
npm run test:be-local-product-parity-gate-v1
npm run test:brand-explorer-local-app-parity
npm run test:be-founder-approval-publication-v1
```

Lesson: `BE-QL-LOCAL-PRODUCT-PARITY-001` in the Founder Quality Lessons Registry.
