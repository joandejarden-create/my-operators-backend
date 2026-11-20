# Curio + Ascend Presentation writeback

**Authorized:** founder chat 2026-11-20  
**Contract:** `BE_CURIO_ASCEND_PRESENTATION_WRITEBACK_V1`  
**Table only:** `Brand Setup - Brand Explorer Presentation`

## Results

| Brand | Status | Airtable writes | Text hash match | Notes |
|-------|--------|-----------------|-----------------|-------|
| Curio `receQkxgjlezsc1xg` | `APPLIED_VERIFIED` | 256 | yes | Fee join retains `$85,000`; scenario.2 title patched to `&` |
| Ascend `reclkgOzvAcBheUSo` | `APPLIED_VERIFIED` | 252 | yes | Cumulative assemble + writeback |

## Payload

Proposed Presentation = **cumulative assembler** (live CURRENT + Phase 2H candidate), not raw candidate-only.

## Product chrome

Registry `founderApprovalStatus` → `FOUNDER_APPROVED_WRITEBACK`. Soft-default profile mode returns to **Current Saved**. Founder Proposed staging banner is hidden on Current Saved for these brands.

## Activation

`configs/brand-explorer-founder-publication-activation-v1.json` set `productionWritesAuthorized: true` for this controlled writeback (automatic publication remains off).
