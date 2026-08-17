# Dazzler + Trademark Content Thickening (27-brand PVQL)

**Version:** `27-dazzler-trademark-content-thickening-v1`  
**Targets only:** `dazzler-by-wyndham`, `trademark-collection-by-wyndham`  
**Out of scope:** Tapestry, original protected 24, Radisson Collection, images, Company Validated, Source Library, Registry, Brand Status, release fields.

## What this does

Targeted owner-facing content thickness cleanup so Dazzler and Trademark clear remaining PVQL content gates after visual materialization:

- Presentation `Title` / `Body` (scenarios, why_value with **5** bullets, proofs, lifecycle/opening thin slots, Brand Positioning, operator tags)
- Basics `Guest Psychographics Description`, `Brand Positioning`, value/differentiator chips
- Case Summary fill on thin openings (no Image writes)
- Structured Recent Momentum cards (named property + date + https URL)
- Scrub `conversion-friendly` stub chip language → `accessible conversion path`

## npm

```bash
npm run brand-explorer-27-dazzler-trademark-content-thickening -- --brands dazzler-by-wyndham,trademark-collection-by-wyndham --dry-run

npm run brand-explorer-27-dazzler-trademark-content-thickening -- --brands dazzler-by-wyndham,trademark-collection-by-wyndham --apply \
  --approve-content-thickening \
  --confirm-target-brands-only \
  --confirm-targeted-field-fixes-only \
  --confirm-no-company-validation-changes \
  --confirm-no-source-library-status-changes \
  --confirm-no-registry-approval-changes \
  --confirm-no-brand-status-changes \
  --confirm-no-release-field-changes \
  --confirm-no-image-writes \
  --confirm-no-protected-24-brand-changes \
  --confirm-no-tapestry-changes \
  --confirm-no-broad-rewrites
```

## Reports

- `reports/brand-explorer-27-dazzler-trademark-content-thickening.json`
- `reports/brand-explorer-27-dazzler-trademark-content-thickening.md`
- `reports/brand-explorer-27-dazzler-content-thickening.md`
- `reports/brand-explorer-27-trademark-content-thickening.md`

## Acceptance (2026-07-24)

| Gate | Dazzler | Trademark | Notes |
|------|---------|-----------|-------|
| Rendered field completeness | PASS | PASS | 80 fields each |
| No empty rendered components | PASS | PASS | |
| Section pattern parity | PASS | PASS | Structured momentum |
| Golden content quality | PASS | PASS | stub chip cleared |
| Tab Factory audit | PASS | PASS | Added to TARGET_BRANDS |
| PVQL `--public-full-only` | PASS | PASS | **27/27** `overallPass=true` |
| OS release-readiness dry-run | — | — | PASS (`no_action`) |
| Mandatory release gates | — | — | PASS |

Tapestry and original protected 24 remain PASS under PVQL. No CV / Source / Registry / Brand Status / release / image writes in this path.

## Change impact

**High** (Airtable Presentation + Basics owner-copy writes on two Active brands).

**Rollback:** Restore prior Presentation/Basics Body values from Airtable history for the two target brands only. Do not touch Tapestry or protected 24.

## Data contract snapshot

- **Tables:** Brand Setup - Brand Explorer Presentation; Brand Setup - Brand Basics
- **Mapping:** slot keys via full-build packs (`brand-explorer-full-build-content-dazzler-by-wyndham.js`, `…-trademark-collection-by-wyndham.js`)
- **Required for PVQL content:** why_value ≥5 non-empty bullets (renderer pads to 5); scenarios ≥45 words; proofs ≥35; lifecycle ≥35; opening steps ≥30; audience ≥12 words on Basics
- **Forbidden language:** conversion-friendly stub chips; FDD/Item 19/fee/ADR; Company Validated claims; raw URLs except momentum/openings exceptSlots
