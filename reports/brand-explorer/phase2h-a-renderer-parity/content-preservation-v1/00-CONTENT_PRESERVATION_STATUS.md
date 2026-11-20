# Gate 1 — Content Preservation (FOUNDER EVIDENCE)

**overallPass = true**  
**FOUNDER_APPROVED = false**  
**Gate 2 = BLOCKED**  
**Curio / Ascend = NOT APPROVED**  
**Airtable writes = 0**

## Canonical URLs

| Env | URL |
|---|---|
| Product Mount | http://127.0.0.1:8082/brand-explorer-renderer-parity.html?beInternalPreview=1&payload=curio-gate1-renderer-parity-payload.json |
| Comparison Studio · Renderer Parity | http://127.0.0.1:8082/brand-explorer-compare.html?beInternalPreview=1&rendererParity=1 |
| Permanent Comparison Studio | http://127.0.0.1:8082/brand-explorer-compare.html?beInternalPreview=1 |

Payload SHA-256: `6aa7dc98748f9f804364eb40b3513f2c04b9576bd0a3a7314fdd1982973d0591` (frozen; not regenerated)

## Root cause

Content disappeared in Comparison Studio because `splitOeSections` only kept `.oe-section` nodes and **silently dropped non-section product chrome**, notably:

- `.be-standards-meta` (“Last Reviewed: …”) on Owner Considerations / Standards
- Other interstitial markup outside `.oe-section`

That was a **content preservation failure**, not a CSS-only issue.

## Corrected code paths

1. `public/js/brand-explorer-compare-studio.js`
   - **Lossless** `splitOeSections` (walks product root children; never drops non-section nodes; integrity fallback).
   - **Gate 1 Renderer Parity** now mounts the **complete tab HTML** (product-native full-tab) with section anchors for subnav — no splitter truncation on the diagnostic path.
2. `public/css/brand-explorer-compare-studio.css`
   - `.becs-pair { overflow: visible }` so tables/galleries are not clipped.

## Previously missing content (confirmed)

| Tab | Missing block | Cause |
|---|---|---|
| Standards / Owner Considerations | `Last Reviewed: 2026-06-19` (+ standards meta line) | Non-section `.be-standards-meta` omitted by section-only splitter |

After repair: **0 missing tokens** across Overview, Value, Ops, Standards, Economics, Materials (strict phrase check).

## Ten-tab reconciliation

See `01-content-inventory.json` (post-repair).  
`overallPass=true`, `failedTabs=[]`, `missingBlocks=0`.

Scripts:

- `npm` / `node scripts/be-gate1-content-preservation-inventory-v1.mjs`
- `node scripts/be-gate1-find-missing-text-v1.mjs`

## Normal Comparison Studio

Permanent URL uses the same lossless splitter for CURRENT vs PROPOSED product section pairs.  
RIGHT remains the staged candidate (no silent reuse of saved content; no silent 2G fallback).  
Research gaps stay labeled outstanding — not simulated.

## Screenshots

`screenshots/A-product__*.png` · `screenshots/B-compare__*.png`

## Governance

- Frozen Curio Gate 1 payload preserved  
- No content regeneration  
- No Airtable writes / publish / merge / mass population  
- STOP for founder review after content parity evidence  
