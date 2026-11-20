# BE Founder Product Preview (permanent)

**Contract:** `BE_FOUNDER_PRODUCT_PREVIEW_V1`  
**Lesson:** `BE-QL-FOUNDER-PRODUCT-PREVIEW-001`

## Problem

Staged Brand Explorer improvements can exist in Comparison Studio while the real `/app#/brand-explorer-combined` product still shows the older Airtable Presentation. Commit parity alone does not publish Presentation content.

## Solution

Founder-only modes in the **actual product atelier** (`beInternalPreview=1`):

| Mode | Query | Payload |
|------|-------|---------|
| CURRENT SAVED | `beProfileMode=saved` (default) | Live Airtable Presentation |
| PROPOSED UPDATE | `beProfileMode=proposed` | Latest cumulative staged candidate (same assembler + registry as Compare Studio RIGHT) |

## Local URLs

On the Brand Explorer workspace server (`:8082`) before merge; on canonical `:8080` after IC merge + post-merge sync.

Curio — product CURRENT SAVED:

`http://localhost:8082/app#/brand-explorer-combined?id=receQkxgjlezsc1xg&beInternalPreview=1&beProfileMode=saved`

Curio — product PROPOSED UPDATE (direct product; no Memberstack gate):

`http://localhost:8082/brand-explorer-combined.html?id=receQkxgjlezsc1xg&beInternalPreview=1&beProfileMode=proposed`

Ascend — product PROPOSED UPDATE:

`http://localhost:8082/brand-explorer-combined.html?id=reclkgOzvAcBheUSo&beInternalPreview=1&beProfileMode=proposed`

Comparison Studio (RIGHT = proposed):

`http://localhost:8082/brand-explorer-compare.html?beInternalPreview=1&brand=receQkxgjlezsc1xg`  
`http://localhost:8082/brand-explorer-compare.html?beInternalPreview=1&brand=reclkgOzvAcBheUSo`

Verified content-hash parity (product PROPOSED ≡ Compare RIGHT): Curio `55a13b6c5fd5…`, Ascend `8b8a73b7c8c4…`. Evidence: `reports/brand-explorer/founder-product-preview-v1/`.

## Safeguards

- Read-only: mode switch never writes Airtable or publishes  
- Customer sessions without `beInternalPreview=1` always load CURRENT SAVED  
- No silent Phase 2D / 2F / 2G fallback for proposed mounts  
- Curio / Ascend remain unpublished until founder approval  

## Tests

```bash
npm run test:brand-explorer-founder-product-preview-v1
npm run test:brand-explorer-route-state
```
