# BE Founder Product Preview (permanent)

**Contract:** `BE_FOUNDER_PRODUCT_PREVIEW_V1`  
**Lesson:** `BE-QL-FOUNDER-PRODUCT-PREVIEW-001`

## Problem

Staged Brand Explorer improvements can exist in Comparison Studio while the real `/app#/brand-explorer-combined` product still shows the older Airtable Presentation. Commit parity alone does not publish Presentation content.

## Solution

Founder-only modes in the **actual product atelier**. Platform admins do **not** need to hand-edit the hash:

1. Open `http://localhost:8080/app#/brand-explorer-combined` while authenticated as admin.
2. App shell (`public/app.js` route flag `founderProductPreview`) injects `beInternalPreview=1` into the hash and iframe embed.
3. The atelier shows **Current Saved | Proposed Update**.

| Mode | How | Payload |
|------|-----|---------|
| CURRENT SAVED | Default / toggle | Live Airtable Presentation |
| PROPOSED UPDATE | Toggle (or `beProfileMode=proposed`) | Latest cumulative staged candidate (same assembler + registry as Compare Studio RIGHT) |

Ordinary customers never receive `beInternalPreview` and never see the control.

Publication layers (staged ≠ Airtable ≠ customer): `docs/brand-explorer/BE_PUBLICATION_GAP_AND_CURIO_WRITEBACK_V1.md`.

## Local URLs

**Acceptance target (authenticated founder/admin):**

`http://localhost:8080/app#/brand-explorer-combined`

After admin session resolve, the shell adds `beInternalPreview=1` automatically. Select Curio or Ascend in-product, then toggle Proposed Update.

Optional deep links (still valid; not required for acceptance):

- Curio proposed: `…/app#/brand-explorer-combined?id=receQkxgjlezsc1xg&beInternalPreview=1&beProfileMode=proposed`
- Ascend proposed: `…/app#/brand-explorer-combined?id=reclkgOzvAcBheUSo&beInternalPreview=1&beProfileMode=proposed`
- Comparison Studio: Admin Resources → Brand Explorer Comparison, or `/app#/brand-explorer-compare?beInternalPreview=1&brand=receQkxgjlezsc1xg`

Verified content-hash parity (product PROPOSED ≡ Compare RIGHT): Curio `55a13b6c5fd5…`, Ascend `8b8a73b7c8c4…`. Evidence: `reports/brand-explorer/founder-product-preview-v1/`.

## Safeguards

- Read-only: mode switch never writes Airtable or publishes  
- App-shell auto-preview is **admin-only** (`dealality.isAdmin` / admin nav access)  
- Customer sessions without `beInternalPreview=1` always load CURRENT SAVED  
- No silent Phase 2D / 2F / 2G fallback for proposed mounts  
- Curio / Ascend remain unpublished until founder approval  


## Tests

```bash
npm run test:brand-explorer-founder-product-preview-v1
npm run test:brand-explorer-route-state
```
