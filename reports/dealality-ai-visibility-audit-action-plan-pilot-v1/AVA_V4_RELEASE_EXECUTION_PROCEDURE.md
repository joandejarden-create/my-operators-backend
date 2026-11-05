# AVA V4 Release Execution Procedure

**Deploy authorized:** NO — founder authorization required  
**Publication authorized:** NO — separate founder authorization required  

## Exact release identity

- Branch: `release/ava-v4-portfolio-certified`
- Content commit: `2f14f0e6bea272b02e8c1f367f0b0e26e0e0544c`
- Base: `origin/main`
- Artifact inventory: `reports/dealality-ai-visibility-audit-action-plan-pilot-v1/AVA_38_HOTEL_FINAL_MANIFEST.json`

## Pre-deploy (already runnable locally)

```bash
node scripts/ava-v4-verify-38-manifest-v1.mjs
node scripts/test-ava-customer-report-quality-standard-v1.mjs
node scripts/test-ava-pdf-channel-parity-v1.mjs
node scripts/test-ava-durable-storage-gates-v1.mjs
node scripts/test-ava-admin-workflow-v1.mjs
AVA_STORAGE_MODE=local node scripts/ava-v4-stage-certified-drafts-v1.mjs --dry-run
```

## Founder authorization A — production deploy + draft transfer

1. Record authorization in chat / ticket.
2. Backup production AVA published PDFs + meta (if any exist on Railway volume).
3. `npm run deploy:production` from the authorized PR tip only.
4. On production host / with production storage credentials:
   ```bash
   AVA_STORAGE_MODE=required NODE_ENV=production \
     node scripts/ava-v4-stage-certified-drafts-v1.mjs \
     --authorize-production-draft-transfer
   ```
5. Re-verify all 38 draft SHA-256 values against the final manifest.
6. Confirm Admin Preview and Download return identical draft bytes (`channel=draft`).
7. Confirm published channel unchanged (`channel=published`).

## Founder authorization B — customer publication (later)

```bash
AVA_STORAGE_MODE=required NODE_ENV=production \
  node scripts/ava-v4-promote-certified-published-v1.mjs \
  --authorize-customer-publication --all-38
```

Fail-closed on SHA mismatch. Prior published.pdf saved as published.prev.pdf.

## Rollback

1. Redeploy previous production commit via `npm run deploy:production`.
2. Restore AVA published.pdf + meta from pre-deploy backup if draft transfer already ran.
3. Do not rotate share tokens; do not touch ADP/GDI published artifacts.

## Explicit non-goals of this release

- No paid research
- No ADP calculation changes
- No GDI opportunity changes
- No share-token registry changes
- No automatic customer publication
