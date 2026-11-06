# GDI Universal Operational Workflow — usage & activation

## Success standard

`GDI_UNIVERSAL_WORKFLOW_READY` — one repeatable PowerShell entry for every hotel.

## Commands (PowerShell)

```powershell
# Validate local inventory (safe)
.\scripts\gdi-workflow.ps1 -HotelId "recKRJjcPnb4tVDDS" -Action Validate

# Dry-run Airtable sync (no writes)
.\scripts\gdi-workflow.ps1 -HotelId "recKRJjcPnb4tVDDS" -Action SyncAirtable -DryRun

# FullRun: research preflight + validate + sync dry-run + review package; pauses for approval
.\scripts\gdi-workflow.ps1 -HotelId "recKRJjcPnb4tVDDS" -Action FullRun -DryRun

# After founder approval — dry-run publish plan
.\scripts\gdi-workflow.ps1 -HotelId "recKRJjcPnb4tVDDS" -Action Publish -ApprovalReference "FOUNDER_GO_..." -DryRun

# Live publish (authorized only)
.\scripts\gdi-workflow.ps1 -HotelId "recKRJjcPnb4tVDDS" -Action Publish `
  -Environment Production -ApprovalReference "FOUNDER_GO_..." -Apply

# Full-content parity verify
.\scripts\gdi-workflow.ps1 -HotelId "recKRJjcPnb4tVDDS" -Action Verify

# Portfolio inventory (all hotels)
.\scripts\gdi-workflow.ps1 -Action Inventory
```

Equivalent Node:

```bash
node scripts/gdi-universal-operational-workflow-v1.mjs --hotel <id> --action Validate
```

## Safety defaults

- Dry-run unless `-Apply`
- Research never auto-authorizes paid discovery or Surfe
- Publish never auto-applies without `-ApprovalReference` + Production
- Airtable sync preserves sales-owned fields
- Failed Airtable post-publish stamp does **not** roll back a successful publication

## Activation recommendation

1. Merge this PR (code only).
2. Confirm Airtable env: `AIRTABLE_API_KEY` / `AIRTABLE_GDI_BASE_ID`.
3. Run Validate + SyncAirtable `-DryRun` for Spice, Bethesda, Waterstone.
4. Run Inventory for portfolio parity classification.
5. Production SyncAirtable / Publish only with explicit founder GO per hotel.
6. Do not rotate share tokens.

## Reused modules

- `scripts/gdi-research-to-page-workflow-v1.mjs`
- `lib/group-demand-intelligence/airtable-opportunity-store.js`
- `lib/group-demand-intelligence/durable-publication/gdi-one-click-hotel-publish-v1.js`
- `lib/group-demand-intelligence/presentation/gdi-hotel-full-content-parity-verify-v1.js`
