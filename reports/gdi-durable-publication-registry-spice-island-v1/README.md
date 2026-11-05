# GDI Durable Publication Registry — Spice Island Prep

**Status:** FOUNDER_GO_REQUIRED_FOR_PRODUCTION_ACTIVATION  
**Generated:** 2026-11-05T12:52:20.646Z

## Founder classification (honored)

| Class | Count | IDs |
|-------|------:|-----|
| Sales Ready | 1 | `gdi_s4912_ba7b1cb8a3ab` (CARPHA) |
| Account Development | 6 | Beautiful Weddings, Hangers, A'yesha, St. James, Savor 10, Kenwood |
| Rejected / excluded | 1 | Aventa `gdi_sc_f255eb5e9202` |

Classification approval ≠ publication authorization.

## 1. Recommended storage architecture

**Primary SoT: Airtable GDI base** (already production opportunity persistence; weekly updates without redeploy).  
**Secondary mirror: Railway volume filesystem registry** (`GDI_PUBLISHED_INVENTORY_ROOT`) using the implemented `GDI_PUBLISHED_INVENTORY_REGISTRY_V1` schema (ADP durable-store pattern).  
**Not primary:** Waterstone-style `lib/` packs (code deploy required).

## 2. Existing-system compatibility

- Reviewed-prospect allowlist already includes Spice Island  
- `gdishare.v1` reads hotel opportunities via `loadOppDoc` — merge hook prepared, **not enabled**  
- Waterstone lib pack retained as freeze fallback until separate migration GO  

## 3. Migration / rollback

See `04-migration-rollback-and-activation.json`.

## 4. Durability tests

Suite: `node scripts/test-gdi-published-inventory-registry-v1.mjs` → **PASS** (11/11)

Covered: publish, update, revoke, history, unauthorized promotion block, restart reload, simulated volume redeploy, multi-hotel, weekly incremental, unpublished research leakage prevention, Account Development first-class.

## 5. Exact seven Spice Island publication IDs

- `gdi_s4912_ba7b1cb8a3ab` — SALES_READY
- `gdi_s4912_b956aa67a4a2` — ACCOUNT_DEVELOPMENT
- `gdi_s4912_b6d9b1929c05` — ACCOUNT_DEVELOPMENT
- `gdi_s4912_cb2d71ab1b15` — ACCOUNT_DEVELOPMENT
- `gdi_s4912_cc2a8da6ca6e` — ACCOUNT_DEVELOPMENT
- `gdi_s4912_03deda917778` — ACCOUNT_DEVELOPMENT
- `gdi_s4912_3cafec77a21f` — ACCOUNT_DEVELOPMENT

Manifest validation: **PASS**

## 6. Production deployment risks

- Enabling merge without dual-write could hide Airtable-only rows if strictPublishedOnly mis-set — default merge is additive
- Railway volume not yet attached — FS mirror empty until mount + env
- Migrating Waterstone away from lib pack without registry rows would drop share inventory — out of scope until separate GO
- Share token issuance still requires production GDI_SHARE_CAPABILITY_SECRET (already configured)
- Classification approval ≠ publication approval — this package stops before sync/token

## 7. Required founder authorization

```
FOUNDER GO — GDI durable publication registry production activation: deploy GDI_PUBLISHED_INVENTORY_REGISTRY_V1, preserve existing published inventories, sync Spice Island exact seven IDs, verify production, issue gdishare.v1 for recKRJjcPnb4tVDDS
```

No production writes, deployments, paid research, or token issuance executed.
