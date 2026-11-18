# Production authorization boundaries

1. Bethesda 27 research CREATEs require explicit founder approval of `gdi-bethesda-research-inventory-27-creates-v1` **and** a separate apply instruction.
2. Rock Creek Venue ID correction requires its own allowlist authorization.
3. Automatic Full E2E research persistence requires env `GDI_RESEARCH_INVENTORY_PRODUCTION_WRITES=1` plus per-run exact write-set tokens.
4. Paid research authorization never unlocks Airtable writes.
5. GDO and customer publication remain independently gated.
6. This package does **not** apply any production writes.
