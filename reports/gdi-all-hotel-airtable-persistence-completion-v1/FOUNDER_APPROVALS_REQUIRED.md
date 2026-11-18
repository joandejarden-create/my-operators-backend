# Founder approvals required

| Gate | Status | Notes |
|------|--------|-------|
| Schema: create GDI Hotel Research Opportunities | **REQUIRED** | Blocks Airtable research-layer durability |
| Research inventory backfill (all hotels) | **REQUIRED after schema** | Exact hotel batches in PRODUCTION_BATCH_MIGRATION_PLAN |
| Qualified GDO creates (non-zero hotels) | **REQUIRED** | Separate from research inventory |
| Rock Creek Venue ID field correction | **REQUIRED (separate allowlist)** | `recf35Q9B6jpX3Ivm` → `pev_53b689545914754d` |
| Customer publication changes | **NOT requested** | Remains independently gated |
| Bulk production apply in this PR | **DENIED** | Fail-closed |

Prior Bethesda / YOTEL authorizations do **not** grant unrestricted write permission.
