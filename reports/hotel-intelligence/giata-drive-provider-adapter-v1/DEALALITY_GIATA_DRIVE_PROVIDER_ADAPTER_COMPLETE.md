# DEALALITY_GIATA_DRIVE_PROVIDER_ADAPTER_COMPLETE

**Generated:** see `adapter-validation-summary.json`  
**Role:** Complementary identity + geo/brand enrichment via existing Hotel Intelligence MCP provider contract.

## Safety

| Control | Result |
| --- | --- |
| Airtable writes | 0 |
| Census writes | 0 |
| Brand Explorer writes | 0 |
| Automatic merges | 0 |
| Schema changes | 0 |
| Migrations | 0 |
| Secrets exposed | false |

Flags forced: `ENABLE_HOTEL_INTELLIGENCE_AIRTABLE_WRITES=0`, `ENABLE_HBX_CENSUS_WRITES=0`.

## Provider

- Registered: `giata_drive`
- Enable: `HOTEL_INTELLIGENCE_GIATA_DRIVE=1` (default `0`)
- Auth: Bearer `GIATA_DRIVE_API_KEY`
- Client: `lib/research-engine-v2/providers/giata-drive/`
- Adapter: `lib/hotel-intelligence/providers/giata-drive.js`
- Sync state: `giata-drive-sync.js` → local `GIATA_OPEN_CONTENT_REMOVED` (not hotel closed)

## Explicit non-capabilities

- NOT primary CALA universe source
- NOT total room-count (`roomTypes[]` firewalled)
- NOT MultiCodes / supplier crosswalk

## Commands

```bash
npm run test:hotel-intelligence-giata-drive
npm run hotel-intelligence:giata-drive-adapter-validation
```

Artifacts: `reports/hotel-intelligence/giata-drive-provider-adapter-v1/`
