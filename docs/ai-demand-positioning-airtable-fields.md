# AI Demand Positioning — Airtable schema (V1 proposal)

> **Status:** Proposed — apply via `npm run ensure:ai-demand-positioning-schema` with `ADP_SCHEMA_APPLY=true`  
> **Raw monitoring corpora stay outside Airtable.** Only published UI snapshots + metadata are stored here.

## Architecture

| Layer | Storage | Contents |
|-------|---------|----------|
| Raw monitoring | `data/ai-demand-positioning/runtime/` (internal) | Full provider responses, 260+ observations |
| Published snapshot | `fixtures/ai-demand-positioning/published/` + optional Railway volume | Pre-computed owner payload + evidence index JSON |
| Airtable (Live) | `AI Demand Positioning - Published Reports` | Metadata, census link, **Payload JSON** (~15–20 KB). Evidence index stays on disk when > field limit. |

### Production read priority

1. **Airtable Live** (when `ADP_AIRTABLE_READ_LIVE=1`) — Payload JSON  
2. **Published snapshot files** — report + evidence index  
3. **Compute from raw period** — dev fallback only  

## Census linking (future-ready)

When a Hotel Property Census record exists:

| Field | Purpose |
|-------|---------|
| `ADP Property ID` | Stable internal id (`adp_waterstone_boca_raton`) — join key before census exists |
| `Census Record ID` | Text `rec…` id for scripts and audit |
| `Linked Census Property` | Link → **Hotel Property Census** (`tbl9aY5ijiuIzzWam`) |

Publish with census link (registry or override):

```bash
node scripts/publish-ai-demand-positioning-snapshot.mjs \
  --property adp_waterstone_boca_raton \
  --apply --airtable \
  --census-id recXXXXXXXXXXXXXX
```

Registry (default): `fixtures/ai-demand-positioning/census-links-v1.json` — resolved by `lib/ai-demand-positioning/census-link-registry.js`.

## Table: `AI Demand Positioning - Published Reports`

| Field | Type | Notes |
|-------|------|-------|
| Report Name | singleLineText | Primary — `{Property Name} — {date}` |
| ADP Property ID | singleLineText | Internal property id |
| Period ID | singleLineText | Monitoring period id |
| Property Name | singleLineText | List view |
| City | singleLineText | |
| State | singleLineText | |
| Market | singleLineText | Dealality market label |
| Execution Date | dateTime | Period execution timestamp |
| Published At | dateTime | Snapshot publish timestamp |
| Publish Status | singleSelect | Draft, Validated, **Live**, Archived |
| Product Version | singleLineText | e.g. `ai_demand_positioning_v1` |
| Demand Capture Rate | number | Headline % for filtering |
| Provider Count | number | |
| Payload JSON | multilineText | **Pre-computed owner UI payload** (all Executive Summary sections) |
| Evidence Index JSON | multilineText | Compact index or `{ storageOnly: true, evidenceStoreRef }` stub |
| Payload Store Ref | singleLineText | Disk path backup, e.g. `published/adp_waterstone/...` |
| Census Record ID | singleLineText | Nullable until census linked |
| Linked Census Property | multipleRecordLinks → Hotel Property Census | Applied when census record exists |

## Field map

Central mapping: `lib/ai-demand-positioning/airtable-field-map.js` (`map_adp_published_report`)

## Scripts

| Command | Purpose |
|---------|---------|
| `npm run adp:publish-snapshot -- --property=<id> --dry-run` | Preview publish bundle |
| `npm run adp:publish-snapshot -- --property=<id> --apply` | Write to `data/.../published/` |
| `npm run adp:publish-snapshot -- --property=<id> --apply --seed` | Write committed deploy seed |
| `npm run adp:seed-published -- --apply` | Seed all 3 pilot properties |
| `ADP_AIRTABLE_PUBLISH_APPLY=true npm run adp:publish-snapshot -- --property=<id> --apply --airtable` | Upsert Live Airtable row |
| `npm run ensure:ai-demand-positioning-schema` | Schema dry-run |
| `npm run test:ai-demand-positioning-published-foundation` | Read-path gate |

## Env flags

| Variable | Purpose |
|----------|---------|
| `ADP_AIRTABLE_BASE_ID` | Dedicated ADP base (from `adp:create-base --apply`) |
| `ADP_AIRTABLE_READ_LIVE=1` | Production API reads Live payload from Airtable |
| `ADP_AIRTABLE_PUBLISH_APPLY=true` | Allow Airtable upsert on publish |
| `ADP_BASE_CREATE_APPLY=true` | Allow dedicated base creation |
| `ADP_SCHEMA_APPLY=true` | Allow schema ensure apply |

## Data contract snapshot

- **Tables:** `AI Demand Positioning - Published Reports`; link target `Hotel Property Census`
- **Required on publish:** ADP Property ID, Period ID, Property Name, Payload JSON (ok:true)
- **Optional:** Census Record ID, Linked Census Property
- **Select fields:** Publish Status ∈ {Draft, Validated, Live, Archived}
- **UI output:** Same shape as `buildOwnerPayload()` — no raw prompts/responses in customer API
