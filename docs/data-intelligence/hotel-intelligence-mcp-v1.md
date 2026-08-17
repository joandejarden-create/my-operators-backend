# Dealality Hotel Intelligence MCP V1

**Status:** `dealality_hotel_intelligence_mcp_v1_ready`  
**Purpose:** Persistent hotel identity + evidence access layer for Dealality (Census, Deal Setup, Brand/Operator Match, market intelligence foundations).

Hotelbeds / public sources are **providers**, not the system of record. Production SoT remains **Hotel Property Census** (`tbl9aY5ijiuIzzWam`) on Deal Capture Platform.

## Architecture

```text
Dealality Features / AI / Cursor
        → Hotel Intelligence MCP (stdio)
        → Canonical hotel layer (dhl_ IDs, evidence, confidence)
        → Providers: dealality_census | hotelbeds | serpapi | giata_drive | (stayingapi gated)
        → Identity resolution → review queue / staged ingest
        → Hotel Property Census (read; writes gated off by default)
```

| Layer | Path |
| --- | --- |
| Library | `lib/hotel-intelligence/` |
| MCP server | `mcp/hotel-intelligence/server.js` |
| Field map | `lib/hotel-intelligence/map_hotel_intelligence_fields.js` |
| Local staging | `data/hotel-intelligence/` (gitignored) or `HOTEL_INTELLIGENCE_DATA_DIR` |

## Canonical hotel ID

- MCP `hotel_id` = opaque `dhl_<ulid>`
- Mapped locally to Airtable `rec…` + `Property Identity Key` + external IDs
- Never use Hotelbeds / Google / GIATA / Airtable record ID as the canonical Dealality Hotel ID
- Optional Airtable field for `dhl_` is **out of V1** (schema approval required)

## Tools (V1)

| Tool | Role |
| --- | --- |
| `hotel_search` | Census (+ optional Hotelbeds / targeted GIATA) candidates |
| `hotel_get` | Canonical record + evidence summary |
| `hotel_resolve` | Identity match vs census |
| `hotel_enrich` | Provider enrich → local evidence (no blind census write) |
| `hotel_nearby` | Radius query (Haversine, in-memory) |
| `hotel_sources` | Field-level evidence + conflicts |
| `hotel_review_queue` | Ambiguity / conflict queue |
| `hotel_census_ingest` | Batch normalize→resolve→stage |
| `hotel_intelligence_meta` | Availability + future contracts |

### Future tool contracts (stubs only)

`market_hotels`, `market_supply_summary`, `brand_market_presence`, `brand_whitespace`, `brand_saturation`, `competitive_set_generate`, `owner_get`, `owner_portfolio`, `operator_get`, `operator_portfolio`, `hotel_history`, `hotel_reflags`, `pipeline_search`, `opportunity_search`

## Providers

### Add a provider

1. Implement `HotelDataProvider` in `lib/hotel-intelligence/providers/` (`searchHotels`, `getHotel`, `normalizeHotel`, `getAvailabilityStatus`).
2. Register in `providers/registry.js`.
3. Normalize into the shared candidate shape (`providers/types.js`).
4. Never leak credentials; return `provider_status` separately from hotel payloads.

### Hotelbeds (HBX)

- Adapter: `providers/hotelbeds.js` over `lib/research-engine-v2/hbx-content-api-client.js` + rate limiter.
- Env: `HBX_API_KEY`, `HBX_API_SECRET`, `HBX_ENV` (default `test`).
- Enable live calls: `ENABLE_HBX_CONTENT_API=1` or `HOTEL_INTELLIGENCE_HOTELBEDS=1`.
- Quota (`TEST_DAILY_QUOTA_EXHAUSTED` / HTTP 403 `"Quota exceeded"`) → `provider_status.status = quota_exhausted`, `retryable: true`. Does **not** crash the MCP.

### GIATA Drive (Open Content Link) — complementary provider

**Role:** `COMPLEMENTARY_IDENTITY + GEO/BRAND ENRICHMENT`

| Use | Status |
| --- | --- |
| Stable `giataId` external ID | Yes — `external_ids.giata_drive` only; never replaces `dhl_` |
| Name / city / country / address / postal / coordinates | Yes |
| Brand/chain, website, phone, star rating, descriptions, amenities, images | Yes |
| Incremental feed (`urls` / `deletedUrls` / `latestRevision`) | Yes — local sync state only |
| Primary CALA universe discovery | **No** (Open Content ~4.6k global; Cvent remains large-scale) |
| Total property room count | **No** — `roomTypes[]` is firewalled from `room_count` (`SUPPORTED_BUT_NOT_ENTITLED`) |
| MultiCodes / Hotelbeds / Booking / Expedia crosswalk | **No** — not entitled on Drive |

- Adapter: `providers/giata-drive.js` over `lib/research-engine-v2/providers/giata-drive/`
- Enable: `HOTEL_INTELLIGENCE_GIATA_DRIVE=1` (default off)
- Auth: `GIATA_DRIVE_API_KEY` (Bearer). Username/password unused for Drive.
- Discovery: targeted zero / near-zero geos or explicit `giata_discovery` + `country_code` only — stage `NEW_CANDIDATE`, never production import.
- `deletedUrls` → local event `GIATA_OPEN_CONTENT_REMOVED` (does **not** mean hotel closed).
- Tests: `npm run test:hotel-intelligence-giata-drive`
- Validation: `npm run hotel-intelligence:giata-drive-adapter-validation`

Enrichment waterfall (field-targeted; skip SerpApi when GIATA already strong unless validation requested):

```text
identity / geo / address / phone:  census → giata_drive → serpapi → hotelbeds
brand:  census / Brand Explorer → giata_drive
room_count:  NEVER giata_drive
```

## Evidence

Local JSON (`field-evidence.json`):

```json
{
  "hotel_id": "dhl_…",
  "field": "room_count",
  "value": 184,
  "source": "hotelbeds",
  "source_record_id": "123456",
  "observed_at": "2026-08-10",
  "confidence": 0.82
}
```

Agreeing and conflicting observations are both retained. Canonical preference uses `preferCanonicalValue()`.

## Confidence

Documented in `lib/hotel-intelligence/confidence.js` (`SOURCE_FIELD_AUTHORITY`):

| Score | Tier |
| --- | --- |
| 0.95–1.00 | Verified |
| 0.85–0.94 | High |
| 0.70–0.84 | Probable |
| 0.50–0.69 | Needs review |
| &lt;0.50 | Do not auto-accept |

Authority is **field-specific** (e.g. rooms: official_site &gt; brand_directory &gt; hotelbeds; **giata_drive room_count = 0**).

## Identity resolution

Tiers in `identity-resolve.js`:

- **Exact** — trusted external ID, or normalized name + same address
- **Strong** — high name similarity + city + tight geo / phone / website
- **Probable** — similar name + market/city + nearby/brand signals
- **Ambiguous** — multiple near-equal candidates → review; never auto-merge
- **New** — staged `dhl_` id

Reuses `external-hotel-match-engine` + independent-census geo/name helpers.

## Review queue

Issue types: `possible_duplicate`, `identity_ambiguous`, `room_count_conflict`, `brand_conflict`, `location_conflict`, `missing_room_count`, `missing_coordinates`, `provider_mismatch`.

## Batch processing

`hotel_census_ingest` creates a local batch job (`pending` → `running` → `enriched` / `review_required` / `failed` / `completed`). Provider quota → `paused_quota` without destroying completed rows.

## Safety

| Control | Default |
| --- | --- |
| `ENABLE_HOTEL_INTELLIGENCE_AIRTABLE_WRITES` | `0` |
| Brand Explorer / Brand Setup writes | forbidden |
| Secrets in logs | never (HBX / GIATA client redaction) |
| Discovery ≡ production write | **no** |

## Commands

```bash
npm run hotel-intelligence:mcp
npm run test:hotel-intelligence
npm run test:hotel-intelligence-giata-drive
npm run hotel-intelligence:demo
```

Cursor MCP config example (stdio):

```json
{
  "mcpServers": {
    "dealality-hotel-intelligence": {
      "command": "node",
      "args": ["mcp/hotel-intelligence/server.js"],
      "cwd": "C:/Dev/deal-capture-proxy"
    }
  }
}
```

## Future expansion

This layer is the backbone for Brand Match, Operator Match, competitive sets, brand saturation/whitespace, owner/operator portfolios, pipeline, and opportunity discovery — without redesigning hotel identity.
