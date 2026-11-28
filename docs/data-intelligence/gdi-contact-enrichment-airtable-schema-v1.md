# GDI Contact Enrichment Airtable Schema V1

**Status:** `SCHEMA_CREATED` (2026-11-23) — Findymail API still inactive; Surfe durable email storage still forbidden.  
**Contract:** `gdi-contact-enrichment-airtable-schema-v1`

## Destinations

| Surface | Base | Table | ID |
|---------|------|-------|----|
| Usage ledger | AI Demand Positioning / GDI ops `appa2cE7FTRmIbB32` | **GDI Contact Enrichment Usage Events** | `tblvUJlik63pETitY` |
| Durable channels | Hotel Intelligence `appCCUsuGsE1ifoLk` | **HI Contact Channels** | `tblqC7C4ypy5Zojyw` |

Do **not** create a Findymail Contacts table or a Surfe-only usage table.

## Usage ledger privacy

Store: Lookup Event ID, Provider, Lookup Outcome, credits/cost, opaque Person Identity Hash, hotel/opportunity IDs, origin, timestamps, env, schema version, provider request reference, notes.

Never store: names, emails, phones, raw provider payloads, API keys, tokens.

## HI Contact Channels additive fields

| Field | ID |
|-------|----|
| Last Checked At | `fldyUP3ylAUP8uPUi` |
| License / Retention Class | `fldkJEwy47xROXmys` |
| Retention Expires At | `fldeIWS3vz8s4NyrE` |
| Provider Request ID | `fld4xwUDV3AQ2MBa8` |

Costs stay on the usage ledger — not on HI Contact Channels.

## Ensure / test

```bash
node scripts/ensure-gdi-contact-enrichment-airtable-schema-v1.mjs
node scripts/ensure-gdi-contact-enrichment-airtable-schema-v1.mjs --apply --confirm-founder-schema-go
npm run test:gdi-contact-enrichment-airtable-schema-v1
npm run test:gdi-contact-enrichment-airtable-schema-v1 -- --live
```

Env (optional): `GDI_CONTACT_ENRICHMENT_USAGE_AIRTABLE_TABLE_ID=tblvUJlik63pETitY`

## Activation still required

- Findymail subscription / API
- Surfe API (existing click path unchanged; no durable Surfe email/phone)
- Usage-event sync writers (dry-run default)
- HI channel enrichment writes
- Customer email display / export
