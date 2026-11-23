# GDI Contact Intelligence V2.2 — Durable foundation

**Status:** Designed + fixture-validated · Live writes OFF · No Contacts table

## Model

`EMBEDDED_PRIMARY_CONTACT_PLUS_DURABLE_PERSON_INDEX`

- Opportunity channels stay on GDO / research inventory `primaryContact*`.
- Cross-week / multi-hotel person reuse uses `personIdentityKey` plus a durable person index.
- Until founder authorizes typed Airtable fields, the durable index is **fixture/file-only**.

## Reuse layers

| Layer | Lifetime | Use |
|---|---|---|
| Process-local cache | Single Node process | Same-run multi-hotel reuse |
| Durable fixture store | File / test harness | Cross-week simulation |
| Live Airtable | Disabled | Requires founder approvals |

## Modules

- `gdi-contact-intelligence-canonical-model-v1.js`
- `gdi-durable-contact-intelligence-store-v1.js`
- `gdi-contact-intelligence-cross-week-reuse-v1.js`
- `gdi-contact-intelligence-commercial-readiness-v1.js`

## Gates

```bash
npm run test:gdi-contact-intelligence-v2-2-durable-foundation
npm run gdi:contact-intelligence-v2-2-persistence-candidate-validation
```
