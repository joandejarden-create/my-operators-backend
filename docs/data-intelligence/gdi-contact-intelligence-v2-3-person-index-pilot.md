# GDI Contact Intelligence V2.3 — Person index & eight-contact pilot

**Status:** Preflight ready · Schema apply OFF · Live writes OFF · Customer display DENIED

## Identity

- `personIdentityKey` = grouping key only
- Merge requires independent evidence (profile/source fingerprint)
- Ambiguous same-name/org matches stay unresolved

## Airtable

Reuse GDO embedded `primaryContact*`. Proposed typed metadata fields are documented in `SCHEMA_PROPOSAL.json` and require founder+IC approval before `--apply`.

## Commands

```bash
npm run test:gdi-person-identity-collision-v1
npm run test:gdi-contact-intelligence-v2-2-durable-foundation
npm run gdi:contact-intelligence-v2-3-pilot-preflight
```
