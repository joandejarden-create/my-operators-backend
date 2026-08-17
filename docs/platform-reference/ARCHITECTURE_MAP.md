# Architecture Map

> **Placeholder** — to be expanded as Dealality architecture is formally documented.

## Purpose

This file will map Dealality's major systems: Webflow front-end, Node proxy (`server.js`), API routes (`api/`), static UI (`public/`), Airtable bases, Memberstack auth, Railway deployment, and key data flows.

## What to Add Later

- System diagram (front-end → proxy → Airtable / Memberstack)
- Base topology: main product base vs GTM/alt base
- Major product surfaces: Scout, Brand Explorer, Operator Explorer, snapshots, intake flows
- Auth and access layers (current + planned workspace/region/deal access)
- Integration points: Zapier, GitHub, Railway
- Read vs write paths for critical tables

## Existing References

| Area | Location |
|------|----------|
| Repo layout | [../../AGENTS.md](../../AGENTS.md) |
| API routes | `api/*.js`, `server.js` |
| Operator Setup | `public/third-party-operator-setup-new-two.html` |
| Brand Explorer | `public/js/brand-explorer*.js`, `api/brand-library.js` |
| Operator Explorer | `public/js/operator-explorer*.js` |
| GTM / pilot | `api/target-list.js`, `docs/gtm-owner-target-list.md` |
| Radar / CALA | `lib/radar-buildout/`, `lib/travel-infrastructure/` |
| Automations | `airtable/automations/` |

## Related

- [DATA_DICTIONARY.md](./DATA_DICTIONARY.md)
- [TESTING_PROTOCOL.md](./TESTING_PROTOCOL.md)
