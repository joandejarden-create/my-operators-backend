# Contacts and evidence

Status: **PENDING_LIVE_RESEARCH**

## Persistence model

Contacts remain embedded on:

- `GDI Hotel Research Opportunities.researchPayloadJson` (+ typed summary fields where present)
- Qualified GDO `primaryContact*` columns when separately authorized

## Required contact fields when verified

- Name, organization, role/title
- Evidence / source
- Commercial relevance
- Buyer vs entry-point classification
- Associated demand activity / opportunityId

## Explicit non-goals

- No automatic Surfe enrichment
- No parallel Contacts table in this benchmark
- Avoid duplicating the same person across hotel opportunities when composite identities collide
