# Brand-Decision Contact Enrichment Queue

Generated: 2026-07-04T21:38:29.484Z

Working list for **named-email / LinkedIn / verified phone** research on brand-decision-eligible owners.

- Country filter: **Dominican Republic**
- Min intent score: **25**
- Needs enrichment only: **true**
- Total in queue: **50**
- Outreach-ready (skip enrichment): **0**
- Needs contact work: **50**

## Priority breakdown

- **P2_tier_a:** 4
- **P3_high_intent:** 2
- **P4_medium_intent:** 10
- **P5_backlog:** 34

## P1 — Tier A high intent (enrich first)


## P2 — Tier A (enrich next)

- **Servicios Corporativos Piñero S.L.** (66) — missing_contact_name|missing_contact_channel|needs_verification_tier → Manual corp web research + registry optional
- **Impressive Resorts & Spas** (58) — missing_contact_name|missing_contact_channel|needs_verification_tier → Manual corp web research + registry optional
- **Zemi Hotels & Resorts, S.R.L.** (58) — missing_contact_name|missing_contact_channel|needs_verification_tier → Manual corp web research + registry optional
- **Riu Hotels & Resorts - RIU Hotel Bambu** (48) — missing_contact_name|missing_contact_channel|needs_verification_tier → Review mx seed riu-hotels; complete missing fields

## Outreach-ready (P0 — send now)


## Commands

```bash
node scripts/report-gtm-brand-decision-enrichment-queue.mjs --country=Mexico
node scripts/report-gtm-brand-decision-enrichment-queue.mjs --country=Mexico --needs-enrichment-only
node scripts/report-gtm-brand-decision-enrichment-queue.mjs --country=Mexico --limit=100
```