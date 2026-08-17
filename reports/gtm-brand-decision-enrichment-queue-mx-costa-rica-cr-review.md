# Brand-Decision Contact Enrichment Queue

Generated: 2026-07-04T22:09:19.262Z

Working list for **named-email / LinkedIn / verified phone** research on brand-decision-eligible owners.

- Country filter: **Costa Rica**
- Min intent score: **25**
- Needs enrichment only: **true**
- Total in queue: **85**
- Outreach-ready (skip enrichment): **0**
- Needs contact work: **85**

## Priority breakdown

- **P1_high_intent_tier_a:** 1
- **P2_tier_a:** 2
- **P3_high_intent:** 5
- **P4_medium_intent:** 21
- **P5_backlog:** 56

## P1 — Tier A high intent (enrich first)

- **Owner 1: Mohari Hospitality Limited | Owner 2: Gencom** (70) — missing_contact_name|missing_contact_channel|needs_verification_tier → Review mx seed mohari-gencom; complete missing fields

## P2 — Tier A (enrich next)

- **Böëna Lodges** (58) — missing_contact_name|missing_contact_channel|needs_verification_tier → Manual corp web research + registry optional
- **Alojica** (52) — missing_contact_name|missing_contact_channel|needs_verification_tier → Manual corp web research + registry optional

## Outreach-ready (P0 — send now)


## Commands

```bash
node scripts/report-gtm-brand-decision-enrichment-queue.mjs --country=Mexico
node scripts/report-gtm-brand-decision-enrichment-queue.mjs --country=Mexico --needs-enrichment-only
node scripts/report-gtm-brand-decision-enrichment-queue.mjs --country=Mexico --limit=100
```