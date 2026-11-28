# HE Contact Dynamic Enrichment V3 — Founder Report

**Date:** 2026-11-28  
**Stacks on:** Surfe parity PR #699  
**Branch:** `fix/he-contact-dynamic-enrich-v3`

## Acceptance

Every provider call requests **only genuinely missing** channels from canonical Airtable.

| Rule | Status |
|---|---|
| Airtable-first missing-channel detection | PASS |
| Phone-only Surfe → `include.email=false, mobile=true` | PASS |
| Email-only Surfe → `include.email=true, mobile=false` | PASS |
| Both missing → email+mobile | PASS |
| Both present → no lookup / dimmed button | PASS |
| Findymail only missing endpoints | PASS (hybrid V3 wiring) |
| Surfe explicit + session-only | PASS |
| No screenshot PII hardcoded | PASS |

## Bethesda live Airtable (read-only golden)

See `BETHESDA_GOLDEN_V3.md` — states computed from live HI Contact Channels, not historical reports.

Notable:

- Katherine / Patrick → both channels on file → `requestChannel=none`
- Jeff Mullen → email on file, phone missing → Surfe/Findymail **phone-only**
- Meinrad / Jenny → both missing → email+mobile

## Fixes beyond V2

1. Shared `he-contact-missing-channel-v3.js` with `hasVerified*` / `needs*` / Surfe include mapper.
2. Surfe `/reveal` no longer short-circuits when **only one** channel exists (was a credit/skip bug).
3. Surfe include flags driven by Airtable recheck, not blanket both.
4. UI keeps **Find Email & Phone** label; title/aria uses Find Missing Phone/Email when applicable.

## Tests

- `npm run test:he-contact-dynamic-enrich-v3`
- `npm run he-contact-dynamic-enrich-bethesda-golden-v3`
- `npm run test:he-contact-intelligence-v2`
- `npm run test:he-surfe-contact-parity-v1`
