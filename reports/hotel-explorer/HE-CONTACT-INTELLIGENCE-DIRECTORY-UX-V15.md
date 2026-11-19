# HE Contact Intelligence Directory UX + Surfe (HE-QL-027 / V15)

**Date:** 2026-11-19  
**Stream:** Hotel Explorer (`ws-hotel-explorer` / `ws/hotel-explorer`)  
**PR:** #463 (open — Integration Controller owns merge)  
**Standard:** `hotel-explorer-quality-standard-v1.16` · Lessons `v1.16` · **HE-QL-027**

## Founder success criterion

Within seconds on Contact Intelligence a user can:

1. Identify the most appropriate person (Katherine Button for Bethesda)
2. Understand why they matter (one-line relevance)
3. Open LinkedIn / Company Profile
4. See verified person-specific email/phone when present
5. Click **Find Email & Phone** for Surfe (mock for no-cost QA; live when configured)

## Root cause — prior “Surfe unavailable”

1. Live Surfe requires `CONTACT_ENRICHMENT_ENABLED=true` **and** `SURFE_API_KEY`.
2. Founder Preview `fetch` rewrite mapped **all** `/api/contact-intelligence/hotels/:id…` URLs (including **POST `/people/:id/reveal`**) to the GET founder-preview contacts route → reveal returned “API route not found”.

## Fixes

| Area | Change |
| --- | --- |
| UI | Compact directory: Recommended First Contact → Alternative Decision-Makers → Organization Contact Channels → collapsed Research gaps |
| Actions | LinkedIn, Company Profile, email/phone, **Find Email & Phone** on each eligible person |
| Surfe | Authorized reveal only; mock/dry-run path (`CONTACT_ENRICHMENT_MOCK` / `CONTACT_ENRICHMENT_DRY_RUN_PROVIDER`); auto-mock when live Surfe not configured (opt out `MOCK=false`) |
| Founder Preview | Rewrite only package **GET**; never `/reveal`, `/people/`, or enrichment-capability |
| Spending | No Surfe on load; no bulk; inflight dedupe; cached channels skip credits; mock `surfeCalls: 0` |
| Persistence | Reveal never writes Airtable; results `PROVIDER_RETURNED_UNREVIEWED` |

## Bethesda Founder Preview

| Check | Result |
| --- | --- |
| Katherine Button suggested first | PASS — property responsibility inferred / not confirmed |
| Jeff Mullen + Patrick Reece alternatives | PASS |
| Verified LinkedIn links | PASS |
| Mullen org channels once | PASS (`info@mullencap.com`, website, HQ) |
| Find Email & Phone | PASS — mock returns email, 0 credits, unreviewed |
| Details progressive disclosure | PASS |
| No corporate email as personal | PASS |

## Screenshots

- `reports/hotel-explorer/phase4-quality/phase4-founder-preview-screenshots/quality-v15-ci-default.png`
- `reports/hotel-explorer/phase4-quality/phase4-founder-preview-screenshots/quality-v15-ci-expanded.png`

## Tests

```bash
npm run test:he-contact-intelligence-directory-ux-v1
npm run test:he-product-quality-gates-v1
npm run test:he-async-section-loading-v1
```

## Safety

No production deploy · no unauthorized Airtable apply · no automatic paid enrichment · dirty unrelated work preserved.
