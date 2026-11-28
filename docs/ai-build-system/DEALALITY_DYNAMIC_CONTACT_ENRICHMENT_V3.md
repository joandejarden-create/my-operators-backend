# DEALALITY_DYNAMIC_CONTACT_ENRICHMENT_V3

Shared Airtable-first missing-channel contact enrichment for Hotel Explorer and GDI.

## Modules

- Planner: `lib/shared-contact-intelligence/missing-channel-enrichment-v3.js`
- HE adapter: `lib/hotel-intelligence/contact-intelligence/he-contact-missing-channel-v3.js`
- GDI Surfe (explicit-click): `lib/group-demand-intelligence/gdi-surfe-contact-provider-v1.js`
- GDI Stage 4 Findymail: `lib/group-demand-intelligence/workflow/gdi-findymail-stage4-enrichment-v1.js`
- Shared phone display: `DEALALITY_SHARED_INTERNATIONAL_PHONE_FORMATTING_V1`

## Provider contracts (official)

| Provider | Docs | Notes |
|---|---|---|
| Surfe | https://developers.surfe.com/public-015-create-people-bulk-enrichment | Person field `linkedinUrl`; include `email` / `mobile` / `linkedInUrl`; async poll GET `/v2/people/enrich/:id`; landlines free |
| Findymail | https://app.findymail.com/docs/ | Email `POST /search/name` (1 credit when found); Phone `POST /search/phone` with LinkedIn (10 credits when found) |

Chrome extension coverage ≠ API guarantees. Do not hardcode extension screenshot values.

## Meinrad root cause (Bethesda · recLuxvwwxID7U2B8)

1. Wrong LinkedIn request key historically (`linkedInUrl` vs `linkedinUrl`).
2. `include.mobile` not requested → phones only in `mobilePhones` missed.
3. Pending jobs treated as no-result before poll completed.
4. When LinkedIn + mobile requested, API matched extension coverage (live diagnostic; Surfe session-only; not Airtable-written).

## Missing-channel matrix

| Email | Phone | Findymail | Surfe (explicit) |
|---|---|---|---|
| Yes | Yes | none | none |
| Yes | No | phone via LinkedIn if present | mobile only |
| No | Yes | email from name+domain | email only |
| No | No | email then phone (independent) | both |

Surfe is **never** auto-run from Stage 4 / PowerShell.

## Hard cap

Shared `findymailCreditBudget.used` / `hardCap` across primary + alternative contacts; phone steps require remaining ≥ 10 before spend; fail closed.

## Tests

```bash
npm run test:shared-missing-channel-enrichment-global-v3
npm run test:he-contact-dynamic-enrich-v3
npm run test:gdi-stage4-dynamic-contact-enrichment-v3
```
