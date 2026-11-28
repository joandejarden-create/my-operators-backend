# HE Surfe Contact Enrichment Parity — Meinrad Lang III

**Date:** 2026-11-28  
**Hotel:** Bethesda Marriott (`recLuxvwwxID7U2B8`)  
**Person:** Meinrad Lang III · Marriott International · General Manager  
**Verified LinkedIn:** `https://www.linkedin.com/in/meinradlang`

## 1. Exact reason the original Surfe lookup failed

Two compounding defects:

1. **Wrong LinkedIn request field casing.** Dealality sent `linkedInUrl` on POST `/v2/people/enrich` people objects. Surfe’s documented people identifier is **`linkedinUrl`**. The verified LinkedIn URL was effectively dropped, so matching fell back to weaker name/company inputs (and `lastName` previously included generational suffix `III`).
2. **Phone cascade not requested + landlines ignored.** HE reveal gated `include.mobile` on `SURFE_INCLUDE_MOBILE=true` (default false). Meinrad’s phone is returned under **`mobilePhones`**, not free `landlines`. Separately, `normalizeSurfePerson` / `mapEnrichedPerson` ignored `landlines` and top-level `email`, so partial free results could also be dropped.

## 2. Correct LinkedIn URL?

**Yes.** Canonical verified URL used: `https://www.linkedin.com/in/meinradlang` (normalized, trailing slash stripped). Name split for API context: firstName `Meinrad`, lastName `Lang`.

## 3. Live Surfe API result (credit-guarded)

| Run | include.mobile | Status | Email | Phone | Credits |
|-----|----------------|--------|-------|-------|---------|
| 1 | false | COMPLETED | yes (`m***g@marriott.com`) | no | email **1**, mobile 0 |
| 2 | true | COMPLETED | yes | yes (`********2451`, mobilePhones) | email **1**, mobile **1** |

Pending was not observed. No Airtable writes. Screenshot values were **not** hardcoded.

## 4. API vs Chrome extension

Extension and API can both return email + phone for this identity when the API request uses the correct `linkedinUrl` and `include.mobile=true`. Extension UX may surface contacts with different credit accounting; API landlines remain free when present. For Meinrad, the phone was a **mobile** cascade result, not a free landline.

## 5. Email and phone display when returned

Session UI paints email and phone with **Surfe · Session Only**, uses `telHrefForPhone` for dialing, allows partial success, and does not replace canonical Airtable channels.

## 6. No Airtable persistence

Confirm: reveal path returns `persistence: "NOT_WRITTEN"`; `persist: true` is rejected; Surfe PII stays session-only.

## 7. Credits consumed (this diagnostic)

- Email: **2** total across two guarded runs (939→938 remaining after second)  
- Mobile: **1**  
- Starting balance observed: totalEmail 940, totalMobile 192

## 8. Fixes shipped

- `lib/surfe/client.js` — `linkedinUrl` builder, name suffix split, landlines + top-level email normalize, `pickSurfePhoneNumber`
- `lib/market-alerts-contact/surfe-adapter.js` — correct people input; map email/phone/landline
- `lib/group-demand-intelligence/gdi-surfe-contact-provider-v1.js` — same identity + phone fixes
- `api/contact-intelligence.js` — pending ≠ NOT_FOUND; HE explicit both/phone requests mobile; session attribution
- `public/js/hotel-contact-intelligence.js` — pending UX; Surfe · Session Only; tel formatter
- `scripts/test-he-surfe-contact-parity-v1.mjs` — mocked regression gates
- `scripts/he-surfe-meinrad-live-diagnostic-v1.mjs` — credit-guarded live diagnostic

## 9. Playwright / E2E

- Static UI gates: PASS (`UI_CERT.json`)
- Live provider: PASS (diagnostic above; email + mobile)
- Authenticated Contacts DOM: use `deal-capture-radar-with-ranked-list.html?hotelExplorer=…&hexTab=contacts` (Memberstack shell `#/app` alone is empty without HE open). Visibility cert script reuses that path; Surfe click not auto-fired (credit guard).

## 10. Permanent regression

`npm run test:he-surfe-contact-parity-v1` — PASS (mocked).
