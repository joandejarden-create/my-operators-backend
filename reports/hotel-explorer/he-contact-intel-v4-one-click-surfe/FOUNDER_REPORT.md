# CONTACT INTELLIGENCE V4 — Founder Report

**Classification:** `HE_CONTACT_INTEL_V4_CERTIFIED_MOCKED_LIVE_BLOCKED`  
**Date:** 2026-11-28  
**Hotel:** Bethesda Marriott (`recLuxvwwxID7U2B8`)

## Root cause of the two-step workflow

Hotel Explorer Contact Details bound **Find Email & Phone** to `POST …/enrich-hybrid` (Findymail-first). When Findymail returned no durable hit, the UI painted a second **Try Surfe** control that called `/reveal`. That produced a customer-facing two-click path and confused Surfe session results with research-pipeline Findymail behavior.

## Exact changes removing Findymail from the customer click

| Area | Change |
|---|---|
| `public/js/hotel-contact-intelligence.js` | Customer click → `POST /api/contact-intelligence/hotels/:hotelId/people/:personId/reveal` only; `allowPaidFindymail: false`; `persist: false` |
| Markup | `data-hci-one-click-surfe="1"`; removed `data-hci-hybrid-enrich` from the customer button |
| `paintSurfeFallbackOffer` | Stubbed no-op — no Try Surfe UI |
| Research pipeline | `enrich-hybrid` / Findymail left intact for authorized internal research (not wired to the customer button) |

## Actual Surfe endpoint invoked

`POST /api/contact-intelligence/hotels/{hotelId}/people/{personId}/reveal`

Server resolve path (existing authenticated Surfe reveal service):

1. Canonical Airtable missing-channel recheck  
2. `surfeIncludeFromMissingState` → `include.email` / `include.mobile`  
3. Surfe V2 people enrich with `linkedinUrl`  
4. Session-only response (`persistence: NOT_WRITTEN`)

## One-click provider request/response evidence

Mocked Playwright cert (`BROWSER_RESULT.json`):

- One reveal POST after one click  
- Zero `enrich-hybrid` / Findymail customer requests  
- Session UI: `mock.v4@example.hotel` + `+1 (301) 555-0999` + `Surfe · Session Only`  
- Screenshots: `SCREENSHOTS/01_toast_upper_right.png`, `SCREENSHOTS/02_after_one_click.png`

## Five-person Bethesda request matrix

Fresh Airtable-backed package read (`BETHESDA_REQUEST_MATRIX.json`):

| Contact | Surfe request |
|---|---|
| Jeff Mullen | Phone only |
| Katherine Button | None |
| Patrick Reece | None |
| Meinrad Lang III | Email + phone |
| Jenny Pyne | Email + phone |

## Session-only display

- Results stored only in `_sessionSurfeByPerson`  
- Button durable completeness remains Airtable-gated (`data-dimmed-complete`)  
- Label: **Surfe · Session Only**  
- `persist: false` on the customer request body  

## Upper-right toast

Toast host `#hci-enrich-loading-toast` — fixed `top: 20px; right: 20px` on `document.body`.  
Runtime: visible, upper-right, outside table, text `Finding Contact Details…` / `Surfe · Session Only`.

## Corrected table layout

- Contact link CSS uses `overflow-wrap: break-word` (not `anywhere`)  
- Contact Details `min-width: 300px`; horizontal scroll preferred over char-wrap  
- Mocked Meinrad row height stayed within content-driven bounds across five viewports  

## Five-viewport Playwright

See `CERTIFICATION.json` → `CONTACT_TABLE_LAYOUT_STABLE` and `SCREENSHOTS/vp-*.png`  
(1920×1080, 1440×900, 1366×768, 768×1024, 390×844)

## No Surfe Airtable persistence

Customer path: `persist: false`. Session map is in-memory only. Gate `CONTACT_SURFE_NO_AIRTABLE_WRITE` PASS.

## Surfe credits consumed

**0** during this V4 certification (mocked reveal).  
Live browser Surfe click: **BLOCKED** (no `--live-surfe --confirm-live-spend`).

## Permanent gates added

`npm run test:he-contact-intel-v4-one-click-surfe` (+ `--browser`)

- CONTACT_ONE_CLICK_SURFE_ONLY  
- CONTACT_NO_FINDYMAIL_ON_CUSTOMER_CLICK  
- CONTACT_DYNAMIC_SURFE_CHANNEL_SELECTION  
- CONTACT_SURFE_SESSION_RESULT_VISIBLE  
- CONTACT_SURFE_NO_AIRTABLE_WRITE  
- CONTACT_SURFE_NO_SECOND_CLICK  
- CONTACT_NO_DUPLICATE_PROVIDER_CREDITS  
- CONTACT_DETAILS_NO_VERTICAL_CHARACTER_WRAP  
- CONTACT_ENRICHMENT_TOP_RIGHT_TOAST  
- CONTACT_TABLE_LAYOUT_STABLE  

GDI shared reveal client retained (`GDI_SHARED_REVEAL_PARITY`).

## PR / merge / 8080

| Field | Value |
|---|---|
| PR | [#709](https://github.com/joandejarden-create/my-operators-backend/pull/709) |
| Status | `OPEN` — `AWAITING_INTEGRATION_CONTROLLER` |
| Head SHA | `ec65592d6` |
| Base | `origin/main` `@ 2429e4ff7` |
| Merge SHA | pending IC merge |
| Live Surfe credits | **0** (mocked); live click **BLOCKED** |
| Pre-merge 8080 | `ws-gdi` PID overlay of V4 JS/CSS for local cert — **not** merge tip until IC merges + sync |

Post-merge required: IC merge → `dealality:post-merge-sync --stream he --pr 709` → verify 8080 serving SHA equals merge tip.
