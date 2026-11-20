# Guest Sentiment — Provider Access & Licensing Assessment (v1)

**Date:** 2026-11-19  
**Stream:** Hotel Explorer  
**Apply / paid subscribe / deploy:** **NOT AUTHORIZED** in this cycle  
**Persistence classification:** `PERSISTENCE_DESIGNED` (Airtable dry-run only)

## 1. Current provider-access and licensing

| Source | Dealality status today | Review / reputation product? | HE live display | Notes |
|---|---|---|---|---|
| Google Places / Reviews | Places used for geo/phone validation; storage gated | Rating fields possible under Places ToS | **No** | Requires Places storage/display rights review. No Place ID in Bethesda staged packet. |
| Tripadvisor | Apify Actor used for **rooms / hotel profile** enrichment (census path), not HE UI | Actor can return rating + categoryReviewScores | **No** (rights + paid) | Consumer scrape blocked. Apify is paid + ToS-bound; not licensed for HE reputation UI without founder approval. |
| Booking.com | Demand API **interface stub** in research-engine registry | Content/geo licensed path possible | **No** | Consumer scrape hard-blocked. Partner credentials + ToS required. |
| Expedia Rapid | Content API **interface stub** | Content/geo licensed path possible | **No** | Same as Booking — license before storage/display. |
| Multi-platform reputation vendors (ReviewPro, Revinate, TrustYou, etc.) | **Not contracted** in-repo | Would be preferred licensed feed | **No** | No existing Dealality contract artifact found for HE. |

**Production integrations vs proposed**

- **Available (reuse):** HPC identity; Tripadvisor Apify profile schema inventory; Expedia/Booking source registry stubs; ADP “review/reputation action” patterns (not HE ratings); Market Alerts Brand/Operator news pattern (UI hide-when-empty).
- **Proposed / unavailable for live HE:** Licensed multi-OTA reputation feed; Google Reviews product; any scrape of review bodies.

## 2. Reusable Dealality capabilities

- Canonical HPC hotel identity (`recLuxvwwxID7U2B8` Bethesda Marriott).
- Brand / Operator subsection host pattern (Hotel News).
- External source policy: licensed APIs vs forbidden consumer scrape (`lib/research-engine-v2/external-hotel-source-registry.js`).
- Tripadvisor profile intelligence docs/schema (rating, categoryReviewScores) — **enrichment layer**, not HE UI authorization.
- Guest theme taxonomy + observation model (this delivery).

## 3. Recommended initial data providers (founder decision)

1. **Short-term (pilot):** Rights-gated **public listing metadata** only (native score + count + URL + published category scores) — current Bethesda staged packet.  
2. **Preferred scale path:** One licensed multi-platform reputation vendor (single contract, theme feeds, refresh SLA) **or** platform partner APIs (Expedia Rapid + Booking Demand) after ToS/display review.  
3. **Do not:** Unattended consumer scrapes; Apify-as-production-reputation without legal + budget approval.

## 4. Estimated cost per hotel / monthly refresh (order-of-magnitude)

| Approach | Est. cost / hotel / month | Refresh | Risk |
|---|---|---|---|
| Public metadata research (manual / SERP) | ~$0 API; analyst time | Ad hoc | Rights display risk; incomplete categories |
| Tripadvisor via Apify (detailed pull) | ~$0.02–$0.15 / pull (Actor + compute; varies) | Monthly ≈ same | ToS + not a stable enterprise feed |
| Google Places Details (rating fields) | ~$0.017–$0.05 / request (SKU-dependent) | Monthly | Storage/display ToS |
| Expedia Rapid / Booking Demand | Partner / negotiated (often bundled; not $ / hotel list price) | Daily–weekly possible | Contract lead time |
| Enterprise reputation suite | Often **$1–$5+ / hotel / month** at portfolio scale (vendor quote required) | Daily–weekly | Best fit for themes + competitive sets |

*Figures are planning estimates only — not quotes. No paid calls were made in this cycle.*

## 5. Bethesda pilot coverage

| Platform | Property id | Native rating | Reviews | Categories | Live UI |
|---|---|---|---|---|---|
| Tripadvisor | `d83967` | 4.1 / 5 | 366 | Cleanliness 4.5; Sleep 4.3 (partial) | Proposed preview only |
| Booking.com | `bethesda-marriott` | 8.0 / 10 | 234 | Staff, Facilities, Cleanliness, Comfort, Value, Location | Proposed preview only |
| Expedia | `h23292` | 8.6 / 10 | ~1001 | Not captured | Proposed preview only |
| Google | — | — | — | — | Unavailable |

**Sampling limitations:** Single observation date (2026-11-19); no 30/90/180/365 trends; no review-body theme mining; Google unresolved; TA category set incomplete.

## 6. Proposed refresh cadence & quality gates

- **Pilot:** Monthly manual or licensed refresh after founder rights decision.  
- **Portfolio scale:** Weekly platform snapshots once licensed; retain history rows (never overwrite).  
- **Gates:** HE-QL-022 + `npm run test:he-guest-sentiment-brand-operator-v1` + product quality gates.

## 7. Scaling recommendation

1. Approve Airtable observation tables (dry-run packet).  
2. Choose licensed feed path before enabling `live_ui_authorized`.  
3. Expand packet store hotel-by-hotel with verified platform property IDs.  
4. Only then enable competitive-set / renovation-delta analytics (Phase 7) on multi-snapshot history.
