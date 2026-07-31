# Old Home Manual Process — Production Publish QA

**Recommendation: A. Production verified**

Single production publish completed. No second publish. No rollback.

## Publish

| Field | Value |
| --- | --- |
| Timestamp | `2026-07-31T18:09:46.822Z` |
| Payload | `customDomains: [69c5620ab5bfde0ecff71575, 69c56209b5bfde0ecff71549]` (= www + apex), `publishToWebflowSubdomain: true` |
| Result | `{ customDomains: [{id,url dealality.com},{id,url www.dealality.com}], publishToWebflowSubdomain: true, publishScope: "site" }` |
| Production URL | https://www.dealality.com/old-home |
| Source RC | staging `2026-07-31T17:56:47.825Z` |

## Pre-publish gates

All passed. Designer had no unpublished delta after RC. Only Old Home changed since last prod `15:18:27.790Z`. Home/CMS/scripts/domains untouched.

## Production package verified

- `#about[data-oh-problem="manual-process"]` ×1
- embed `1.1.1`, shell `v20260731a`, CSS `v20260731d`, boot `v20260731b`, HTML `v20260731b`, JS `v20260731a`
- Three approved cards; no old titles; no forbidden leverage language
- CTA → `https://www.dealality.com/opportunity-review` (loads form; focus/tap OK)
- Ecosystem `owner-advisor-led` + CSS `v20260731i`; no `#eco-grid`
- No cinematic-v1 / Deal Desk JS / Phase1 storyboard live

## Root safety

- Home ID `6a234ea31be55631d54044d8`, lastUpdated still `2026-07-30T13:46:47.675Z`
- publishedPath `/` unchanged
- Root HTML differs only by Webflow `Last Published` comment timestamp
- Apex → www unchanged

## Console / network

Package assets all HTTP 200. Pre-existing noise only (Finsweet globe CORS; GA collect aborts). No Manual Process errors.

Artifacts: `/opt/cursor/artifacts/manual-process-prod-qa-20260731/`
