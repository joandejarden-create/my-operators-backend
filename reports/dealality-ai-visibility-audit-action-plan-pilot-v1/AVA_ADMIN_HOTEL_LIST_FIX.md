# Fix — AI Visibility Audit Admin Hotel List Loading

**Date:** 2026-11-05  
**Status:** Fixed and verified in local development. No production deployment.

## Root cause

**Deployment / process mismatch**, not authentication and not the hotel-list data model.

| Probe | Result |
|---|---|
| Frontend URL | `GET /api/admin/ai-visibility-audit/hotels` |
| Stale process on **:8080** | **HTTP 404** `{"success":false,"error":"API route not found"}` |
| Current process on **:8081** (`.env` `PORT=8081`) | **HTTP 200** `{ ok: true, count: 38, hotels: [...] }` |

The AI Visibility Audit tab UI (including the new JS) was being served, but the Node process on port **8080** was an older build that did **not** register `/api/admin/ai-visibility-audit/*`. The frontend treated any non-OK response as the generic “Unable to load hotels (auth or server error).”

Auth was not the failure mode on the current server: the same `adpMonthlyReviewAdminAuth` stack returns 200 for the hotels list when the route exists.

## Files changed

1. `api/admin-ai-visibility-audit.js` — harden `getAdminAvaHotels` (per-hotel try/catch; universe remains all staged ADP IDs, not only AVA store entries; logged 500 message)
2. `public/js/admin-ai-visibility-audit.js` — status-specific hotel-list errors (401 / 403 / 404 / 500 / network)
3. `public/app/admin/ai-demand-admin.html` — cache-bust script `?v=ava-admin-v3-hotel-list`

## Local process remediation

- Restarted **8081** with current `server.js` (API + hardened hotels handler).
- Replaced stale **8080** listener with current `server.js` so both common local ports serve AVA routes.

## Verification (dev)

| Check | Result |
|---|---|
| Eligible hotel list (38) includes Spice Island, Wyndham, Waterstone, Bethesda, Castillo, Sheraton | Pass |
| Search “Bethesda” | Pass |
| Filter `AUDIT_VALIDATED` | Pass |
| Open Bethesda staged audit drawer | Pass |
| Preview PDF (`application/pdf`) | Pass |
| Recommendations + Approve/Reject/Defer | Pass |
| `generate-pdf` with `publish:true` | **403 PUBLICATION_BLOCKED** |
| `FOUNDER_APPROVED_FOR_CUSTOMER` without flag | **403 FOUNDER_APPROVAL_FLAG_REQUIRED** |
| Express `req.body` POST handling (Phase 8 fix) | Pass |

## Auth & publication gates

- Still mounted behind `adpMonthlyReviewAdminAuth` (Memberstack + Dealality user + ADP monthly-review admin).
- No new authentication bypass introduced.
- `AUDIT_VALIDATED` ≠ `FOUNDER_APPROVED_FOR_CUSTOMER`; PDF generation cannot publish.

## Deployment requirements (not authorized here)

If a remote/staging host shows the same generic error, deploy:

1. Backend `server.js` + `api/admin-ai-visibility-audit.js` (route registration + hotels handler)
2. Frontend `public/js/admin-ai-visibility-audit.js` + `public/app/admin/ai-demand-admin.html`

Then restart the Node process serving that host. No customer publication, paid research, or ADP baseline changes required.
