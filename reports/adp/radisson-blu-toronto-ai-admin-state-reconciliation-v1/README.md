# Radisson Blu Toronto — AI Admin State Reconciliation V1

## Trace (exact source)

AI Visibility Audit tab
→ `public/app/admin/ai-demand-admin.html` + `public/js/admin-ai-visibility-audit.js`
→ `GET /api/admin/ai-visibility-audit/hotels`
→ `api/admin-ai-visibility-audit.js` → `getAdminAvaHotels`
→ `resolveAvaAdminHotelRow` (`lib/ai-visibility-audit-action-plan/ava-admin-hotel-row-v1.js`)
→ `getAuditMeta` / `listStoredAudits` (`audit-store-v1.js`, durable + production/store seed merge)
→ `loadAdpBaselineForAudit` (Core period pointer)

## Wrong values (before fix)

| Field | Current | Source | Why wrong | Correct source | Correct value |
|---|---|---|---|---|---|
| Measurement / Audit Status | Not Started | Synthetic `BASELINE_READY` mislabeled as "Not Started" when list meta missed seed | Durable meta `FOUNDER_REVIEWED_STAGED` existed but list/store path missed it | `AUDIT_META.status` via `getAuditMeta` | Founder Reviewed (`FOUNDER_REVIEWED_STAGED`) |
| Count shown as 0 | 0 | Latest version `actionCount` only | Latest entry had 0 while prior versions/accepted count had 5–6 | `resolveRecommendationCount` (max across versions / accepted) | 6 |
| PDF | Missing | `pdfStatus` empty / not inferred | PDF paths existed on versions | `resolvePdfStatus` + staged paths | Available (`STAGED`) |
| Authorization | Not Authorized | `founderApprovedForCustomer=false` / `NOT_AUTHORIZED` | Correct for AVA customer publication governance | Same | Not Authorized (keep) |

## Working-hotel diff (Bethesda / Waterstone)

Shared pattern after fix: measurementComplete=true, pdfStatus=STAGED→UI Available, customerPublicationAuthorized=false.
Toronto differs by auditStatus=`FOUNDER_REVIEWED_STAGED` (Founder Reviewed) vs peers `AUDIT_VALIDATED` (Validated).

## Counters after reconciliation

Eligible 38 · Audits Created 38 · Audits Validated 38 · Approved for Customer **0**
Toronto does **not** increment Published / Approved for Customer.

## Browser + restart QA

Verified on localhost:8080 after server restart + hard refresh:
Radisson Blu Toronto Downtown · Founder Reviewed · 6 · Available · Not Authorized
