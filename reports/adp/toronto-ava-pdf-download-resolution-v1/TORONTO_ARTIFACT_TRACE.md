# TORONTO ARTIFACT TRACE

| Artifact | Path |
|----------|------|
| AVA CURRENT / PUBLISHED PDF | `reports/ai-visibility-audit-action-plan/archive/adp_radisson_blu_toronto_downtown/current.pdf` |
| AVA named November PDF (same bytes) | `reports/ai-visibility-audit-action-plan/archive/adp_radisson_blu_toronto_downtown/Dealality - AI Visibility Audit & Action Plan - Radisson Blu Toronto Downtown - NOVEMBER2026.pdf` |
| AVA DRAFT PDF | Distinct version file may exist under durable `data/ava-data/pdfs/...` — **not** used by generic Download PDF for PUBLISHED Toronto |
| STANDARD ADP PDF | Separate ADP current-report PDF under `reports/ai-demand-positioning/current-report-pdf/adp_radisson_blu_toronto_downtown/` |
| PERFORMANCE REVIEW PDF | Separate monthly-review archive under `reports/ai-demand-positioning/monthly-review/archive/adp_radisson_blu_toronto_downtown/` |

## Metadata (canonical Admin store)

`data/ai-visibility-audit-action-plan/production/store/adp_radisson_blu_toronto_downtown/AUDIT_META.json`

- status: `PUBLISHED`
- founderApprovedForCustomer: true
- customerReady / CUSTOMER_READY: true
- customerPublished: true
- published SHA: `a75de648c24b981bb5b0e9f9707e2981dc5f0ce4904a18ad2670e763050bca1e`

## Button trace (after fix)

| Layer | Location |
|-------|----------|
| HTML/JS | `public/js/admin-ai-visibility-audit.js` — `buildActions` label **Download PDF** / `handleAct` → channel `current` |
| API | `GET /api/admin/ai-visibility-audit/:propertyId/pdf?channel=current&download=1` |
| Handler | `api/admin-ai-visibility-audit.js` → `getAdminAvaPdf` |
| Resolver | `lib/ai-visibility-audit-action-plan/ava-pdf-channels-v1.js` → `resolveAvaPdfByChannel` + `pickLifecycleCurrentPdfChannel` |
