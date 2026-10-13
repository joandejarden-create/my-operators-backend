# AI Demand Leak Audit — Airtable Generation QA (Phase 2A)

**Status:** Active  
**Date:** 2026-09-07  
**Template:** Locked (`TEMPLATE_LOCK_V1.md`) — do not redesign during generation work

## Storage modes

| Mode | When | Root / target |
|------|------|----------------|
| **filesystem_fallback** | `AIRTABLE_LEAK_AUDIT_BASE_ID` unset | `data/ai-demand-positioning/leak-audit/live/` |
| **airtable** | Leak Audit base + API key configured | `AIRTABLE_LEAK_AUDIT_BASE_ID` only |

Repository API: `createLeakAuditRepository()` — generation does not care which backend is active.

Isolation rules:

* Never write to paid ADP history / publish snapshots / Census / Brand Explorer / Operator Explorer
* Leak Audit base must not equal `ADP_AIRTABLE_BASE_ID` unless `AIRTABLE_LEAK_AUDIT_ALLOW_SHARED_BASE=1`

## Local QA checklist

1. Confirm env:
   - Without Leak Audit Airtable env → expect `storageBackend: filesystem`
   - With env → expect `storageBackend: airtable` (dual-write optional)
2. Create request via `/admin/adp-leak-audits/new`
3. Approve + run (sample_seed path) from admin queue/reports
4. Confirm metrics / competitors / evidence / actions exist under live store
5. Open web report via share token (`/adp-leak-audit/share/[token]`)
6. Download PDF from shared renderer (print)
7. Copy share link (token only — no Airtable/production IDs)
8. Mark sent
9. Promote stub only (`productionAdpRecordCreated: false`)
10. Portfolio: create via `/admin/adp-leak-audits/portfolios` generation path

## Copy from ADP

```bash
npm run adp-leak-audit-copy-from-adp-v1 -- --dry-run
npm run adp-leak-audit-copy-from-adp-v1 -- --apply --hotelName "Cambridge Beaches Resort & Spa" --mode leak_audit_lite --scope 15x4 --output filesystem
```

## Gates

```bash
npm run test:adp-leak-audit-airtable-schema-v1
npm run test:adp-leak-audit-copy-from-adp-v1
npm run test:adp-leak-audit-report-generation-v1
npm run test:adp-leak-audit-portfolio-generation-v1
npm run test:adp-leak-audit-lite-research-mode-v1
npm run test:adp-leak-audit-template-lock-v1
```

Each gate / admin meta response should report whether **Airtable** or **filesystem fallback** was used.
