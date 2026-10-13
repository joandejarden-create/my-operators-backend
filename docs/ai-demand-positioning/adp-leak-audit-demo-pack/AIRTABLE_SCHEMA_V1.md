# AI Demand Leak Audit — Airtable Schema V1 (Phase 2A)

**Status:** Contract locked · filesystem store mirrors tables under `data/ai-demand-positioning/leak-audit/`  
**Code:** `lib/ai-demand-positioning/leak-audit/airtable-schema-contract-v1.js`  
**Isolation:** Leak Audit never writes production ADP, Census, Brand Explorer, or Operator Explorer.

When the physical Airtable base is provisioned, map each table name below 1:1. Until then, `createLeakAuditStore()` collections are authoritative.

## Tables

| Airtable table | Store collection | Purpose |
|----------------|------------------|---------|
| AdpLeakAuditRequests | `requests` | Inbound / manual audit requests |
| AdpLeakAuditHotels | `hotels` | Leak-only hotel records |
| AdpLeakAuditRuns | `runs` | Per-hotel execution runs |
| AdpLeakAuditMetrics | `metrics` | KPI card values |
| AdpLeakAuditCompetitors | `competitors` | Absent-subject competitor appearances |
| AdpLeakAuditEvidence | `evidence` | Client-safe evidence objects |
| AdpLeakAuditActions | `actions` | Priority improvements |
| AdpLeakAuditReports | `reports` | Single-property reports + `shareToken` |
| AdpLeakAuditPortfolios | `portfolios` | Portfolio groups |
| AdpLeakAuditPortfolioHotels | `portfolio-hotels` | Portfolio ↔ hotel join |
| AdpLeakAuditPortfolioRuns | `portfolio-runs` | Portfolio runs |
| AdpLeakAuditPortfolioReports | `portfolio-reports` | Portfolio roll-up + `shareToken` |
| AdpLeakAuditPromotions | `promotions` | Paid ADP pilot stubs only |

## Defaults

- `prioritySource` = `inferred_from_results`
- `researchMode` = `leak_audit_lite`
- Report URLs use `/adp-leak-audit/share/[shareToken]` (not internal IDs)
- `matchedProductionHotelId` / `matchedHotelId` are internal-only and stripped from client payloads
- Free runs default to `maxScenarios=15`, `maxProviders=4`, `maxObservations=60`
- Local storage: filesystem under `data/ai-demand-positioning/leak-audit/live/` when Airtable Leak Audit env is unset
- Code: `airtable-schema.js`, `airtable-client.js`, `leak-audit-repository.js`

## Research mode fields (ADP Lite)

**AdpLeakAuditRuns:** `researchMode`, `maxScenarios`, `maxProviders`, `maxObservations`, `actualScenariosRun`, `actualProvidersRun`, `actualObservations`, `estimatedProviderCost`, `providerCostCurrency`, `completenessFlag`, `costControlNotes`

**AdpLeakAuditReports:** `diagnosticScopeLabel`, `scopeDisclaimer`, `scenarioSummaryLabel`, `researchMode`

Modes: `leak_audit_lite` | `adp_pilot_monthly` | `adp_full_platform`. Free audits must stay on `leak_audit_lite` (`promptSetVersion: leak_audit_lite_v1`). Do not invoke paid ADP history/publish writers.

## Generation entry points

- `generateSinglePropertyLeakAuditReport(store, { requestId, mode })`
- `generatePortfolioLeakAuditReport(store, { hotels })`
- `copyFromAdpToLeakAudit({ dryRun })` via `npm run adp-leak-audit-copy-from-adp-v1`
