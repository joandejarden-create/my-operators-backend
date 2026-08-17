# Brand AI Visibility — Phase 3B.6 Multi-Provider Recurring Monitoring Foundation

**Status:** Foundation (dry-run proven; no live calls)  
**Baseline Freeze:** `FOUR_PROVIDER_BASELINE_V1_COMPLETE` (336/336 immutable)

## Objective

Build governed biweekly recurring-monitoring architecture without executing Period 2 or activating the scheduler.

## Commands

```bash
npm run test:ai-visibility-phase3b6          # foundation tests
npm run ai-visibility:phase3b6-dry-run       # Period 2 dry-run manifest (336 requests)
node scripts/ai-visibility-phase3b6-dry-run.mjs --create-recurring-period
node scripts/ai-visibility-phase3b6-dry-run.mjs --resume --period-id=<periodId>
```

**Blocked in 3B.6:** `--execute` (no live provider calls)

## Key modules

| Module | Purpose |
|---|---|
| `recurring-period-model.js` | Period schema, states, idempotency keys |
| `recurring-monitoring-config.js` | Cadence, caps, execution order, 336 matrix |
| `recurring-comparability.js` | Period-to-period comparability keys |
| `recurring-drift-guards.js` | Model/tool/prompt/peer/metric pre-run guards |
| `recurring-period-orchestrator.js` | Create period, dry-run, checkpoint/resume |
| `recurring-period-read-service.js` | Latest/prior/specific period reads |
| `period-trend-foundation.js` | Trend calc foundation (no values until Period 2) |
| `period-source-change-foundation.js` | Source change foundation (no movement yet) |
| `phase3b6-orchestrator.js` | Phase report builder |

## Period model

- **Baseline period:** purpose=`baseline`, immutable reference to freeze
- **Recurring periods:** purpose=`recurring`, biweekly cadence (scheduler disabled)
- **Period ID:** `aiv_monitoring_period_<YYYYMMDD>_<hex>`
- **Observation unique key:** `periodId|provider|semanticFingerprint`

## Hard caps (from baseline actuals + buffer)

Derived via `deriveRecurringHardCaps()` from stored baseline cost ledgers.

## Next recommended phase

**PHASE_3C1_DISCOVERABILITY_BUSINESS_IMPACT_FOUNDATION** (founder priority)

Then: **PHASE_3B7_FIRST_RECURRING_MULTI_PROVIDER_PERIOD** when approved for live execution.

## Regression

```bash
npm run test:ai-visibility-phase3b5
npm run test:ai-visibility-phase3b4
npm run test:ai-visibility-phase3b3
npm run test:ai-visibility-phase3b2
npm run test:ai-visibility-phase3b1
npm run test:ai-visibility-phase3a11
```
