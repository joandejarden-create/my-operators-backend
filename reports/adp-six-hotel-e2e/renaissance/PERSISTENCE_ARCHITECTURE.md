# Renaissance ADP — persistence architecture (closeout)

## Policy (`adp-persistence-policy-v1`)

- **PRIMARY_SOT:** filesystem published snapshots  
- **LIVE_OVERLAY:** Airtable Published Reports (optional)  
- **HISTORY_AIRTABLE:** disabled unless explicitly gated  

## What production reads

Default: filesystem (`ADP_PUBLISHED_READ_SOURCE` unset → filesystem).  
Airtable Live is not re-enabled unless `ADP_AIRTABLE_READ_LIVE=1` / explicit source.

## Renaissance `…2ff16a` storage map

| Class | Location | Durable how |
|-------|----------|-------------|
| Report payload | `data/ai-demand-positioning/published/adp_renaissance_times_square/report-…2ff16a.json` | Git + deploy |
| Evidence | `…/evidence-…2ff16a.json` | Git + deploy |
| Manifest | `…/manifest.json` → latestPeriodId `…2ff16a` | Git + deploy |
| Runtime period | `data/ai-demand-positioning/runtime/…2ff16a.json` | Git + deploy |
| Dry-run stub (excluded from CVP) | `runtime/…fdc3db.json` status `DRY_RUN_COMPLETE` | Retained for audit; not customer-comparable |
| Current PDF | `reports/ai-demand-positioning/current-report-pdf/…` + archive `…_v4` | Git + deploy |
| Monthly PDF | monthly-review archive `2026-10/…_v2/report.pdf` | Git + deploy |
| Airtable overlay | `rech8MWMcTRiCDlJm` | Compact JSON + `payloadStoreRef`; evidence stubbed |

## Airtable compaction contract

`compactPayloadForAirtableOverlay`:

- Never truncates mid-string  
- Stubs whole nested keys with `{ storageOnly, payloadStoreRef, field, periodId, propertyId }`  
- Full payload remains on PRIMARY_SOT  

`resolveCompactedAirtablePayload` / `loadPublishedReportFromAirtable`:

- Resolve stubs from validated PRIMARY_SOT paths only  
- Reject traversal, cross-hotel, wrong period, missing files  
- On failure return null → filesystem fallback — **never** return storageOnly stubs to customers  
- Evidence stubs follow the same rule  

Proof: `COMPACTION_READ_PATH_PROOF.md` + `scripts/test-adp-airtable-payload-compaction-v1.mjs`

## Durability proof status

| Check | Result |
|-------|--------|
| Manifest → complete report/evidence files | PASS (local + pre-deploy prod report API) |
| Checksums / period IDs agree | PASS for published edition `…2ff16a` |
| Retry does not duplicate research | PASS (no rerun; resume ledger notes no duplicate submits) |
| Partial Airtable overlay does not replace filesystem edition | PASS (overlay stubs; SoT = filesystem) |
| Customer actions independent of regen | Report actions baked in payload; monthly 5-action set is separate review agenda |
| Isolated PRIMARY_SOT write→read→cleanup probe | PASS — `DURABILITY_PROBE.json` (non-customer) |
| Redeploy survives for **bundled** published artifacts | Expected after PR deploy |
| Redeploy survives for **new ephemeral runtime-only** writes | **NOT PROVEN** without durable volume — precise unresolved blocker |

## Failure injection

Isolated unit tests cover Airtable compaction/resolve and dry-run longitudinal exclusion. Durability probe uses disposable `_durability_probe_*` property only. No destructive injection against live customer Airtable/actions.
