# GDI FullEnrich Fallback V1 — Reliability Hardening Report

**Date:** 2026-11-21  
**PR:** [#521](https://github.com/joandejarden-create/my-operators-backend/pull/521)  
**Status:** HARDENING COMPLETE — AWAITING INTEGRATION CONTROLLER  
**Paid spend / customer display flags:** OFF

---

## 1. Spending-control architecture

Authoritative budget control moved from process-local memory to a durable ledger:

`lib/group-demand-intelligence/gdi-fullenrich-spend-ledger-v1.js`

- File-backed JSON under `data/group-demand-intelligence/fullenrich-spend/` (override via `GDI_FULLENRICH_SPEND_DIR` / `_PATH`).
- Exclusive `wx` file lock (same pattern as Surfe usage accounting) — concurrent requests and multiple processes on a shared filesystem cannot independently exceed the cap.
- **Reserve before submit** → `reserveFullEnrichSpend`.
- **Attach enrichment id** → `markFullEnrichJobSubmitted` (no PII).
- **Reconcile** → confirmed / estimated / UNKNOWN / released.
- Timeouts, errors, and no-matches default to **UNKNOWN** (never assumed free).
- Ledger write/lock failure sets `accountingHealthy=false` and **blocks** new paid FullEnrich requests.

## 2. Restart and concurrency test results

`npm run test:gdi-fullenrich-fallback-reliability-v1` — **PASS** (all cases), including:

- Durable reserve survives re-read (restart simulation)
- Pending enrichment recovery without duplicate paid POST
- Concurrent reserves cannot exceed cap
- Cap exhaustion after UNKNOWN reconcile
- Duplicate clientRequestId suppression

`npm run test:gdi-fullenrich-selective-fallback-v1` — **PASS**

## 3. Async recovery behavior

Non-PII job records store: lookupRequestId, enrichmentId, hotelId, opportunityId, provider status, timestamps, reservation/charge status, retry/reconciliation status.

After restart / duplicate client request with known enrichmentId:

1. Reserve returns `duplicate: true` (no new spend).
2. `prepareFullEnrichJobRecovery` marks recovery-poll intent.
3. Provider `reveal({ recoverEnrichmentId })` **GETs/polls only** — does not start a new paid bulk job.

## 4. Provider usage and cost tracking

Ledger metrics (separate from Surfe credit pools):

- Eligible fallback offers
- Confirmed paid submissions
- Successful work-email recoveries / no-matches
- Credits confirmed / estimated / unknown
- Incremental recovery rate
- Cost per incremental successful work email
- Duplicate paid submissions prevented
- `distinguishedFromPublicSource=true` on FE recoveries (not permanent public-source GDI contacts)

Surfe Usage accounting continues to attribute `contactProvider: FULLENRICH` without corrupting Surfe totals.

## 5. Privacy verification

- Ledger strip of forbidden PII keys (email/phone/payload/contactDetails).
- API still returns `contactDetails: null` + `customerDisplayBlocked: true` for FE results.
- Fixture test asserts no email/phone strings in ledger file.

## 6. CI results

Local:

- `test:gdi-fullenrich-selective-fallback-v1` PASS
- `test:gdi-fullenrich-fallback-reliability-v1` PASS
- `dealality:ci-precheck` (to be re-run on push)

GitHub CI on updated SHA — Integration Controller verifies after push.

## 7. Updated PR SHA

See latest commit on `feat/gdi-fullenrich-selective-fallback-v1` after push (updates PR #521).

## 8. Remaining licensing and activation requirements

1. Confirm FullEnrich embedded/customer-facing redistribution rights.
2. Keep `GDI_FULLENRICH_CUSTOMER_DISPLAY=0` until rights confirmed.
3. Explicit founder spend authorization + `GDI_FULLENRICH_FALLBACK_SPEND_AUTHORIZED=1`.
4. Enable offer flag only after staging validation on localhost:8080.
5. Phone fallback remains disabled.
6. Do not deploy/activate paid FullEnrich in production without further approval.

**STOP — hand to Integration Controller for merge + localhost:8080 staging integration. Do not activate paid spend or customer display.**

`AWAITING_INTEGRATION_CONTROLLER`
