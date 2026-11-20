# GDI Surfe Usage Tracking V1 — Audit & Implementation

**Status:** Implementation complete (PR for Integration Controller)  
**Authorization:** Founder GO — Surfe on-demand usage tracking + Admin → Resources → Surfe Usage  
**Paid Surfe calls for this task:** `$0` (fixture / mock tests only)

## 1. Existing Surfe integration audit

| Area | Finding |
|---|---|
| Live reveal path | `POST …/contacts/reveal` in `api/group-demand-intelligence.js` — user-initiated only (`authorizeLiveProvider` + access grant) |
| Provider | `lib/group-demand-intelligence/gdi-surfe-contact-provider-v1.js` + `lib/surfe/client.js` (server-side API key) |
| Credit gate | `preflightSurfeCredits` / in-flight dedupe in `gdi-surfe-live-gate-v1.js` |
| Prior accounting | Minimal append to `data/group-demand-intelligence/surfe-usage/gdi-live-accounting-v1.json` (no admin UI, weak schema) |
| Workflow ledger | Separate `surfe-usage-ledger-v1.js` for research workflows — **not** reused for drawer reveals (different lifecycle; may hold richer fields). V1 extends **live accounting** only. |
| Ephemeral PII | Reveal responses remain `ephemeral: true`; customer sanitize strips Surfe affordances; no durable email/phone from Surfe |
| Admin Resources | Left nav in `public/app.js` (`Admin Resources` group) — pattern matched to AI Demand Admin + internal runbook gate |

**Safe tracking insertion point:** after live Surfe attempt completes (credits before/after), via non-throwing `appendSurfeLiveAccountingSafe`. Do not track public-only resolutions as Surfe usage when that path exists.

## 2. Files modified / added

| Path | Role |
|---|---|
| `lib/group-demand-intelligence/gdi-surfe-live-gate-v1.js` | Usage schema, PII strip, dedupe, credit/cost derive, summarize |
| `api/admin-surfe-usage.js` | Admin summary + events APIs |
| `api/group-demand-intelligence.js` | Safe append + latency + portfolio/user hash inputs; Surfe-attempted only |
| `server.js` | `GET /api/admin/surfe-usage/{summary,events}` behind `adminAuth` |
| `public/app.js` | Route + Admin Resources → Surfe Usage |
| `public/app/admin/surfe-usage.html` | Dashboard shell |
| `public/css/admin-surfe-usage.css` | Admin styling |
| `public/js/admin-surfe-usage.js` | KPI / trends / hotel / ops client |
| `scripts/test-gdi-surfe-usage-tracking-v1.mjs` | Permanent regression gates |
| `package.json` | `test:gdi-surfe-usage-tracking-v1` |
| `reports/gdi-surfe-usage-tracking-v1/*` | This audit + completion notes |

## 3. Tracking schema & persistence

**Destination:** JSON ledger  
`data/group-demand-intelligence/surfe-usage/gdi-live-accounting-v1.json`  
(override: `GDI_SURFE_ACCOUNTING_PATH`)

**Per-event fields (non-PII):**  
`lookupEventId`, `at`, `hotelId`, `portfolioId`, `opportunityId`, `requestingUserHash` (sha256 truncated), `identityKey` (hash), `lookupType` / `channel`, `status` (`success`|`partial`|`no_match`|`error`), `emailFound`, `phoneFound`, `creditsCharged`, email/mobile credit splits, `costEstimateUsd`, `costEstimateSource`, `providerLatencyMs`, `errorCategory`, `enrichmentId`, `surfeAttempted`, `surface`.

**Never persisted:** emails, phones, raw Surfe payloads, API secrets, person display names.

**Dedup:** identical `enrichmentId`; same `identityKey`+`channel` within 15s.

## 4. Admin UI

- Nav: **Admin → Resources → Surfe Usage** (`/admin/surfe-usage`, `internalRunbookOnly`)
- Gate: `support-admin-gate.js` + server `adminAuth`
- KPIs, daily/weekly/monthly trends, hotel breakdown, latest activity, error list, live remaining credits
- Historical note: pre-instrumentation usage is **not** fabricated

## 5. Metrics supported

Total / successful+partial / success rate / credits / est. cost / avg cost per success; trends; hotel rollups; no-match frequency; latency; recent errors; remaining Surfe credits (live balance read).

## 6. Contact details not persisted

Confirmed by strip list + regression test: forbidden keys removed; email/phone-shaped strings scrubbed; admin APIs set `includesPersonalContactDetails: false`.

## 7. Credit / cost methodology

1. Prefer **balance delta** (`creditsBefore` − `creditsAfter`) when both sides finite.  
2. Else **estimate on success only** using `SURFE_CREDIT_COSTS` × USD estimates from `paid-multisegment-research-v1.js`.  
3. Misses / no-match → **no assumed charge**.  
4. V1: no customer billing, allowances, or new providers.

## 8. Policy preserved

No automatic Surfe; no bulk; no customer billing change; enrichment remains on-demand and ephemeral.
