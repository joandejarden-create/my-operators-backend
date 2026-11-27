# GDI Surfe miss capture & FullEnrich benchmark preparation — Founder report

**Date:** 2026-11-21  
**Branch:** `feat/gdi-surfe-miss-fullenrich-benchmark-prep-v1`  
**Workspace:** exceptional temp WT (registered `ws-gdi` preserved dirty on unrelated feature branch)  
**Paid enrichment:** NONE — FullEnrich remains OFF  
**Airtable mutations:** NONE  
**Customer-facing FE results:** NONE  

---

## 1. Current Surfe miss-tracking capability

Canonical on-demand reveal accounting (`gdi-surfe-live-gate-v1` / usage tracking v1.2) already records non-PII Surfe lookups with:

| Field | Present |
|---|---|
| Canonical opportunity ID | Yes (`opportunityId`) |
| Opportunity hotel ID / name / market / country | Yes |
| Requesting organization / office | Yes |
| Lookup channel | Yes (`channel` / `channelRequested`) |
| Provider result / status / miss | Yes (`status`, `miss`, `emailReturned`, `phoneReturned`) |
| Credit consumption + certainty | Yes |
| Timestamp | Yes (`at`) |
| Environment + fixture classification | Yes (`dataEnvironment`, `usageClass`) |
| Hotel/opportunity-scoped identity key | Yes (`identityKey` = hash hotel\|opp\|name\|channel) |
| Contact **name** stored | **No** (stripped — by design) |
| Cross-hotel person key | **Added this PR** (`personIdentityKey`) |
| Contact name hash | **Added this PR** (`contactNameHash`) |
| FE identity-input readiness flags | **Added this PR** (`fullEnrichIdentityInputs`) |

Admin Surfe Usage UI + APIs remain admin-only and never expose provider-returned emails/phones.

## 2. Missing attribution / identity fields (before → after)

**Before:** Miss rows could not reliably re-identify the verified person across hotels without recomputing `identityKey` against live GDI opportunity contacts. No durable person-level dedupe key. No FE input-readiness flags on the ledger.

**After:**

- `personIdentityKey` — hash(name + organization); requires first+last and org (no name-only merge)
- `contactNameHash` — non-PII name fingerprint for resolution assist
- `fullEnrichIdentityInputs` — boolean readiness only (first/last, company, domain, LinkedIn)
- Admin join resolves display names from **public GDI opportunity records** via `identityKey` / person key

## 3. Genuinely eligible contacts (live ledger)

Against `ws-gdi` Surfe accounting at report time (29 Surfe entries: 16 partial, 13 `no_match`):

- **Genuine eligible Surfe miss events:** `0`
- **Distinct verified contacts:** `0`
- **Fixtures excluded (no_match / miss class):** `13`
- **Benchmark shortlist:** empty

All observed email misses are fixture/test-classified. There are still **no proven production Surfe work-email misses** suitable for a FullEnrich benchmark. The system can now detect and report them when they occur.

## 4. Cross-hotel deduplication

Regression-proven:

- Same verified person + organization appearing on two hotels/opportunities → **one** shortlist row
- Both opportunity links retained with requesting-organization attribution
- Same display name + different organizations → **different** person keys (no false merge)

## 5. Benchmark-ready shortlist

**None** (live). Admin view path:

- `GET /api/admin/surfe-usage/fullenrich-benchmark-eligibility`
- Surfe Usage admin panel → “FullEnrich benchmark readiness”

Estimated credit exposure formula: `1 credit × distinct eligible contacts` (docs). Current exposure: **0**.

## 6. Privacy and accounting validation

- No provider-returned emails/phones persisted
- Eligibility module does not call FullEnrich client / API key / paid submit
- Report flags: `paidLookupsTriggered: false`, `customerFacing: false`, `customerDisplayBlocked: true`
- Surfe remains primary; FullEnrich flags remain off
- Tests assert ledger rows lack `contactName` / email values

## 7. Changes made and tests

**Code**

- `lib/group-demand-intelligence/gdi-fullenrich-benchmark-eligibility-v1.js` (new)
- `api/admin-surfe-usage.js` — eligibility endpoint
- `server.js` — admin route
- `gdi-surfe-live-gate-v1.js` — persist person key / name hash / identity-input flags
- `gdi-fullenrich-spend-ledger-v1.js` — `listAllFullEnrichJobs`, `personIdentityKey` on jobs
- `api/group-demand-intelligence.js` — capture identity fields on Surfe (and FE) accounting appends
- Admin Surfe Usage HTML/JS panel
- `scripts/test-gdi-fullenrich-benchmark-eligibility-v1.mjs`
- `package.json` script `test:gdi-fullenrich-benchmark-eligibility-v1`

**Tests**

- `node scripts/test-gdi-fullenrich-benchmark-eligibility-v1.mjs` → **PASS (9)**
- `node scripts/test-gdi-fullenrich-fallback-reliability-v1.mjs` → **PASS**
- `node scripts/test-gdi-surfe-usage-tracking-v1.mjs` → **PASS**

## 8. PR status

- **PR:** [#533](https://github.com/joandejarden-create/my-operators-backend/pull/533) — **OPEN**
- GDI chat does **not** merge — Integration Controller owns merge + CI.
- Registered `ws-gdi` left dirty/unrelated — preserved. Exceptional WT used for this PR only.

## 9. Remaining licensing requirements (unchanged HOLD)

Before any paid FullEnrich benchmark:

1. Founder GO for controlled, capped spend
2. Partner / licensing path if third-party Enriched Data display is desired (customer display remains BLOCKED)
3. At least one genuine eligible Surfe miss in the shortlist
4. Explicit confirmation FullEnrich remains secondary; Surfe primary
5. No production deploy / no Airtable writes from this workstream without separate authorization

**STOP — awaiting founder review before any paid enrichment.**

---

`AWAITING_INTEGRATION_CONTROLLER`
