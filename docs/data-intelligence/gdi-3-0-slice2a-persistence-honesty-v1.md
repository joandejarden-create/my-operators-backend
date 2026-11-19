# GDI 3.0 Slice 2A — Persistence Honesty

**Status:** Implementation PR (founder review before merge)  
**Mode:** $0 code + dry-run tests only  
**Non-goals:** Slice 2B/2C implementation · production Airtable writes · schema mutations · paid research · default-on Airtable reuse · expanding this PR into all GDI 3.0 pillars

## Founder correction (binding)

Weekly examples were **accumulation illustrations**, not separate research phases.

**Production target:** one hotel → one integrated Full E2E run → complete applicable intelligence → durable persistence → accurate Weekly Change → actionable briefs.

Development phases remain the **build/certify order**. Production execution must not defer contacts, partners, competitive evidence, Weekly Change, or persistence to a “later weekly pipeline.”

Contract: `docs/data-intelligence/gdi-3-0-complete-intelligence-one-run-founder-correction-v1.md`  
Machine: `lib/group-demand-intelligence/workflow/gdi-full-e2e-complete-intelligence-lifecycle-contract-v1.js`  
Backlog of unwired one-run capabilities: `reports/gdi-3-0-one-run-intelligence-capability-backlog-v1/BACKLOG.md`

Slice 2A therefore requires:

- Persistence honesty so available intelligence is not lost mid-run  
- Visible persistence failure (`ok:false`) — durable completion must not be claimed on failed persistence  
- Lifecycle dimension outcomes (`VERIFIED` / `REUSED` / `NOT_FOUND` / …) as the certification vocabulary for later Full E2E wiring  
- Explicit backlog for dimensions not yet completable in one run

## What changed

1. **`chain.persistenceOpportunities`** — full commercial opportunities carried alongside thin `processed[]` scorecards (trace omits bulk).
2. **`gdi-research-inventory-slim-payload-v1`** — allowlist slim payload preserving identity, evidence URLs, contacts/roles, fit, partners, competitor fields, continuation, run economics, commercial brief inputs. Strips sales/hotel-private/publication/telemetry.
3. **Bridge** — prefers `input.opportunities` → `chain.persistenceOpportunities` → ORS reload (`GDI_DATA_ROOT` aware) → thin row (fail-closed). Lossless guard fails when contacts/evidence disappear or thin scorecards are the only source. Build/upsert failures set `ok:false`.
4. **Store** — default serialization uses slim payload; `sourceSummary` also reads `evidence[]` URLs. 95k fail-closed retained.
5. **One-run lifecycle contract** — 12 dimensions + dimension outcomes + durable-completion evaluator (claim blocked while dimensions `NOT_YET_WIRED`).
6. **Tests** — Bethesda / Radisson / YOTEL frozen fixtures prove retention, thin-row failure, ORS reload, idempotency, sales-key strip, reconstruction; lifecycle contract unit test.

## Results

See `reports/gdi-slice2a-persistence-honesty-v1/FOUNDER_RESULTS.md`.

## Remaining (not in 2A — see backlog)

- Typed contact/evidence columns on Research Opportunities (JSON sufficient for now)
- DG / Program / Signal / Fit wiring in the **same** Full E2E run → **Slice 2B**
- Weekly Change as required default step on every Full E2E run
- Default-on Airtable reuse → after identity/confidentiality validation
- Populate lifecycle ledger from live chain stages
- ORS gitignored local files are not production-durable without mirrored http evidence URLs

## Acceptance

```bash
npm run test:gdi-slice2a-persistence-honesty-v1
npm run test:gdi-full-e2e-complete-intelligence-lifecycle-contract-v1
```
