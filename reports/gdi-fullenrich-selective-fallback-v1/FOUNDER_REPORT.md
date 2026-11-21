# GDI FullEnrich Selective Fallback V1 — Founder Report

**Status:** IMPLEMENTATION COMPLETE — AWAITING INTEGRATION CONTROLLER  
**Date:** 2026-11-21  
**Branch:** `feat/gdi-fullenrich-selective-fallback-v1`  
**Activation:** **OFF** (server flags default off; no paid calls; no customer display)

---

## 1. Implemented fallback architecture

Surfe remains the primary on-demand email/phone provider. After a Surfe response:

1. Server evaluates channel-specific FullEnrich eligibility (pure, no paid call).
2. If work email is missing and flags allow, response includes `fullEnrichFallback.offerVisible=true`.
3. Client shows a secondary CTA (**Try FullEnrich work email**) — never auto-executes.
4. User must confirm (`window.confirm`) potential credit use.
5. Confirmed request posts `authorizeFullEnrichFallback` + `fullEnrichFallbackOnly` (no Surfe re-spend).
6. FullEnrich adapter polls asynchronously; results stay ephemeral; **PII is redacted from the API response** until licensing clears.

Phone fallback is hard-disabled in V1.

## 2. FullEnrich API integration reused

Thin adapter: `lib/group-demand-intelligence/gdi-fullenrich-contact-provider-v1.js`  
Reuses `lib/fullenrich/client.js` (`startFullEnrichBulkWorkEmailsOnly`, `pollFullEnrichBulkUntilDone`, credit costs).  
API keys never leave the server. No duplicated HTTP client.

## 3. Feature flags and activation conditions

| Env | Default | Meaning |
|-----|---------|---------|
| `GDI_FULLENRICH_FALLBACK_ENABLED` | off | Offer eligibility after Surfe |
| `GDI_FULLENRICH_FALLBACK_SPEND_AUTHORIZED` | off | Allow paid FE after user confirm |
| `GDI_FULLENRICH_FALLBACK_PHONE_ENABLED` | ignored | Forced off in V1 |
| `GDI_FULLENRICH_CUSTOMER_DISPLAY` | off | Must stay off; if set, blocks spend/offer |
| `GDI_FULLENRICH_CREDIT_RESERVE` | 20 | Refuse when balance ≤ reserve |
| `GDI_FULLENRICH_SPEND_CAP` | 10 | Process budget cap |
| `FULL_ENRICH_API_KEY` | required for live | Existing key |

Activation requires: flags on + spend authorized + balance above reserve + remaining spend cap + user confirm. Customer display of FE-derived emails remains blocked regardless.

## 4. Asynchronous lookup behavior

- Pending / timeout (`FULLENRICH_TIMEOUT`) with bounded poll (`GDI_FULLENRICH_POLL_MS`, default 120s)
- In-flight dedupe via ephemeral store (`LOOKUP_IN_FLIGHT`)
- Short-lived in-memory result cache (TTL ~5 min, delete on consume/expiry)
- Request `close` aborts further polling semantics (`ABORTED`)
- No new paid start while a lookup is in flight for the same identity key
- Clear miss / success / error states

## 5. Usage and cost attribution

Extended non-PII Surfe Usage ledger (`gdi-surfe-live-gate-v1`):

- `contactProvider`: `SURFE` | `FULLENRICH`
- `fullenrichAttempted`, `fallbackReason`, `incrementalEmailFound`
- `channelRequested`, enrichment / client request IDs, latency, credits
- Surfe credit/cost totals **exclude** FullEnrich rows (FE credits tracked separately)
- Distinguishes Surfe-found contacts vs FullEnrich incremental recovery

## 6. Privacy and licensing safeguards

- No persistence of emails, phones, or raw provider payloads to Airtable/disk ledgers
- Ephemeral store only; forbidden PII keys stripped from accounting
- API returns `contactDetails: null` for FullEnrich with `incrementalEmailFound` + `customerDisplayBlocked` — **no transient display workaround**
- Feature remains disabled until licensing + operational gates pass

## 7. Automated test results

```
npm run test:gdi-fullenrich-selective-fallback-v1  → PASS (all fixture scenarios)
npm run test:gdi-surfe-on-demand-contact-reveal-v1 → PASS
npm run test:gdi-surfe-usage-tracking-v1           → PASS
scripts/test-gdi-surfe-usage-v1-2.mjs              → PASS
scripts/test-gdi-surfe-usage-v1-3.mjs              → PASS
```

No paid FullEnrich or Surfe enrichment calls during implementation/tests.

## 8. PR and CI status

See PR opened for Integration Controller. Local CI precheck run before open. **Do not merge/activate from GDI chat.**

## 9. Proposed controlled benchmark (post-activation approval)

Once licensing + spend auth approved:

1. Enable flags on a non-production host with spend cap ≤ 5 credits and reserve ≥ 20.
2. Select ≤ 5 Surfe email-miss contacts with strong LinkedIn/company identifiers.
3. Run confirmed FE email fallback only; measure incrementalEmailFound rate, latency, credits charged.
4. Stop when spend cap hit or sample complete.
5. Report incremental coverage vs Surfe-only baseline — still without customer display until rights confirmed.

**Do not run this benchmark without founder spend authorization.** Current reported balance: 36 credits.

## 10. Remaining activation blockers

1. **Licensing / embedded redistribution rights** for FullEnrich contact details to hotel customers — unresolved.
2. **Customer display** must remain blocked; separate governance decision required before any UI reveal of FE emails.
3. Explicit **`GDI_FULLENRICH_FALLBACK_SPEND_AUTHORIZED=1`** + founder budget approval.
4. Phone fallback remains out of scope for V1.
5. Confirm miss billing semantics with provider before assuming zero-cost misses.
6. Integration Controller merge + post-merge sync of `ws/gdi`.

---

**STOP for founder review before activation.**

`AWAITING_INTEGRATION_CONTROLLER`
