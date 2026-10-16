# Full GDI E2E v1.5 — verify-harden + lessons-capture closeout (shared)

**Branch:** `fix/gdi-full-e2e-verify-harden`  
**Contract:** `gdi-full-e2e-contract-v1.5`  
**Regression:** `npm run test:gdi-full-e2e-contract` → **PASS**  
**Leak gate:** `npm run test:gdi-customer-provider-leak-sanitize-v3` → **PASS**

## Shared implementation landed

1. **Delivery verification** — fail-closed schema, hotel identity, publication reconcile, list/detail, provider-leak fail, observed build identity (`full-e2e-verification-adapters-v1.js`).  
2. **Auth reconcile** — ledger authorization identity first; hotel/cap/amount fail-closed.  
3. **Geo** — NYC Midtown local/foreign patterns; disposition probe preserves `localConnection`.  
4. **Publication readiness** — never_executed ≠ provisional/unknown after assessment; corporate + apply_publish + PE promote gated.  
5. **Customer provider-leak sanitize v3** — strip `webhound` / `researchProvider` / `WEBHOUND_*` from customer DTOs; preserve source URLs.  
6. **False contact exhaustion** — enrichment cannot stamp EXHAUSTED without documented attempts/sources.  
7. **Lessons capture required** — `full-e2e-lessons-capture-v1` + coordinator `capture_lessons` (stage P) + evaluator gate; five-axis proof; closeout incorporated / gaps / next-hotel inheritance. Defect fixes must update shared impl + test + register + runbook/prompt in the same task.  
8. **Lesson register reconciled** — MF/SD/Ren outstanding items including inventory, contact-exhaustion, dedupe, provider-leak, assessment-quality, delivery (no duplicate registers).

## Renaissance linked execution (same $12 auth)

See `reports/gdi-six-hotel-e2e/renaissance/CLOSEOUT_A_K.md` (includes lessons closeout section).

- Ledger **$4.06** incurred / **$7.94** available; this pass Parallel/Surfe **$0**  
- Evaluator: **PARTIAL** (clean-main deploy, production leak clear, browser/share/PDF, named-buyer depth remain)

## Deploy

Lean-stage `deploy:production` of this branch after merge: **PENDING**. Production detail leak clear and several `productionVerified` axes wait on that deploy.

## Docs

- `LESSON_REGISTER.md` (v1.5 five-axis)  
- `OPERATOR_RUNBOOK.md` (defect-fix rule + stage P)  
- `CURSOR_PROMPT_FULL_E2E.md` (non-negotiable #18 lessons capture)
