# Known implementation gaps (v1.4 — honest)

## Corrected since #118 → v1.1 → v1.2 → v1.3 → v1.4

- Empty / omitted / unknown / duplicate segment axes fail closed.  
- Deliverable `gapRecorded` no longer satisfies COMPLETE.  
- Contact queue reconciliation from buyer-enrichment artifacts.  
- Local/production stages execute API adapters (not status copy).  
- Resume re-runs stale/not_run verification via fingerprints.  
- Budget reconcile inspects authoritative ledger + hotel/product isolation; **ledger identity first**.  
- Research auth ≠ publish auth; `--dry-run` overrides all paid/writes.  
- Status CLI writes **only** under `--out-dir` (never mutates source run journals).  
- **v1.2:** sports/edu independent assessment; org-route cannot skip contact depth; publication requires structured hotel-fit + drawers; canonical persist asserts eligibility research fields (no post-import repair); identity dedupe gate; lesson register + Cursor prompt.  
- **v1.3:** delivery verify fail-closed (schema/hotel/manifest/list-detail/leaks/build); auth reconcile ledger-first; geo preserves `localConnection`.  
- **v1.4 honesty + harden:**  
  - Lesson register uses **per-axis proof** — not blanket `IMPLEMENTED_AND_TESTED` from fixture rejects.  
  - Independent segment requires **supported** independent evidence (not anti-proxy silence).  
  - Canonical eligibility uses **use-case suitability** — `whyThisMatters` / `proposedHotelUse` alone insufficient.  
  - `assertPublicationWriteReadiness` gates authorized corporate persist + `apply_publish` + PE new promotions.  
  - Publication quality / dedupe prefer **canonical persist outputs**.  
  - Mayflower / San Diego lessons (PE schema/link, publication metadata, contact ownership, local/prod parity) registered explicitly.  
  - **Customer provider-leak sanitize v3:** strips `evidence.researchProvider` / `researchLevel`, `webhoundUsed`, and `WEBHOUND_*` channel-source enums from list/detail DTOs while preserving source URLs; regression `test:gdi-customer-provider-leak-sanitize-v3`.  
  - Readiness distinguishes **unfinished assessment** (missing / never_executed) from **honest unknown facts** after a provisional / insufficient_evidence assessment.

## Still gaps / incomplete proof

1. Authenticated Memberstack browser drawer walkthrough (API list/detail automated).  
2. Clean-main lean-stage `deploy:production` of v1.4 from `main` (wrapper correctly refuses this feature branch until merge).  
3. Share-link mint UX (existing paths; no tokens in git).  
4. PDF visual clipping QA when PDF routes run.  
5. Automatic population of publication-quality flags from **every** persist path (partially wired via `derivePublicationQualityFromPersistOutputs`).  
6. Named-buyer depth for majority of Renaissance accounts (`NAMED_RESEARCH_NOT_ATTEMPTED` still material).  
7. Structured Overall Hotel Fit tiles thin on some lean Ren packages.  
8. Production still serves pre-v3 sanitize until clean-main deploy — live detail currently leaks `webhound` on several legacy facing rows (76th withdrawn / 404; leak is shared projection, not one ID).  
9. Cross-hotel contact-ownership bleed audit not fully automated in Full E2E.
