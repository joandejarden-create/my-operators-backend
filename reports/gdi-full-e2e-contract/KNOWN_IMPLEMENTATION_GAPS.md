# Known implementation gaps (v1.2 fail-closed)

## Corrected since #118 → v1.1 → v1.2

- Empty / omitted / unknown / duplicate segment axes fail closed.  
- Deliverable `gapRecorded` no longer satisfies COMPLETE.  
- Contact queue reconciliation from buyer-enrichment artifacts.  
- Local/production stages execute API adapters (not status copy).  
- Resume re-runs stale/not_run verification via fingerprints.  
- Budget reconcile inspects authoritative ledger + hotel/product isolation.  
- Research auth ≠ publish auth; `--dry-run` overrides all paid/writes.  
- Status CLI writes **only** under `--out-dir` (never mutates source run journals).  
- **v1.2:** sports/edu independent assessment; org-route cannot skip contact depth; publication requires structured hotel-fit + drawers; canonical persist asserts eligibility research fields (no post-import repair); identity dedupe gate; lesson register + Cursor prompt.

## Still gaps / manual

1. Authenticated Memberstack browser drawer walkthrough (API list/detail automated).  
2. Cross-run join of continuation scorecards into an older ledger’s segmentCoverage.  
3. Share-link mint UX (existing paths; no tokens in git).  
4. PDF visual clipping QA when PDF routes run.  
5. Automatic population of `publication.structuredHotelFitOk` / `drawersComplete` / `identityDedupeReconciled` from every persist path into the run evidence overlay (evaluator already requires them when present on publication claims).
