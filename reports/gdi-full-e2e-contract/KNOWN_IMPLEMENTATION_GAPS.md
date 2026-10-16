# Known implementation gaps (v1.1 fail-closed)

## Corrected since #118

- Empty / omitted / unknown / duplicate segment axes fail closed.  
- Deliverable `gapRecorded` no longer satisfies COMPLETE.  
- Contact queue reconciliation from buyer-enrichment artifacts.  
- Local/production stages execute API adapters (not status copy).  
- Resume re-runs stale/not_run verification via fingerprints.  
- Budget reconcile inspects authoritative ledger.  
- Research auth ≠ publish auth; `--dry-run` overrides all paid/writes.  
- Status CLI writes **only** under `--out-dir` (never mutates source run journals).

## Still gaps / manual

1. Authenticated Memberstack browser drawer walkthrough (API list/detail automated).  
2. Cross-run join of continuation scorecards into an older ledger’s segmentCoverage.  
3. Share-link mint UX (existing paths; no tokens in git).  
4. PDF visual clipping QA when PDF routes run.  
5. No new hotel activation / no new paid batch in this correction task.
