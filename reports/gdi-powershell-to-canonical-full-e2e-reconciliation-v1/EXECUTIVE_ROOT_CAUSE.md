# Executive root cause

## Verdict

**We did not lose contact research.** The canonical Full E2E chain still runs `CONTACT_RESEARCH` after deep research via `runRestoredContactInvestigationPassAsync`.

PR #382's "zero verified named contacts" was primarily a **reporting / serialization gap**, compounded by honest commercial demand gates:

1. Contact pass executed — `25` contacts investigated, `10` sales-ready-classified contacts across `6` opportunities.
2. Named people were attached on opportunity objects in research state (e.g. Cara Neil / True Anomaly, Nicholas Wong / Whiting-Turner, Tina Kang / PCI).
3. `chain.processed` rows exported only `contactsInvestigated` / `salesReadyContacts` counts — **not** `primaryContactName` — so the priority-8 scorecard's `namedContact()` returned empty.
4. Sales-ready remained **0** because most dispositions were `EXCLUDE` / `WATCHLIST_RESEARCH` — demand qualification correctly refused promotion even when a named entry point existed.

## What was NOT missing

- Contact module code (`gdi-original-full-e2e-execution-restore-v1.js`)
- Official-source HTTP follow-up (Surfe intentionally off)
- Deep research + drawer + dry-run persistence stages

## What was missing / bypassed

- Processed-row name hydration for scorecards and operators
- Optional Scaling Companies specialist buyer-intel stage on the fourteen-segment path (`PARTIALLY_MIGRATED`, not required to explain the zero-contact scorecard)
- Universal PS1 `FullRun` still points at the legacy research-to-page coordinator (`HISTORICAL_ONLY` parallel entry)

## Correction shipped in this package

- Hydrate `primaryContactName` / `primaryContact` / `salesReadyContactNames` on chain `processed` rows
- Defensive primaryContact attach from eligible classified people
- Scorecard reads hydrated names + salesReadyContacts tallies
- Deterministic before/after fixture proof (no paid calls)

**Do not relaunch broad paid discovery until operators can see recovered named entry points on the scorecard.**
