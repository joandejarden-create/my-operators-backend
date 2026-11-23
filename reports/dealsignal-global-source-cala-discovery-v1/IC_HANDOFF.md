# Integration Controller Handoff — Global Source Expansion & CALA-First Discovery V1

**Workspace:** `C:\Dev\dealality\ws-hif-v6-track-a`  
**Branch:** `feat/hif-v6-track-a-cb5-trigger-first-portfolio` @ `e8007e006` (+ uncommitted research)  
**Publication:** CUSTOMER_BLOCKED  
**Paid research:** $0  

## Code / config added (reusable)

| Path | Role |
|---|---|
| `lib/hotel-intelligence/dealsignal/dealsignal-source-registry-v1.js` | Versioned source registry (13 sources; readiness + tested status) |
| `lib/hotel-intelligence/dealsignal/dealsignal-cala-research-priority-v1.js` | CALA queue tiers + fairness controls; reuses Track C geography |
| `lib/hotel-intelligence/dealsignal/dealsignal-multilingual-query-lexicon-v1.js` | EN/ES/PT commercial query lexicon |
| `scripts/test-dealsignal-cala-research-priority-v1.mjs` | Focused unit test |
| `scripts/dealsignal-global-source-cala-discovery-v1-persist.mjs` | Dry-run / apply persistence |

## Extends (does not replace)

- `hif-v6-track-a-source-inventory.js`  
- `SOURCE_FAMILY` / CMBS extractors  
- `intl-readiness-v1.js`  
- Track C `CALA_COUNTRY_LABELS` / `isCalaCountry`

## CALA scheduling rules

Implemented as **research-processing priority only**:

1. Time-critical global  
2. High-value CALA  
3. Mexico when comparable  
4. Other CALA  
5. Remaining global  

Fairness: no hide/downgrade of non-CALA; no CALA confidence inflation; hotel geography ≠ owner HQ.

## Airtable

- 12 creates (6 inv + 6 findings); idempotent 12 NOOP  
- Ledger: `PERSISTENCE_LEDGER_FIRST_APPLY.json`  
- Key IDs: FINN parent `rec246OEIvYAfRJFD` / `recp0bShKmxpOFQ2N`; Yucatán Resort `recKfUmMDG5JJDUpP` / `rec3YUdLkWthuLmrM`

## Track B / C

- `TRACK_B_HANDOFF.md`  
- `TRACK_C_HANDOFF.md`

## Not in this wave

- Full automation scheduler  
- New Airtable schema  
- Paid Exa  
- Surfe  
- Production publish  
- Drawer UI changes (separate IC quality branch if needed)

## Status

`INTEGRATION_IN_PROGRESS` — IC branch `feat/dealsignal-source-expansion-integration-v1` absorbs reusable modules + research package + DealSignal drawer wiring. Airtable already persisted (idempotent); do not re-apply unless NOOP verification requested.
