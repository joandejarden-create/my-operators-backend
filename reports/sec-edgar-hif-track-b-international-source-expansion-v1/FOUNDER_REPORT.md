# Track B — International Contract & Capital Source Expansion V1

**As of:** 2026-11-23  
**Publication:** CUSTOMER_BLOCKED  
**Cost:** $0  

## Existing sources reused
- SEC EDGAR 10-K/10-Q schedules
- Track B global waves persist pipeline
- HI discovery inventory / findings / contracts / milestones
- year/month precision + debt≠conversion rules

## New sources tested
- CLDT_YE2025_FRANCHISE_IHM: productive=true
- PEB_YE2025_FRANCHISE_NONTERM_MGMT: productive=true
- PEB_IR_JULY_2026_TERMINABILITY: productive=true
- PEB_Q3_2026_MORTGAGE_REPAYMENT: productive=true (capital only)
- FIBRA_INN_ES_PRESS_28_HOTELS: productive=true
- FIBRA_INN_GOVERNANCE_AIMBRIDGE: productive=false
- BR_CVM: productive=false (staged)

## Integrations implemented
- hif-track-b-international-source-registry-v1.js
- hif-track-b-international-source-expansion-dossiers-v1.js
- reports batch for hif-track-b-global-waves-persist.mjs --batch-dir=

## Property-level findings
- CLDT: 33 franchise schedule hotels (year-only) + Holtsville IHM notice terms
- PEB: Margaritaville HMA terminable Sep 2026 (month); franchise Jul 2028; mortgage repaid Feb 2026 (capital≠conversion); Westin Copley HMA Dec 2028; Embassy SD franchise Jan 2028
- Fibra Inn: 28 MX hotels Aimbridge → Servicios Hoteleros FINN; portfolio completion **2026-01-15**

## CALA
- Mexico: Fibra Inn 28-hotel Spanish IR press + governance Aimbridge authorization
- Portuguese/CVM: staged (no hotel-named persist this wave)

## Verified dates / notice windows
- CLDT portfolio / Holtsville: IHM 90-day non-renewal; 6-month sale exit (term-end day unknown)
- Margaritaville Hollywood Beach: HMA terminable at will from Sep 2026 (MONTH)
- Westin Copley Place: Non-terminable HMA expires Dec 2028 (MONTH)
- Embassy Suites SD Bay: Franchise expires Jan 2028 (MONTH)
- Fibra Inn 28: Operator internalization completed 2026-01-15 (EXACT_DAY_PORTFOLIO)
- CLDT Holtsville franchise: Franchise expiration year 2026 (YEAR_ONLY)

## Source URLs
- CLDT_10K: https://www.sec.gov/Archives/edgar/data/1476045/000143774926006103/cldt20251231_10k.htm
- PEB_10K: https://www.sec.gov/Archives/edgar/data/1474098/000147409826000015/peb-20251231.htm
- PEB_10Q_Q3: https://www.sec.gov/Archives/edgar/data/1474098/000147409826000072/peb-20260930.htm
- PEB_IR_JULY_2026: https://uk.marketscreener.com/news/pebblebrook-hotel-trust-investor-presentation-july-2026-low-resolution-ce7f51d3dc8ef126
- FIBRA_INN_START: https://fibrainn.mx/storage/docs/fibra-inn-internaliza-la-operacion-en-28-de-sus-hoteles.pdf
- FIBRA_INN_DONE: https://fibrainn.mx/storage/docs/fibra-inn-finaliza-proceso-de-internalizacion-de-la-operacion-en-28-hoteles.pdf
- FIBRA_GOV_2025: https://fibrainn.mx/assets/docs/2025-corporate-governance-letters-updated_69d5ae218a163.pdf

## Unresolved
- CLDT franchise month/day and franchise notice windows
- PEB Margaritaville HMA manager identity and termination fee
- Fibra Inn per-hotel transition dates within Nov 2025–Jan 2026
- Aimbridge original HMA instrument text not public
- CVM hotel-named contract schedules still thin

## Track A handoff
- 38 research candidates / decision windows
- No auto-Verified conversion classifications
- Capital repayment explicitly tagged DEBT_MATURITY_NOT_CONVERSION

## Track C
- 2 identity issues (dual Holiday Inn Express Toluca room counts)

## Airtable persistence / read-back
- Dry-run: creates=230, noops=6, failures=0 (inventory 62, findings 67, contracts 66, milestones 28)
- Apply: creates=232, noops=10, failures=0, staged=11 (Graph Evidence skipped for unmatched)
- Idempotent re-apply: creates=0, noops=242, failures=0
- Ledgers: `PERSISTENCE_LEDGER_DRY_RUN.json`, `PERSISTENCE_LEDGER_APPLY.json`
- No new schema fields or enums invented
- CUSTOMER_BLOCKED preserved on writes

## Tests
- `npm run test:hif-track-b-international-source-expansion-v1` — PASS (67 dossiers; year-only Holtsville; capital blocked from contracts; 28 Fibra Inn)

## Track A handoff
- Decision windows / research candidates only — **no auto-Verified conversion**
- Holtsville franchise year 2026 + IHM related-party limitation
- Margaritaville Hollywood HMA terminable Sep 2026 (month); franchise Jul 2028 separate; mortgage repaid = capital only
- Westin Copley non-terminable HMA Dec 2028; Embassy SD franchise Jan 2028
- Fibra Inn 28: completed operator internalization 2026-01-15 (Aimbridge → Servicios Hoteleros FINN) — portfolio operator change, not brand conversion
- Artifacts: `TRACK_A_HANDOFF.json`

## Track C identity requirements
- Dual **Holiday Inn Express Toluca** (268 vs 127 rooms) need address / census disambiguation
- Artifact: `TRACK_C_HANDOFF.json`

## Integration Controller handoff
- Open PR from `feat/hif-v6-track-b-international-source-expansion-v1` (successor to open #549 V3 tip)
- Do not merge from Track B chat
- Preserve dirty `tmp/*` including `tmp/ic-fold-519-audit.mjs`
- After merge: post-merge sync for registered HIF Track B workspace when IC completes cycle
