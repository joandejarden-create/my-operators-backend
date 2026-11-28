# FOUNDER CLOSEOUT — Close Outstanding GDI Full E2E Certification Gaps

**Status:** `PARTIAL — INTEGRATED_ON_8080 — AWAITING_FOUNDER_REVIEW`  
**Not claimed:** full operational E2E certification  
**UI freeze:** preserved (`GDI_CANONICAL_HOTEL_TEMPLATE_V1`)  
**Expenditure this cycle:** Findymail 0 · Exa $0 · Surfe 0

## Accepted (this assignment)

- PR #663 implementation progress (founder accepted)
- `GDI_CANONICAL_HOTEL_TEMPLATE_V1` permanent
- `GDI_CONTACT_RESEARCH_BEST_EFFORT_V1` permanent
- Full operational E2E certification **not** accepted yet — five gaps addressed below

## 1. Unified V3 Live Orchestrator — CLOSED (wiring)

`runFullE2eV3Live` is implemented. Generic stage CLI no longer exits 3 on Live.

- Hotel-agnostic; no Wyndham Live defaults
- Fail-closed without `-AuthorizePaidResearch` / WHO auth params
- Discovery/Deep = reuse existing artifacts (HOLD if missing)
- WHO/HOW = existing Exa adapter (Checkpoint A params)
- Contact + Findymail when authorized; Surfe never
- STAGED activate on Live; Airtable persistence separately authorized
- Tests: `npm run test:gdi-full-e2e-v3-live-orchestrator-v1`

**Remaining for Live operational proof:** one authorized-spend hotel run (see budget).

## 2. Contact gap reconciliation

| Metric | Before | After evidence recovery |
|---|---:|---:|
| Email NOT_YET | 4 | 1 |
| Telephone NOT_YET | 14 | 14 |
| LinkedIn NOT_YET | 12 | 12 |
| Findymail eligible remaining | 0* | 1 |
| Authority annotations missing | 26 | 0 |

\*Eligible was 0 because 3 email-gap people were `ALREADY_IN_LEDGER` (prior Findymail no-match not hydrated onto package flags) and Gutierrez failed identity regex on parentheses/credentials. Ledger hydration recovered 3; identity fix makes Gutierrez Findymail-eligible (1 credit max; **not spent**).

## 3. Buying-authority annotation — COMPLETED (in-memory / isolated)

All 26 primaries annotated. Title alone never upgrades to verified lodging DM. Result: 0 verified direct lodging decision-makers; 25 `BUYING_AUTHORITY_NOT_VERIFIED`; 1 identity-requires-research (`Suzanne K.`). Setting `requiresAlternativeEntryPoints` surfaces 12 opportunities still missing named alternative-entry research (honest gap → budget).

**Persistence to Airtable + STAGED rematerialize:** outstanding (requires founder write authorization).

## 4. Airtable field-level read-back

- 26/26 STAGED IDs present in Airtable
- Name / email / phone parity PASS vs STAGED
- Authority annotations **not yet** in Airtable or STAGED package fields
- Isolated reconstruction written under `reports/gdi-close-e2e-cert-gaps-v1/_isolated_reconstruction/` (STAGED pointer unchanged)

## 5. Operational repeatability

- A–G DryRun path: certified (prior + this cycle)
- Second hotel DryRun portability: `recLuxvwwxID7U2B8` PASS/PARTIAL (no paid)
- Live fail-closed: proven without spend
- Live authorized-spend proof: **not executed**

## Consolidated founder authorization needed

1. **Findymail:** ≤1 credit — Alejandra Gutierrez (query name `Alejandra Gutierrez`)
2. **Exa public search:** ~$0.25–$0.57 planning estimate — LinkedIn (12), telephone (14), alt-entry research (12 opps)
3. **Airtable write-set:** persist `buyingAuthorityStatus` + `contactChannelResearch` flags + relationships; then isolated→STAGED rematerialize (no CURRENT)
4. Optional: **hotel-agnostic Live Full E2E proof** ceiling ~$25 + Findymail ≤15 on a non-Wyndham hotel

Exact line items: `03-CONSOLIDATED_INCREMENTAL_RESEARCH_BUDGET_REQUEST.json`

## Exact commands (runbook)

See `docs/ai-build-system/GDI_CANONICAL_POWERSHELL_OPERATIONS_RUNBOOK_V1.md`.
