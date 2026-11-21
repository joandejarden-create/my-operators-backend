# HIF Track A — CB2 Commercial Opportunity Discovery (Resume)

**As of:** 2026-11-21  
**Publication:** CUSTOMER_BLOCKED  
**Research spend:** **$0**  
**Branch:** `feat/hif-v6-track-a-cb2-commercial-discovery`  
**Baseline:** `origin/main` `9cce68f0f` (includes merged PR **#541** + Track C #542)  
**E2E classification:** **PERSISTENCE_VALIDATED** (when apply + idempotent read-back succeed) — not FULL_E2E / not CUSTOMER_PUBLISHED

---

## State verification (before research)

| Item | Result |
|------|--------|
| Prior PR #541 | **MERGED** 2026-11-21 · merge `95cda99a3` |
| Prior feature branch | Preserved history; **new** CB2 branch from `origin/main` |
| Duplicate research | Avoided re-creating #541 inventory/findings (Lakeville/Trinidad/Gideon/Pueblo/etc. = NOOP or carry) |
| Uncommitted at start | Clean |

---

## Status re-checks (mandatory)

| Hotel | Prior | CB2 result |
|-------|-------|------------|
| **Lakeville Inn** (US) | First-100 BCT | **STILL OPEN** — no brand announcement found |
| **Hilton Trinidad** (TT) | First-100 MCT | **STILL OPEN** — HMA + HotelTT EOI active; no buyer/operator award |

---

## 1. Hotels researched

**22** distinct hotels/sites investigated (US, CA, MX, BR, GB, TT, PT, ES, TC).

## 2. New opportunities discovered

| Opportunity | Type | Status |
|-------------|------|--------|
| **Legacy Hotel, Green Bay, WI** | **BCT** (Marriott soft brand planned; collection unresolved) | **NEW first-100** |
| Hotel do Mel – Casa da Costeira, Góis, PT | Operating agreement | OPEN (due 2026-11-27) |
| Jericho Beach Hostel, Vancouver, CA | Operator lease | OPEN (due 2026-11-25) |
| Hotel Torre de Uriz, Navarra, ES | Concession | Award pending (closed 2026-11-19) |
| White Sands Beach Hotel, Grand Turk, TC | Lease/redevelopment/ops | Award pending |
| Dorset 61-property portfolio | MCT portfolio | Cabinet award **2026-12-08** |

## 3. First-100 BCT/MCT

| Metric | Count |
|--------|------:|
| Open BCT | **2** (Lakeville + Legacy Green Bay) |
| Open MCT | **1** (Hilton Trinidad) |
| **First-100** | **3 / 100** (Δ +1) |
| Commercially relevant / accessible | **3 / 3** |
| Outreach-ready | **0** |

### Legacy Hotel — Green Bay, WI — PRIORITY A BCT

- Dreamscape assumed management (Oct 2026); ownership publicly plans **Marriott soft-brand** conversion  
- Soft-brand **collection not named** → brand selection still open (agreement type unresolved soft-brand license; opportunity type BCT)  
- Private pathway; ACCESSIBLE_PROBABLE via operator/owner  
- Sources: Hotel Management 2026-10-22; Hospitality Net 2026-10-26

## 4–7. Expanded pipeline · relevance/access · matrix · public/private

See `MATRIX_COUNTRY_TIMING_DEALTYPE_FRESHNESS.json` and `FIRST_100_SCORECARD.json`.

| Pathway | First-100 | Expanded |
|---------|----------:|---------:|
| Private | 2 | — |
| Public | 1 | 2 open + 3 award-watch |

MX remains UNDER_RESEARCHED for new open principals; BR this wave = completed conversions only (Ibis Guaíba→Tri Hotéis).

## 8–9. Airtable

| Action | Result |
|--------|--------|
| Creates | **10** (5 inventory + 5 findings) |
| No-ops (first apply) | **4** (Lakeville + Hilton Trinidad inventory/findings) |
| Idempotent retry | **14** no-ops |
| Conflicts | **0** |
| HPC / schema | **0** |
| Publication | CUSTOMER_BLOCKED |

Created: Legacy inv `rec3XVviNYdBxEBIN` / find `recAVM0frtNV6tzLJ` · Hotel do Mel `recw9q2EE7YHHRETd` / `recxdcXwV1mS2DSTV` · Jericho `recUARaWN7p2NUQXt` / `rec3MeA4RHaaqIdvy` · Torre Uriz `recJo3fWX6tylsDbB` / `reczOEqCNKky5yrOe` · White Sands `recW4MH52uuw5hvb1` / `recZ1A0a8HCn3pHlk`

Ledgers: `PERSISTENCE_LEDGER_DRY_RUN.json` · `PERSISTENCE_LEDGER_FIRST_APPLY.json` · `PERSISTENCE_LEDGER_IDEMPOTENT.json`

## 10. Cost, tests, PR

| Item | Value |
|------|-------|
| Cost | **$0** |
| Tests | `test:hif-track-a-continuous-foundation` · `test:hif-foundation-persistence` · `test:hif-v7-commercial-dimensions` · CB2 scorecard asserts |
| PR | For Integration Controller only — **do not merge from Track A** |

**CUSTOMER_BLOCKED**
