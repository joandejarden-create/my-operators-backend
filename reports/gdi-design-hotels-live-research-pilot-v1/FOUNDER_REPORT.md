# GDI Design Hotels Live Research Pilot V1 — Founder Report

**Date:** 2026-11-23  
**Authorization:** FOUNDER GO — max **$12** aggregate / **$4** per hotel (Exa only)  
**Portfolio run:** `gdi_dh_live_20261123141330`  
**Actual spend:** **$3.20** of $12 (did not force full budget)  
**Status:** Controlled paid research complete — **STOP** before more spend / Airtable apply / publication

---

## Phase 1 — Execution preflight (PASS)

| Gate | Result |
|---|---|
| PR #564 / #566 / #567 / #570 / #571 on `main` | **MERGED** (all five) |
| Canonical HPC IDs | Condesa `rece0NdV9FCnaFlVS` · LaLuna `recaokE8psIbuVeuH` · Memmo `rec1t35e3sPerzxYu` |
| Existing GDI inventories | Empty before run (0 local opportunities) — no duplicate overwrite |
| Design Hotels affiliation | Verified via configs + prior V1.2 enrollment evidence |
| Hotel profiles | Seeded GDI configs (required — prior $0 attempt blocked on `HOTEL_PROFILE_UNRESOLVED`) |
| PowerShell → canonical Full E2E | `DesignHotelsLiveExecute` → `gdi-design-hotels-live-research-pilot-v1.mjs` → `executeHotelPaidPilot` → `runCanonicalFullE2eResearch` |
| Paid auth ≠ publication | `authorizePublish=false`, Airtable apply=false |
| Dual budget gates | Durable ledgers per hotel ($4) + aggregate ($12) |
| Retry / resume | Durable ledgers; zero-paid replay added **$0** |
| Surfe / Findymail / PDL / FullEnrich | **Off** |

**Blocker fixed before spend:** first execute-paid attempt (earlier same day) completed at **$0** because discovery stopped on unresolved hotel profiles. Configs + cleared blocked durable state enabled live Exa.

---

## Comparative three-hotel scorecard

| Metric | Condesa DF | LaLuna | Memmo Príncipe Real | Portfolio |
|---|---:|---:|---:|---:|
| Paid spend (USD) | **1.20** | **1.00** | **1.00** | **3.20** |
| Remaining of $4 hotel cap | 2.80 | 3.00 | 3.00 | 8.80 of $12 |
| Discovery candidates extracted | 87 | 66 | 73* | 226 |
| Fresh discovery cells | 8 | 7 | 7 | 22 |
| Deep-selected | 4 | 4 | 4 | 12 |
| Credible future demand (deep set) | 4 OPEN | 4 OPEN | 4 OPEN | 12 |
| Hotel-scope (unknown sourcing) | 4 | 4 | 4 | 12 |
| Competitive (verified competitor commit) | 0 | 0 | 0 | 0 |
| Sales Ready | **0** | **0** | **0** | **0** |
| Research continuation / watchlist | 4 | 4 | 4 | 12 |
| Named buyer roles (deep set) | 3 (Salesforce) | 0 verified persons | 2 (TDCX) | 5 |
| Org entry points only | 1+ | 4 | 2 | — |
| Cost / deep-selected opp | $0.30 | $0.25 | $0.25 | $0.27 |
| Cost / Sales Ready | n/a | n/a | n/a | n/a |
| Airtable writes | 0 | 0 | 0 | 0 |
| Publication | none | none | none | none |

\*Memmo economics `candidatesDiscovered=61` vs matrix extracted 73 — economics uses post-dedupe commercial set; matrix counts raw extracted cells.

---

## Hotel intelligence packages (deep-selected)

### 1) Condesa DF — Mexico City (`rece0NdV9FCnaFlVS`) — $1.20

| ID | Opportunity | Org | Notes |
|---|---|---|---|
| `disc_0ca4947f4e8e` | WAPOR 2026 Annual Conference | World Association for Public Opinion Research | OPEN; org desk only |
| `disc_1342860c1693` | Mexico City office / GDC expansion | Salesforce | Strongest package — named buyers (travel/meetings procurement + Mexico workplace); 12 investigated URLs |
| `disc_2017a6cef8cd` | IAFMHS 2027 Mexico City Conference | IAFMHS | OPEN; buyer not verified |
| `disc_227427613732` | SAH 79th Annual International Conference 2026 | Society of Architectural Historians | OPEN; buyer not verified |

**Recommended sales actions:** Prioritize Salesforce Mexico City workplace / travel-procurement path; continue WAPOR/IAFMHS/SAH housing accessibility research. Do not claim Sales Ready.

### 2) LaLuna — Grenada (`recaokE8psIbuVeuH`) — $1.00

| ID | Opportunity | Org | Notes |
|---|---|---|---|
| `disc_a6def3588729` | CARPHA 71st Annual Health Research Conference 2027 | CARPHA | OPEN |
| `disc_023923963f08` | CARPHA 71st (duplicate segment path) | CARPHA | Near-duplicate — reconcile |
| `disc_f8938e28ec56` | Annual Regional CTLPA Conference 2027 | CTLPA | OPEN |
| `disc_db8429d00434` | Pure Grenada Sailing Week 2027 | Grenada Sailing Week Board | Destination leisure fit; buyer not verified |

**Recommended sales actions:** Deduplicate CARPHA; pursue Sailing Week lodging partners / DMC intermediaries; verify CARPHA/CTLPA housing channels.

### 3) Memmo Príncipe Real — Lisbon (`rec1t35e3sPerzxYu`) — $1.00

| ID | Opportunity | Org | Notes |
|---|---|---|---|
| `disc_8e730047c1b9` | ESICM LIVES 2026 Annual Congress | ESICM | OPEN; may be large-congress overflow only for 41-key boutique |
| `disc_62b0361d0b7e` | Lisbon campus opening / 2027 expansion | TDCX | Strongest — named Portugal/EMEA ops leaders; 10 investigated URLs |
| `disc_9cd2c082c05d` | Advantage Travel Partnership Annual Conference 2027 | Advantage Travel | OPEN |
| `disc_51073448c775` | Advantage Travel (duplicate segment path) | Advantage Travel | Near-duplicate — reconcile |

**Recommended sales actions:** Prioritize TDCX visiting-team / training lodging; qualify ESICM as overflow-only; dedupe Advantage Travel.

Full structured package: `HOTEL_INTELLIGENCE_PACKAGES.json`.

---

## Deep research effectiveness

| Observation | Detail |
|---|---|
| Provider | Exa Agent (medium), ledger unit **$0.10** |
| Discovery ops | ~22 fresh cells billed (~$2.2) |
| Deep ops | 12 × $0.10 = **$1.20** |
| Meaningful evidence lift | Clear on **Salesforce** (Condesa) and **TDCX** (Memmo) — investigated URLs + accepted claims |
| Weak / thin deep passes | Several association events retained seed-only evidence (`investigatedUrlCount=0`) — treat as **NO_IMPROVEMENT / thin incremental** despite successful provider call |
| Lesson | Provider call success ≠ commercial research success |

---

## Market Scope / reconciliation

- Intake dispositions: all deep-selected **OPEN** with **UNKNOWN** sourcing → classify as **Hotel** population (not Competitive).
- Verified competitor commitments: **0**.
- Reconciled cycle treatment (partial report): **WATCHLIST_RESEARCH** ×12.
- Sales Ready: **0** (canonical eligibility not met — honest).
- Near-duplicates flagged: CARPHA ×2 (LaLuna), Advantage Travel ×2 (Memmo).
- Local `opportunities.json` store not written (persistence apply unauthorized) — opportunity IDs durable in `opportunity-research-state/` + run reports.

---

## Zero-paid replay

```
Mode=DesignHotelsLiveReplay → additionalSpendUsd=0, noAdditionalPaidCalls=true
Ledgers unchanged: Condesa 1.2 / LaLuna 1.0 / Memmo 1.0
```

---

## Technical notes / gaps

1. Hotel GDI configs required for Design Hotels HPC rows (legacy field schema + empty local profiles).
2. `hpcIdentityFields` extended to map legacy `name`/`city`/`country`/`Affiliation`.
3. PowerShell modes `DesignHotelsLivePreflight|Execute|Replay` added to `Invoke-GdiPortfolioProcessing.ps1`.
4. Dirty `ws-gdi` contact-intel work preserved (isolated temp worktree used).
5. Commercial quality gaps: Sales Ready 0; thin deep passes; segment-path duplicates; large congress vs 41-key boutique fit risk (ESICM).

---

## Next recommended research actions (NOT authorized yet)

1. Targeted Exa deep continuation on Salesforce + TDCX only (~$0.40–$0.80).
2. Public-source contact verification for named buyers (no Surfe).
3. Deduplicate CARPHA / Advantage Travel IDs before any persistence.
4. Separate founder GO for research-inventory Airtable apply.
5. Separate founder GO for customer publication.

---

## PR / integration

Focused certification PR: **#574** — https://github.com/joandejarden-create/my-operators-backend/pull/574  
(configs + runner wiring + evidence reports/ledgers). Integration Controller owns merge.

**Final state:** `AWAITING_INTEGRATION_CONTROLLER`

**STOP** — no additional paid research, Airtable persistence, customer publication, portfolio expansion, or weekly automation.
