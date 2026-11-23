# GDI Design Hotels Operational Pilot V1.1 — Founder Report

**Date:** 2026-11-23  
**PR:** [#571](https://github.com/joandejarden-create/my-operators-backend/pull/571) (update)  
**Status:** Controlled preparation — **STOP** before paid research / publication  
**Change from V1:** Preferred Hotel 3 = Memmo Príncipe Real (Lisbon). **No verified Dealality HPC ID** → no speculative substitution; Good Hotel Antigua retained as **interim** Hotel 3 only (HPC not deleted).

---

## 1. Verified European replacement and HPC ID

### Preferred: Memmo Príncipe Real — Lisbon, Portugal

| Field | Evidence |
|---|---|
| Official name | Memmo Príncipe Real |
| Design Hotels | YES — [designhotels.com](https://www.designhotels.com/hotels/portugal/lisbon/memmo-principe-real/) |
| Marriott / Design Hotels | YES — MARSHA **LISDM** — [marriott.com](https://www.marriott.com/en-us/hotels/lisdm-memmo-principe-real-lisbon-a-member-of-design-hotels/overview/) |
| Rooms | **41** (official Design Hotels / Marriott) |
| Meetings | Boutique / limited meetings (no multi-event conference plant advertised) |
| Dealality HPC ID | **`null` — NOT FOUND** |
| Local Design Hotels census index | CALA-only (40 matched) — Europe not covered |
| Live HPC API lookup | Attempted read-only → **403 NOT_AUTHORIZED** with available credentials |

**Do not invent an HPC ID. Do not create a duplicate HPC in this pilot.**

### European alternatives with verified existing HPC IDs

**None found** in accessible Dealality inventory (local Design Hotels index + GDI hotel profiles + unauthorized HPC API).

Official Design Hotels Europe properties (e.g. Memmo Alfama, Hotel Hotel Lisbon, Sir Victor Barcelona) are **census-enrollment candidates only** — not cohort replacements without verified HPC IDs.

**Required next step:** Steward enroll Memmo Príncipe Real (LISDM) into Hotel Property Census, return `rec…` ID, then swap interim Antigua → Memmo in the runnable hotels file.

---

## 2. Final three-hotel cohort

### Target (preferred — Europe slot incomplete)

| # | Hotel | HPC | Posture |
|---|---|---|---|
| 1 | Condesa DF Hotel | `rece0NdV9FCnaFlVS` | Urban / meetings |
| 2 | LaLuna Boutique Hotels & Villas | `recaokE8psIbuVeuH` | Leisure / destination |
| 3 | **Memmo Príncipe Real** | **PENDING** | European boutique / limited meetings |

### Runnable interim (for $0 Preview/Rehearsal)

| # | Hotel | HPC | Role |
|---|---|---|---|
| 1 | Condesa DF | `rece0NdV9FCnaFlVS` | Retained |
| 2 | LaLuna | `recaokE8psIbuVeuH` | Retained |
| 3 | Good Hotel Antigua | `recGL1epYhEle5SDM` | **INTERIM** pending Memmo HPC |

Antigua remains in census / research associations — only the pilot *slot* is interim.

---

## 3. Design Hotels affiliation verification

| Hotel | Affiliation | Confidence |
|---|---|---|
| Condesa DF | Design Hotels | HIGH (CALA census + MARSHA MEXDS) |
| LaLuna | Design Hotels | HIGH (CALA census + MARSHA GNDDS) |
| Good Hotel Antigua (interim) | Design Hotels | HIGH (CALA census + MARSHA GUAGH) |
| Memmo Príncipe Real (target) | Design Hotels | HIGH_OFFICIAL (DH + Marriott LISDM) — HPC pending |

---

## 4–5. Updated preflight + Preview/Rehearsal

Runnable cohort: **3/3** `NEW_HOTEL`, `readyForDryRun=true`, `$0`.

```powershell
.\scripts\Invoke-GdiPortfolioProcessing.ps1 -Mode Preview -HotelsFile reports\gdi-design-hotels-operational-pilot-v1\design-hotels-pilot-cohort-hotels.txt
.\scripts\Invoke-GdiPortfolioProcessing.ps1 -Mode Rehearsal -HotelsFile reports\gdi-design-hotels-operational-pilot-v1\design-hotels-pilot-cohort-hotels.txt
```

**Executed 2026-11-23 (this update):**

| Mode | Run ID | Result |
|---|---|---|
| Preview | `gdi_portfolio_20261123124829_8716c0` | `passedCount=3`, `spentUsd=0`, `LIMITED_PILOT_ONLY` |
| Rehearsal | `gdi_portfolio_20261123124829_ad4254` | `passedCount=3`, `spentUsd=0`, `LIMITED_PILOT_ONLY` |

Preflight tests: **5/5 PASS** (incl. `memmo_europe_target_has_no_invented_hpc`).  
Europe Memmo slot: **not** in hotels.txt until HPC verified.

---

## 6. Configuration gaps

| Gap | Action |
|---|---|
| Memmo HPC missing | Steward census enrollment (LISDM) — **blocker for Europe cohort** |
| HPC API 403 in this environment | Use Integration Controller credentials / founder-authorized census read |
| Rooms unknown for CALA interim hotels in local plans | Do not invent; Memmo rooms=41 from official sources only |
| No local GDI artifacts for any runnable hotel | Expected `NEW_HOTEL` |
| #564/#566/#567/#570 still OPEN | Prefer IC merge before paid |

---

## 7–8. Exact first paid research plan (NOT EXECUTED)

| Hotel | Priority segments (examples) | Proposed budget | Deep-research caps |
|---|---|---|---|
| Condesa DF | corporate_meetings_incentives, associations_conferences_events, corporate_accounts_scaling, government_public_sector, projects_infrastructure | **$15–20** | MaxCandidates 8; deep ≤3 high-confidence |
| LaLuna | travel_trade_group_intermediaries, sports_entertainment, social_special_events, corporate_celebrations_private_events | **$12–18** | MaxCandidates 8; deep ≤3 |
| Hotel 3 (Antigua interim **or** Memmo after HPC) | corporate_meetings_incentives, travel_trade, social_special_events, preferred_lodging_partnerships | **$12–18** | MaxCandidates 8; deep ≤3 |

- **Aggregate ceiling:** **$60** portfolio (`-BudgetUSD 60`)  
- Informed by V1.1 Bethesda/YOTEL **$2** pilot economics — that commercial yield was **not** certified  
- Public-source contacts only; Surfe separate; no invented emails  
- Competitor commitments → Competitive Market Scope (existing classifier; #570 preferred merged)  
- Stop if budget hit, affiliation uncertain, or quality gates fail  
- Target 10–15 prospects **only if earned**

Paid command (after GO + preferably after Memmo HPC swap):

```powershell
.\scripts\Invoke-GdiPortfolioProcessing.ps1 `
  -Mode FullE2E `
  -HotelsFile reports\gdi-design-hotels-operational-pilot-v1\design-hotels-pilot-cohort-hotels.txt `
  -BudgetUSD 60 `
  -AuthorizePaidResearch `
  -AllowLivePilot
```

---

## 9. Commercial certification requirements

Reuse existing GDI standards: credible future lodging/meeting demand; no RFP required; Hotel/Competitive scopes; no invented Sales Ready; customer publication separate; execution complete ≠ commercially certified.

---

## 10. Repeatable operating commands

| Mode | Command |
|---|---|
| Single preview | `-HotelId <rec> -Mode Preview` |
| Three-hotel preview | `-Mode Preview -HotelsFile …\design-hotels-pilot-cohort-hotels.txt` |
| Rehearsal $0 | `-Mode Rehearsal -HotelsFile …` |
| Resume | `-Mode Resume -ResumeState <state.json> -HotelsFile …` |
| Paid | FullE2E + `-BudgetUSD` + `-AuthorizePaidResearch` + `-AllowLivePilot` |
| Zero-paid replay | Preview/Rehearsal without live flags |

See `SCALING_RUNBOOK.md`.

---

## 11. PR / CI status

| PR | Status |
|---|---|
| **#571** Design Hotels operational pilot | OPEN — CI green on prior push; this V1.1 update |
| #564 / #566 / #567 / #570 | OPEN (do not duplicate) |
| #562 Portfolio PowerShell | MERGED |
| #224 Market Scope | MERGED |

---

## 12. Outstanding authorizations

1. **Census steward:** enroll Memmo Príncipe Real → return HPC `rec…`  
2. Founder confirm Europe hotel (Memmo vs wait)  
3. Founder budget ceiling for paid FullE2E  
4. IC merge #564/#566/#567/#570 before paid (recommended)  
5. Separate GO for Airtable research-inventory apply  
6. Separate GO for customer publication  

---

## Engine readiness

| Capability | Ready? |
|---|---|
| $0 Preview/Rehearsal on interim Mexico/Grenada/Antigua | **YES** |
| Europe Memmo in runnable cohort | **NO — HPC blocker** |
| Paid research | **NO — founder GO required** |

**STOP for founder review before spending or publishing.**
