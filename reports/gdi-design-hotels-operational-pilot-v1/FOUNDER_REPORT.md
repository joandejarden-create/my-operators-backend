# GDI Design Hotels Operational Pilot V1 — Founder Report

**Date:** 2026-11-23  
**Status:** Operational preparation complete — **STOP** for founder approval of the exact three hotels + research budget before paid execution  
**Worktree:** isolated clean from `origin/main` (dirty `ws-gdi` preserved)

---

## 1. Three Design Hotels candidates (HPC IDs)

| # | Posture | Hotel | HPC ID | Country / city | MARSHA | Meeting inventory |
|---|---|---|---|---|---|---|
| 1 | Urban / meetings | **Condesa DF Hotel** | `rece0NdV9FCnaFlVS` | Mexico / Mexico City | MEXDS | 7 event spaces |
| 2 | Leisure / destination | **LaLuna Boutique Hotels & Villas** | `recaokE8psIbuVeuH` | Grenada / St. George's | GNDDS | Leisure villa resort (room-night focus) |
| 3 | Boutique / limited meetings | **Good Hotel Antigua** | `recGL1epYhEle5SDM` | Guatemala / Antigua | GUAGH | Co-working only |

- Affiliation: Design Hotels (HIGH — CALA census sitemap match).  
- Existing GDI local store: **none** → all `NEW_HOTEL`.  
- Room counts: **unknown locally** (not invented).  
- Submarket: Dealality corridor not fully resolved in local corridor plan for Grenada/Antigua; CDMX Condesa historically maps toward Mexico City market.  
- Hotel Explorer intelligence: no hotel-specific HE package referenced in this pass.  
- **No duplicate HPC records created.**

### Affiliation flags (excluded from cohort)

| HPC | Name | Flag |
|---|---|---|
| `recOIzFTFK8vTDTJp` | Tulum, a Tribute Portfolio Hotel | Tribute vs Design Hotels uncertain |
| `recdz5f9TrLZHDVb0` | City Express by Marriott La Paz | Unlikely Design Hotels soft collection |
| `rec2ZOABX5Kz1Ax55` | Banyan Tree Veya Valle de Guadalupe | Brand membership uncertain |

---

## 2. Hotel-level preflight results

All three: `readyForDryRun=true`, `processingClass=NEW_HOTEL`, `publication.authorized=false`, `airtableWritesAuthorized=false`.

| Hotel | Demand mix | Primary segment focus (examples) |
|---|---|---|
| Condesa DF | URBAN_MEETINGS | corporate_meetings_incentives, associations_conferences_events, corporate_accounts_scaling, government_public_sector, projects_infrastructure |
| LaLuna | LEISURE_DESTINATION | travel_trade_group_intermediaries, sports_entertainment, social_special_events, corporate_celebrations_private_events |
| Good Hotel Antigua | BOUTIQUE_LIMITED_MEETINGS | corporate_meetings_incentives, travel_trade, social_special_events — **meetings not required** |

Evidence: `preflight-results.json`, `pilot-package.json`.

Portfolio Preview (same PS command, 3 hotels): **passedCount=3**, `spentUsd=0`, verdict `LIMITED_PILOT_ONLY`.

---

## 3. Exact PowerShell command

```powershell
.\scripts\Invoke-GdiPortfolioProcessing.ps1 `
  -Mode Preview `
  -HotelsFile reports\gdi-design-hotels-operational-pilot-v1\design-hotels-pilot-cohort-hotels.txt
```

Rehearsal ($0):

```powershell
.\scripts\Invoke-GdiPortfolioProcessing.ps1 `
  -Mode Rehearsal `
  -HotelsFile reports\gdi-design-hotels-operational-pilot-v1\design-hotels-pilot-cohort-hotels.txt
```

Paid (NOT authorized yet):

```powershell
.\scripts\Invoke-GdiPortfolioProcessing.ps1 `
  -Mode FullE2E `
  -HotelsFile reports\gdi-design-hotels-operational-pilot-v1\design-hotels-pilot-cohort-hotels.txt `
  -BudgetUSD <N> `
  -AuthorizePaidResearch `
  -AllowLivePilot
```

---

## 4. Configuration gaps

| Gap | Severity | Action |
|---|---|---|
| No local GDI profile / opportunities for any of the three | Expected for NEW_HOTEL | Created on first authorized research run |
| Rooms count unknown locally | Medium | Resolve from census/HPC before Sales Ready claims; do not invent |
| Submarket corridor incomplete for Grenada / Antigua | Low | Use country + city until corridor backfill |
| HE intelligence package not confirmed | Low | Optional enrichment |
| Upstream portfolio cert PRs #564/#566/#567/#570 still OPEN | Process | IC merge before paid recommended |

---

## 5. Existing GDI components reused

- `Invoke-GdiPortfolioProcessing.ps1` + portfolio certification runner (#562 MERGED)
- Canonical Full E2E entry / fourteen-segment discovery universe
- Market Scope All / Hotel / Competitive (existing; not rebuilt)
- Universal contact research contract (V2.x standards)
- Research persistence bridge (dry-run gates)
- Design Hotels CALA census audit + Marriott amenities plans (read-only)

---

## 6. Changes genuinely necessary for repeatability

| Change | Why |
|---|---|
| `gdi-hotel-operational-preflight-v1.js` | Universal NEW/EXISTING/REFRESH/CONTINUATION/INCOMPLETE + demand-mix prioritization; meetings optional |
| Design Hotels pilot package + hotels.txt | Repeatable three-hotel Preview/Rehearsal without property scripts |
| Scaling runbook | SELECT → PREFLIGHT → AUTHORIZE → RUN → VERIFY → SAVE |

No Market Scope redesign. No hotel-specific research scripts.

---

## 7. Projected research cost per hotel

| Mode | Cost |
|---|---|
| Preflight / Preview / Rehearsal | **$0** |
| Proposed first live band (founder authorize) | **~$8–25 / hotel** (depends on MaxCandidates + Exa usage; enforce portfolio ceiling) |
| Contact Surfe / paid enrichment | **Not included** — separate auth |

---

## 8. Proposed first live execution plan

1. Founder approves the three HPC IDs above (or substitutes).  
2. IC merges #564/#566/#567 (and #570 if Competitive mapping desired for competitor findings).  
3. Founder sets portfolio `-BudgetUSD` (suggest start **$40–60** for three hotels).  
4. Run FullE2E with `-AllowLivePilot -AuthorizePaidResearch` only.  
5. Verify quality package (10–15 prospects **only if earned**).  
6. Persistence plan dry-run → separate GO for Airtable research inventory.  
7. No customer publication in this pilot.

---

## 9. Certification criteria

- Same PS command for all hotels  
- No hotel-specific code  
- Stable IDs / no duplicates  
- Hotel vs Competitive classification via existing gates  
- Budget enforcement  
- Credible demand (no RFP required)  
- Contact standards (no invented emails)  
- Consistent report format  
- Resume / zero-paid rerun  
- Weekly Change readiness annotated, not activated  
- Execution complete ≠ commercially certified  

---

## 10. Is the engine ready to process the three hotels?

| Capability | Ready? |
|---|---|
| $0 Preview / Rehearsal / operational preflight | **YES** — demonstrated (3/3 pass, $0) |
| Paid live research | **NOT YET** — needs founder hotel + budget GO; prefer IC merge of #564–#567/#570 first |
| Airtable baseline persistence | **NOT YET** — dry-run plan only |
| Customer publication | **NO** — not authorized |

### Phase 1 baseline (PRs)

| PR | Title | State |
|---|---|---|
| #562 | Portfolio Processing Certification V1 | **MERGED** |
| #224 | Market Scope toggle | **MERGED** |
| #564 | V1.1 paid pilot controls | **OPEN** |
| #566 | V1.2 false-EXCLUDE remediation | **OPEN** |
| #567 | V1.3 evidence integrity | **OPEN** |
| #570 | Competitive Intelligence Integration V1 | **OPEN** |

Canonical PowerShell + Node engine on `main` includes #562. Approved Market Scope taxonomy is on `main`. Paid-pilot control / conversion / evidence / CMI-mapping fixes are **not** all on `main` until IC merges.

---

## Persistence plan (non-destructive)

Per hotel after authorized research:

1. Research discovery artifacts (FS)  
2. Verified evidence  
3. Canonical opportunity fields  
4. Hotel Research Inventory upsert (idempotent; read-back) — **separate GO**  
5. Customer eligibility remains false until publication GO  
6. Market Scope via existing annotate  

No live Airtable writes in this PR.

---

## STOP

Awaiting founder approval of:

1. Exact three hotels (or replacements)  
2. Research budget ceiling  
3. Whether to wait for IC merges of #564/#566/#567/#570 before paid run
