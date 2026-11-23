# GDI Design Hotels — Scaling Runbook V1 / V1.1

**Objective:** Process another Design Hotels property as an operational task, not a custom software project.

```
SELECT HOTEL → PREFLIGHT → AUTHORIZE BUDGET → RUN GDI → VERIFY QUALITY → SAVE APPROVED BASELINE
```

**V1.2 Europe note:** Hotel 3 = Memmo Príncipe Real HPC `rec1t35e3sPerzxYu` (MARSHA `LISDM`, 41 rooms, Lisbon). Antigua removed from this cohort only. Operational $0 success ≠ commercial quality certification.

## Prerequisites

- Canonical HPC record ID (`rec…`) — **required**; enrollment first for Europe properties missing from census
- Design Hotels affiliation confirmed (or flagged)
- Repo checkout with Portfolio Processing Certification V1 (`Invoke-GdiPortfolioProcessing.ps1`)
- Node on PATH
- Founder budget authorization **before** any paid flags

## One hotel ($0 preflight)

```powershell
.\scripts\Invoke-GdiPortfolioProcessing.ps1 `
  -HotelId recXXXXXXXX `
  -Mode Preview
```

Optional operational preflight package:

```powershell
npm run gdi:design-hotels-operational-pilot-v1
node --input-type=module -e "import { runHotelOperationalPreflight } from './lib/group-demand-intelligence/workflow/gdi-hotel-operational-preflight-v1.js'; console.log(JSON.stringify(runHotelOperationalPreflight('recXXXXXXXX'),null,2))"
```

## Three hotels (this pilot)

```powershell
.\scripts\Invoke-GdiPortfolioProcessing.ps1 `
  -Mode Preview `
  -HotelsFile reports\gdi-design-hotels-operational-pilot-v1\design-hotels-pilot-cohort-hotels.txt

.\scripts\Invoke-GdiPortfolioProcessing.ps1 `
  -Mode Rehearsal `
  -HotelsFile reports\gdi-design-hotels-operational-pilot-v1\design-hotels-pilot-cohort-hotels.txt
```

Same command for all hotels — no property-specific scripts.

## Ten hotels / larger portfolio

1. Build `hotels.txt` (one HPC ID per line).
2. Run `-Mode Preview` — fix `CONFIGURATION_INCOMPLETE` / affiliation flags.
3. Run `-Mode Rehearsal` ($0) — confirm resume/state files.
4. Founder approves aggregate `-BudgetUSD` and hotel list.
5. Only then:

```powershell
.\scripts\Invoke-GdiPortfolioProcessing.ps1 `
  -Mode FullE2E `
  -HotelsFile path\to\hotels.txt `
  -BudgetUSD 100 `
  -AuthorizePaidResearch `
  -AllowLivePilot
```

Publication and Airtable production writes remain **independent** switches and require separate GO.

## Resume / retry

```powershell
.\scripts\Invoke-GdiPortfolioProcessing.ps1 `
  -Mode Resume `
  -ResumeState reports\gdi-portfolio-processing-certification-v1\<run>.state.json `
  -HotelsFile path\to\hotels.txt
```

Failed hotels do not invalidate successful peers.

## Zero-paid rerun

Rehearsal / Preview / fixture paths must not duplicate paid spend. Re-run with `$0` and without `-AllowLivePilot`.

## Certification checks (per hotel)

- Stable opportunity IDs / no duplicates
- Hotel vs Competitive Market Scope correct when competitor fields present
- Budget ledger respected
- No invented contacts / no provider-restricted storage
- Customer publication still unauthorized unless separately approved
- Distinguish **execution complete** vs **commercially certified**

## Exception handling

| Blocker | Action |
|---|---|
| Invalid HPC ID | Stop hotel; continue peers |
| Preferred hotel lacks HPC (e.g. Memmo LISDM) | Enroll in census; keep interim Hotel 3 for $0 only; **no speculative Europe swap** |
| Affiliation uncertain | Flag for founder; do not invent brand |
| Rooms unknown | Proceed (do not invent); enrich later |
| No meeting facilities | **Eligible** — prioritize room-night segments |
| Paid auth missing | Stay on Preview/Rehearsal |
| Partial research | `RESEARCH_CONTINUATION` — resume, do not reset IDs |

## Exact command matrix (V1.1)

| Intent | Command |
|---|---|
| Single hotel dry-run | `.\scripts\Invoke-GdiPortfolioProcessing.ps1 -HotelId <rec> -Mode Preview` |
| Three-hotel Preview | `-Mode Preview -HotelsFile reports\gdi-design-hotels-operational-pilot-v1\design-hotels-pilot-cohort-hotels.txt` |
| Three-hotel Rehearsal ($0) | `-Mode Rehearsal -HotelsFile …\design-hotels-pilot-cohort-hotels.txt` |
| Resume | `-Mode Resume -ResumeState <state.json> -HotelsFile …` |
| Authorized paid | `-Mode FullE2E -HotelsFile … -BudgetUSD <N> -AuthorizePaidResearch -AllowLivePilot` |
| Zero-paid replay | Preview/Rehearsal without live/paid flags |
| Certification package | `npm run gdi:design-hotels-operational-pilot-v1` (or `node scripts/gdi-design-hotels-operational-pilot-v1.mjs`) |

**Operational execution success** = Preview/Rehearsal pass, $0, stable IDs.  
**Commercial quality certification** = separate founder gate after paid outputs meet quality gates (see `paid-research-plan-v1-1.json`).

## Weekly Change

Do not activate weekly automation in this pilot. Annotate Weekly Change readiness only after certified baseline exists.
