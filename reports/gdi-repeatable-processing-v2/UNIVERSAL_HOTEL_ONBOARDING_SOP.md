# GDI Universal Hotel Onboarding SOP (Repeatable Processing V2)

Routine operational procedure for any verified HPC hotel. **Do not** create hotel-specific research runners. Prefer `Cohort*` modes over historical `DesignHotelsLive*`.

## Procedure

1. **VERIFY HPC** — Confirm the hotel exists as a verified Hotel Property Census record (`rec…`).
2. **GENERATE CONFIG** — Propose GDI config from HPC (auto-propose inside CohortPreflight when config file is absent). Unresolved fields must remain unresolved — no guessing.
3. **PREFLIGHT ($0)** — Run CohortPreflight; confirm `ok: true`, `spentUsd: 0`, publication/Airtable gates fail-closed.
4. **AUTHORIZE BUDGET** — Founder sets `-BudgetUSD` / `-PerHotelBudgetUSD` and paid-research flags only when ready.
5. **EXECUTE** — `CohortExecute` (paid only after explicit authorization).
6. **REVIEW** — Inspect per-hotel reports, dedupe decisions, deep selection, contacts.
7. **PRESERVE** — Keep immutable run artifacts under `reports/`.
8. **AUTHORIZE PERSISTENCE** — Separate gate from paid research; dry-run write-set before any Airtable apply.
9. **REFRESH** — Local Research inventory / authorized surfaces only after persistence authorization.

## Exact commands

### One new hotel (preflight, $0)

```powershell
.\scripts\Invoke-GdiPortfolioProcessing.ps1 `
  -Mode CohortPreflight `
  -HotelId <HPC_RECORD_ID> `
  -BudgetUSD 0 `
  -PerHotelBudgetUSD 0
```

### Cohort from HotelsFile (preflight, $0)

```powershell
.\scripts\Invoke-GdiPortfolioProcessing.ps1 `
  -Mode CohortPreflight `
  -HotelsFile .\path\to\hotels.txt `
  -BudgetUSD 0 `
  -PerHotelBudgetUSD 0
```

`hotels.txt` = one HPC `rec…` id per line.

### Execute / Replay (only after founder budget authorization)

```powershell
.\scripts\Invoke-GdiPortfolioProcessing.ps1 -Mode CohortExecute -HotelsFile .\path\to\hotels.txt -BudgetUSD <N> -PerHotelBudgetUSD <M> -AuthorizePaidResearch -AllowLivePilot
.\scripts\Invoke-GdiPortfolioProcessing.ps1 -Mode CohortReplay -HotelsFile .\path\to\hotels.txt -BudgetUSD 0 -PerHotelBudgetUSD 0
```

### Historical Design Hotels live modes (regression only — not default for new hotels)

```powershell
.\scripts\Invoke-GdiPortfolioProcessing.ps1 -Mode DesignHotelsLivePreflight ...
.\scripts\Invoke-GdiPortfolioProcessing.ps1 -Mode DesignHotelsLiveExecute ...
.\scripts\Invoke-GdiPortfolioProcessing.ps1 -Mode DesignHotelsLiveReplay ...
```

## Guarantees

- Stable opportunity IDs
- Cross-segment dedupe **before** paid deep selection (`gdi-cross-segment-opportunity-dedupe-v2`)
- Aggregate + per-hotel budget caps
- Contact verification distinct from email enrichment
- Existing Hotel / Competitive classification preserved
- No unauthorized publication or Airtable writes
- Sales Ready remains fail-closed

## Fourth-hotel acceptance evidence (2026-11-23)

- Hotel: **AC Hotel A Coruña** — HPC `rec2PVBDavppGpenm` (outside Design Hotels pilot)
- HPC-to-config: `ok: true` with rooms=116, meeting space populated; `demandMix` / `commercialSegmentPriorities` left **UNRESOLVED** (no guessing)
- `$0` CohortPreflight: `ok: true`, `spentUsd: 0`, `executeReady: false` (budget not authorized)
- Artifacts: `reports/gdi-repeatable-processing-v2/fourth-hotel-hpc-to-config/`
