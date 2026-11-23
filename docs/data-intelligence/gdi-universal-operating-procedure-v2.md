# GDI Universal Operating Procedure V2

**Contract:** `gdi-universal-cohort-processing-v2`  
**Status:** Ready for founder-authorized hotel cohorts (not weekly automation; not customer publication)  
**Engine:** Canonical Full E2E via `executeHotelPaidPilot` — no hotel-specific research runners

## Acceptance criterion

The next new hotel requires:

1. Verified HPC record ID (`rec…`)
2. Approved research budget
3. Standard PowerShell command

—not another hotel-specific development project.

## Required steps

1. **Hotel HPC ID or validated cohort**  
   One ID, or a `HotelsFile` (txt: one `rec…` per line; or JSON `{ "hotelIds": [...] }`).

2. **Automatic hotel configuration / preflight**  
   - Existing `config/group-demand-intelligence/hotels/{hotelId}.json` preferred.  
   - Else `proposeHotelGdiConfig` from HPC / verified evidence (unresolved fields marked, never guessed).  
   - Preflight: `CohortPreflight`.

3. **Approved research budget**  
   `-BudgetUSD` aggregate + `-PerHotelBudgetUSD` + founder spend phrase `FOUNDER_GDI_UNIVERSAL_COHORT_PAID` + `-AllowLivePilot` + `-AuthorizePaidResearch`.

4. **Canonical PowerShell execution**  
   See exact command below. Uses `Invoke-GdiPortfolioProcessing.ps1` → `gdi-universal-cohort-processing-v2.mjs` → established GDI engine.

5. **Research quality validation**  
   Review per-hotel reports under `reports/gdi-universal-cohort-processing-v2/`. Provider success ≠ commercial success.

6. **Opportunity deduplication**  
   Cross-segment program dedupe runs before deep selection (CARPHA / Advantage class failures corrected).

7. **Buyer identification and verification**  
   Public sources only. Retain verified names without personal email. Sales Ready remains fail-closed. No Surfe / Findymail.

8. **Research inventory preservation**  
   Dry write-set first. Persistence apply requires separate authorization. Do not erase prior belief.

9. **Weekly refresh readiness**  
   Document only — do **not** activate weekly automation in this procedure.

10. **Separate customer publication authorization**  
    Paid research ≠ publication. Publication requires independent founder GO.

## Exact standard PowerShell command

### Preflight ($0)

```powershell
.\scripts\Invoke-GdiPortfolioProcessing.ps1 `
  -Mode CohortPreflight `
  -HotelsFile .\reports\gdi-repeatable-processing-v2\design-hotels-pilot-cohort-hotels.txt `
  -BudgetUSD 12 `
  -PerHotelBudgetUSD 4 `
  -MaxCandidates 4
```

### One hotel

```powershell
.\scripts\Invoke-GdiPortfolioProcessing.ps1 `
  -Mode CohortPreflight `
  -HotelId recXXXXXXXXXXXXXX
```

### Execute (founder GO only)

```powershell
$env:GDI_EXA_AGENT_LIVE = '1'
.\scripts\Invoke-GdiPortfolioProcessing.ps1 `
  -Mode CohortExecute `
  -HotelsFile .\path\to\cohort-hotels.txt `
  -BudgetUSD 12 `
  -PerHotelBudgetUSD 4 `
  -AllowLivePilot `
  -AuthorizePaidResearch `
  -FounderSpendConfirmation FOUNDER_GDI_UNIVERSAL_COHORT_PAID `
  -MaxCandidates 4
```

### Zero-paid replay

```powershell
.\scripts\Invoke-GdiPortfolioProcessing.ps1 `
  -Mode CohortReplay `
  -HotelsFile .\path\to\cohort-hotels.txt
```

## Historical regression path (frozen)

`DesignHotelsLivePreflight|Execute|Replay` remain hard-coded to Condesa / LaLuna / Memmo for historical pilot regression only. **Do not use DesignHotelsLive for new hotels.**

## Forbidden without founder GO

- Paid research beyond authorized ceiling  
- Airtable live writes / production persistence  
- Customer publication  
- Paid contact enrichment  
- Weekly automation activation  
- Production deployment  
