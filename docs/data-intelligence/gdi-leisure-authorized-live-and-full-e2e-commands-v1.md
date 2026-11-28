# GDI Leisure Authorized Live + Full E2E V3 — executable commands

Product-wide. Not Wyndham-specific. DryRun / fixture remains default.

Authorization identity (Checkpoint B continuation):

`FOUNDER_GO_WYNDHAM_FINAL_COMMERCIAL_CONTINUATION_18USD_20261127`

- Hard ceiling: **USD 18.00**
- Prior spend (Checkpoint A): **USD 5.00**
- Remaining: **USD 13.00**
- Hotel: `recnPB3Q02NO8Ma1o` (Wyndham San Diego Bayside)

Gates: `NO_AIRTABLE_APPLY` · `NO_STAGED_ACTIVATE` · `NO_CURRENT_PUBLISH` · `NO_FINDYMAIL` · `NO_SURFE_AUTO` · `NO_CARRYOVER` · `NO_REDISCOVERY_161` · Bethesda freeze ON · CLEAN11 freeze ON · locked 26 unchanged.

---

## 1. Validation ($0)

```powershell
cd C:\Dev\dealality\ws-gdi
npm run test:gdi-leisure-authorized-live-v1
npm run test:gdi-canonical-full-e2e-v3-v1
npm run test:gdi-leisure-demand-intelligence-v1
npm run gdi:checkpoint-b-future-pathway-reassess
```

## 2. Individual Full E2E V3 stages (DryRun)

```powershell
cd C:\Dev\dealality\ws-gdi
.\scripts\gdi\Invoke-GdiDiscovery.ps1 -HotelId recnPB3Q02NO8Ma1o -Mode DryRun
.\scripts\gdi\Invoke-GdiDeepResearch.ps1 -HotelId recnPB3Q02NO8Ma1o -Mode DryRun
.\scripts\gdi\Invoke-GdiWhoHow.ps1 -HotelId recnPB3Q02NO8Ma1o -Mode DryRun
.\scripts\gdi\Invoke-GdiContactResearch.ps1 -HotelId recnPB3Q02NO8Ma1o -Mode DryRun
.\scripts\gdi\Invoke-GdiQualification.ps1 -HotelId recnPB3Q02NO8Ma1o -Mode DryRun
```

## 3. Full execution (DryRun)

```powershell
cd C:\Dev\dealality\ws-gdi
.\scripts\gdi\Invoke-GdiFullE2E.ps1 -HotelId recnPB3Q02NO8Ma1o -Mode DryRun
.\scripts\gdi\Invoke-GdiValidateFullE2E.ps1 -HotelId recnPB3Q02NO8Ma1o
```

## 4. Leisure modules — fixture default ($0)

```powershell
cd C:\Dev\dealality\ws-gdi
.\scripts\gdi-leisure\Invoke-GdiLeisureEvents.ps1 -HotelId recnPB3Q02NO8Ma1o -ResearchMode fixture
.\scripts\gdi-leisure\Invoke-GdiLeisureSports.ps1 -HotelId recnPB3Q02NO8Ma1o -ResearchMode fixture
.\scripts\gdi-leisure\Invoke-GdiLeisureAttractions.ps1 -HotelId recnPB3Q02NO8Ma1o -ResearchMode fixture
.\scripts\gdi-leisure\Invoke-GdiLeisureUniversities.ps1 -HotelId recnPB3Q02NO8Ma1o -ResearchMode fixture
.\scripts\gdi-leisure\Invoke-GdiLeisureDestination.ps1 -HotelId recnPB3Q02NO8Ma1o -ResearchMode fixture
.\scripts\gdi-leisure\Invoke-GdiLeisureDistribution.ps1 -HotelId recnPB3Q02NO8Ma1o -ResearchMode fixture
```

Or via shared runner:

```powershell
.\scripts\gdi-leisure\Invoke-GdiLeisureModule.ps1 -Module leisure_events `
  -HotelId recnPB3Q02NO8Ma1o -ResearchMode fixture
```

## 5. Authorized live — $0 preflight (never spends)

```powershell
cd C:\Dev\dealality\ws-gdi
.\scripts\gdi-leisure\Invoke-GdiLeisureModule.ps1 -Module leisure_events `
  -HotelId recnPB3Q02NO8Ma1o -ResearchMode live -PreflightOnly `
  -AuthorizePaidResearch -AllowLivePilot `
  -AuthorizationId 'FOUNDER_GO_WYNDHAM_FINAL_COMMERCIAL_CONTINUATION_18USD_20261127' `
  -HardCeilingUsd 18 -BudgetUSD 2
```

Node equivalent:

```powershell
node scripts/gdi-leisure-demand-intelligence-v1.mjs `
  --hotel-id recnPB3Q02NO8Ma1o --modules leisure_events --mode live `
  --preflight-only --authorize-paid-research --allow-live-pilot `
  --authorization-id FOUNDER_GO_WYNDHAM_FINAL_COMMERCIAL_CONTINUATION_18USD_20261127 `
  --hard-ceiling-usd 18 --budget-usd 2
```

## 6. Authorized live mode (after IC merge + $0 preflight PASS)

Requires `$env:GDI_LDI_LIVE = '1'`. Reuses fourteen-segment cells + durable ledger. Settled-job reuse / resume. No Findymail / Surfe / publication.

```powershell
cd C:\Dev\dealality\ws-gdi
$env:GDI_LDI_LIVE = '1'
$Auth = 'FOUNDER_GO_WYNDHAM_FINAL_COMMERCIAL_CONTINUATION_18USD_20261127'
$Hotel = 'recnPB3Q02NO8Ma1o'

foreach ($m in @(
  'leisure_events','leisure_sports','leisure_attractions',
  'leisure_universities','leisure_destination','leisure_distribution'
)) {
  .\scripts\gdi-leisure\Invoke-GdiLeisureModule.ps1 -Module $m `
    -HotelId $Hotel -ResearchMode live `
    -AuthorizePaidResearch -AllowLivePilot `
    -AuthorizationId $Auth -HardCeilingUsd 18 -BudgetUSD 2 -Resume
}
```

## 7. Resume

```powershell
# Leisure
$env:GDI_LDI_LIVE = '1'
.\scripts\gdi-leisure\Invoke-GdiLeisureModule.ps1 -Module leisure_events `
  -HotelId recnPB3Q02NO8Ma1o -ResearchMode live `
  -AuthorizePaidResearch -AllowLivePilot `
  -AuthorizationId 'FOUNDER_GO_WYNDHAM_FINAL_COMMERCIAL_CONTINUATION_18USD_20261127' `
  -HardCeilingUsd 18 -BudgetUSD 2 -Resume

# Full E2E V3
.\scripts\gdi\Invoke-GdiFullE2E.ps1 -HotelId recnPB3Q02NO8Ma1o -Mode Live `
  -AuthorizePaidResearch -Resume
```

## 8. Budget-limited execution

Per-run `-BudgetUSD` must be ≤ remaining ledger ceiling. Hard ceiling is enforced before job submission via `gdi-authorized-paid-budget-ledger-v1`.

```powershell
$env:GDI_LDI_LIVE = '1'
.\scripts\gdi-leisure\Invoke-GdiLeisureModule.ps1 -Module leisure_events `
  -HotelId recnPB3Q02NO8Ma1o -ResearchMode live `
  -AuthorizePaidResearch -AllowLivePilot `
  -AuthorizationId 'FOUNDER_GO_WYNDHAM_FINAL_COMMERCIAL_CONTINUATION_18USD_20261127' `
  -HardCeilingUsd 18 -BudgetUSD 1 -Resume
```

Do **not** require exhausting the remaining USD 13.00.

## 9. Weekly continuation

```powershell
cd C:\Dev\dealality\ws-gdi
npm run dealality:git-preflight -- --op status-check --stream gdi
.\scripts\gdi\Invoke-GdiFullE2E.ps1 -HotelId recnPB3Q02NO8Ma1o -Mode DryRun -Resume
npm run gdi:checkpoint-b-future-pathway-reassess
# Paid only when founder authorization + remaining ceiling > 0 + mechanism operational
```

## 10. Commercial viability precheck (Stage 2 end)

Wired into Full E2E V3 orchestrator as `02b-commercial-viability-precheck.json` — **not** a sixth stage. Reuses FQO demand / hotel fit / future pathway rules.

Standalone batch:

```powershell
node -e "import('./lib/group-demand-intelligence/workflow/gdi-commercial-viability-precheck-v1.js').then(m=>console.log(JSON.stringify(m.batchCommercialViabilityPrecheck([]),null,2)))"
```

## 11. Research Receipt telemetry fields (prep only)

Each authorized LDI live run records:

- signalsEvaluated
- candidatesDiscovered
- duplicatesConsolidated
- opportunitiesScreenedOut
- deepResearchCompleted
- namedPeopleIdentified
- existingEvidenceReused
- contactRoutesValidated
- fullyQualifiedOpportunities
- attribution (`MIXED_SHARED_MARKET_AND_HOTEL_SPECIFIC`)

This prepares the GDI Executive Summary Research Receipt; it does **not** authorize Exec Summary work in the Checkpoint B PR.
