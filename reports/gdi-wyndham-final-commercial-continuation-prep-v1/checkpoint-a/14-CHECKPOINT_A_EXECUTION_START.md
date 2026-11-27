# Checkpoint A — Execution Start

**At:** 2026-11-27  
**Authorization:** `FOUNDER_GO_WYNDHAM_FINAL_COMMERCIAL_CONTINUATION_18USD_20261127`  
**Permission:** Checkpoint A ONLY · max USD 5.00 · aggregate ceiling USD 18.00  
**Hotel:** `recnPB3Q02NO8Ma1o` Wyndham San Diego Bayside  

## Exact reproducible command

```powershell
cd C:\Dev\dealality\ws-gdi
$env:GDI_EXA_AGENT_LIVE = '1'
.\scripts\gdi\Invoke-GdiWhoHow.ps1 `
  -HotelId recnPB3Q02NO8Ma1o `
  -Mode Live `
  -AuthorizePaidResearch `
  -AuthorizationId 'FOUNDER_GO_WYNDHAM_FINAL_COMMERCIAL_CONTINUATION_18USD_20261127' `
  -HardCeilingUsd 5 `
  -Manifest 'reports/gdi-wyndham-who-how-v3-tranche-20261127/01-TRANCHE_MANIFEST.json' `
  -ContinuationOnly `
  -Resume
```

## Path

`Invoke-GdiWhoHow.ps1` → `run-gdi-who-how-checkpoint-a-v1.mjs` → `gdi-exa-who-how-live-adapter-v1.js`

## Preflight (before paid submit)

| Check | Result |
|---|---|
| PowerShell path wired | PASS (`pathWired=true`) |
| Manifest top-25 | PASS (25 ids) |
| Checkpoint ceiling USD 5 | PASS |
| Aggregate USD 18 | Documented on auth |
| Settled-job reuse / resume | PASS ($0 adapter tests) |
| Locked 26 baseline | PASS (not applied) |
| Freezes / no Airtable/STAGED/CURRENT/Findymail/Surfe | PASS |
| Dirty unrelated interference | Preserved; runner uses explicit paths |
| Fail-closed without env | PASS (adapter tests) |
| EXA credential | present |
| PR #615 authorized-path | OPEN (gitleaks green); executing from same branch code |

## Governance

NO_AIRTABLE_APPLY · NO_STAGED · NO_CURRENT · NO_FINDYMAIL · NO_SURFE_AUTO · NO_CARRYOVER · NO_REDISCOVERY_161 · Bethesda ON · CLEAN11 ON · Checkpoint B BLOCKED
