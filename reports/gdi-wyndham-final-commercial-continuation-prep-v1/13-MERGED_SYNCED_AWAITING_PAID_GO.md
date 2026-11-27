# MERGED_AND_SYNCED — AWAITING_FOUNDER_PAID_GO

**At:** 2026-11-27  
**Stream:** GDI (`C:\Dev\dealality\ws-gdi`)  
**Permission to spend:** **NO**  
**Checkpoint B:** **BLOCKED** until Checkpoint A assessment accepted

---

## Merge + sync

| Field | Value |
|---|---|
| PR #613 | **MERGED** |
| Merge commit | `6b474490de542ab633b01fe44b03a76cf1dc3e8d` |
| Home branch | `ws/gdi` |
| HEAD | `6b474490de542ab633b01fe44b03a76cf1dc3e8d` |
| `origin/main` | `6b474490de542ab633b01fe44b03a76cf1dc3e8d` |
| HEAD == origin/main | **YES** |
| Dirty unrelated | **Preserved** |
| Baseline 26 | Locked / accepted / **not applied** |

---

## Integration blocker resolved

Generic V3 Live previously exited with “not enabled” (ad-hoc bypass risk).

Canonical path now:

`Invoke-GdiWhoHow.ps1` → `run-gdi-who-how-checkpoint-a-v1.mjs` → `gdi-exa-who-how-live-adapter-v1.js`

$0 preflight verified: `pathWired=true`, `opportunityCount=25`, `spentUsd=0`.

---

## Exact Checkpoint A PowerShell (after founder GO only)

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

| Control | Value |
|---|---|
| Authorization | `FOUNDER_GO_WYNDHAM_FINAL_COMMERCIAL_CONTINUATION_18USD_20261127` |
| Scope | Top 25 WHO/HOW continuations from manifest |
| Checkpoint ceiling | **USD 5.00** (inside USD 18.00 aggregate) |
| Resume | `-Resume` (settled-job reuse / no duplicate bill) |
| Checkpoint B | Still blocked |

### $0 path preflight (already executed)

```powershell
cd C:\Dev\dealality\ws-gdi
.\scripts\gdi\Invoke-GdiWhoHow.ps1 `
  -HotelId recnPB3Q02NO8Ma1o `
  -Mode DryRun `
  -AuthorizationId 'FOUNDER_GO_WYNDHAM_FINAL_COMMERCIAL_CONTINUATION_18USD_20261127' `
  -HardCeilingUsd 5 `
  -Manifest 'reports/gdi-wyndham-who-how-v3-tranche-20261127/01-TRANCHE_MANIFEST.json' `
  -ContinuationOnly `
  -Resume `
  -PreflightOnly
```

---

## Status

**MERGED_AND_SYNCED — AWAITING_FOUNDER_PAID_GO**

Next founder authorization releases **Checkpoint A only**.
