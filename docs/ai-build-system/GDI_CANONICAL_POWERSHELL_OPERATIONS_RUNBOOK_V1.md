# GDI Canonical PowerShell Operations Runbook V1

**Contract:** `GDI_CANONICAL_POWERSHELL_OPS_V1`  
**Lifecycle:** FULL E2E → DURABLE PERSISTENCE → STAGED → FOUNDER APPROVAL → CURRENT  
**Template:** `GDI_CANONICAL_HOTEL_TEMPLATE_V1` (first three tabs — no UI changes)  
**Contacts:** `GDI_CONTACT_RESEARCH_BEST_EFFORT_V1`  
**Certification gate:** `npm run test:gdi-canonical-powershell-ops-cert-v1` · `node scripts/gdi-canonical-powershell-ops-cert-v1.mjs`

## Operator path (product-wide)

All hotels use the same runners under `scripts/gdi/`. Pass `-HotelId <rec…>` — never hardcode hotel identity into the scripts.

| Stage | Runner |
|---|---|
| Full E2E | `Invoke-GdiFullE2E.ps1` |
| Discovery | `Invoke-GdiDiscovery.ps1` |
| Deep research | `Invoke-GdiDeepResearch.ps1` |
| WHO/HOW | `Invoke-GdiWhoHow.ps1` |
| Contact (+ Findymail) | `Invoke-GdiContactResearch.ps1` |
| Qualification | `Invoke-GdiQualification.ps1` |
| Validate | `Invoke-GdiValidateFullE2E.ps1` |
| Content hydration | `Invoke-GdiContentHydrationValidate.ps1` |
| Executive Summary | `Invoke-GdiExecutiveSummary.ps1` |
| CURRENT promote | `Invoke-GdiPromoteCurrent.ps1` (separate founder gate) |

Shared helper: `_GdiV3Common.ps1` → `run-gdi-full-e2e-v3-stage-cli-v1.mjs`.

## Exact commands

Use `pwsh` when available; on Windows hosts without PowerShell 7, `powershell -NoProfile -ExecutionPolicy Bypass -File …` is equivalent for these runners. The Node stage CLI under each wrapper is also valid:

```powershell
node scripts/gdi/run-gdi-full-e2e-v3-stage-cli-v1.mjs --stage FULL --hotel-id <rec...> --mode DryRun
```

### New hotel / Full E2E (DryRun first)

```powershell
pwsh -File scripts/gdi/Invoke-GdiFullE2E.ps1 -HotelId <rec...> -Mode DryRun
pwsh -File scripts/gdi/Invoke-GdiValidateFullE2E.ps1 -HotelId <rec...> -RequirePort8080
```

### Individual stages

```powershell
pwsh -File scripts/gdi/Invoke-GdiDiscovery.ps1 -HotelId <rec...> -Mode DryRun
pwsh -File scripts/gdi/Invoke-GdiDeepResearch.ps1 -HotelId <rec...> -Mode DryRun
pwsh -File scripts/gdi/Invoke-GdiWhoHow.ps1 -HotelId <rec...> -Mode DryRun
pwsh -File scripts/gdi/Invoke-GdiContactResearch.ps1 -HotelId <rec...> -Mode DryRun
pwsh -File scripts/gdi/Invoke-GdiQualification.ps1 -HotelId <rec...> -Mode DryRun
```

### Contact-only continuation (best-effort channels)

```powershell
pwsh -File scripts/gdi/Invoke-GdiContactResearch.ps1 -HotelId <rec...> -Mode DryRun
# Live Findymail — founder-authorized only:
pwsh -File scripts/gdi/Invoke-GdiContactResearch.ps1 -HotelId <rec...> -Mode Live -AuthorizePaidResearch -AuthorizeFindymail
```

Finding one channel must not terminate other channels. Surfe is never auto-invoked.

### WHO/HOW Checkpoint A (Live) — hotel-specific auth required

```powershell
pwsh -File scripts/gdi/Invoke-GdiWhoHow.ps1 -HotelId <rec...> -Mode DryRun `
  -AuthorizationId '<FOUNDER_GO_…>' -HardCeilingUsd 5 `
  -Manifest 'reports/<hotel-tranche>/01-TRANCHE_MANIFEST.json' `
  -ContinuationOnly -Resume -PreflightOnly
```

No Wyndham-default AuthorizationId or Manifest is permitted.

### Resume

```powershell
pwsh -File scripts/gdi/Invoke-GdiFullE2E.ps1 -HotelId <rec...> -Mode DryRun -Resume
```

### Weekly refresh

```powershell
npm run gdi:weekly-refresh -- --hotel-id <rec...> --dry-run
```

### STAGED review on :8080

```powershell
pwsh -File scripts/gdi/Invoke-GdiContentHydrationValidate.ps1 -HotelId <rec...> -RequirePort8080
# Browser: http://127.0.0.1:8080/group-demand-intelligence.html?hotelId=<rec...>&preview=staged
```

### CURRENT (separate authorization)

```powershell
pwsh -File scripts/gdi/Invoke-GdiPromoteCurrent.ps1 -HotelId <rec...> -PublicationVersion <ver> -Mode DryRun
```

## Budget and authorization

- DryRun: non-mutating, no paid research.
- Live: requires `-AuthorizePaidResearch`; Findymail additionally requires `-AuthorizeFindymail`.
- Generic V3 stage CLI currently **refuses** Live paid execution (exit 3) — use Checkpoint A / authorized Findymail paths until the unified Live orchestrator gap is closed.
- Budget exhaustion ≠ COMPLETE.
- Airtable writes and CURRENT remain separately authorized.

## CAD / PLP / LDI

- Embedded in Discovery / canonical Full E2E opt-in stages (not separate mandatory Live CLIs).
- Leisure wrappers: `scripts/gdi-leisure/Invoke-GdiLeisure*.ps1` + `npm run gdi:leisure-demand-intelligence-v1`.
- CAD: classifier + opt-in stage tests; Phase 3A pilot is read-only.

## Troubleshooting

| Symptom | Action |
|---|---|
| Live exit 3 from Full E2E | Expected until unified Live orchestrator closes; use stage-specific authorized runners |
| Checkpoint A missing auth | Pass hotel-specific `-AuthorizationId`, `-HardCeilingUsd`, `-Manifest` |
| STAGED missing on :8080 | `Invoke-GdiValidateFullE2E.ps1 -RequirePort8080`; reclaim `:8080` via Integration if needed |
| Contact gaps remain | Contact DryRun → best-effort ledger → founder-authorized Findymail/Exa gap continuation |

## Certification

```powershell
node scripts/gdi-canonical-powershell-ops-cert-v1.mjs
npm run test:gdi-canonical-powershell-ops-cert-v1
```

Operational certification requires demonstrated Live hotel-agnostic Full E2E — not DryRun alone.
