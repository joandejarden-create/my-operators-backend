# GDI Canonical PowerShell Operations Runbook V1

**Contract:** `GDI_CANONICAL_POWERSHELL_OPS_V1`  
**Lifecycle:** FULL E2E → DURABLE PERSISTENCE → STAGED → FOUNDER APPROVAL → CURRENT  
**Template:** `GDI_CANONICAL_HOTEL_TEMPLATE_V1` (first three tabs — no UI changes)  
**Contacts:** `GDI_CONTACT_RESEARCH_BEST_EFFORT_V1`  
**Certification gate:** `npm run test:gdi-canonical-powershell-ops-cert-v1` · `node scripts/gdi-canonical-powershell-ops-cert-v1.mjs`  
**Live orchestrator gate:** `node scripts/test-gdi-full-e2e-v3-live-orchestrator-v1.mjs` (also run by `Invoke-GdiValidateFullE2E.ps1`; no provider spend)

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

Shared helper: `_GdiV3Common.ps1` → `run-gdi-full-e2e-v3-stage-cli-v1.mjs` → `runFullE2eV3Live` / stage runners in `gdi-full-e2e-v3-orchestrator-v1.js`.

## Live model (hotel-agnostic)

Live = `-Mode Live -AuthorizePaidResearch`. Live reuses existing adapters; there is no parallel research engine and no hotel default.

| Stage | Live behavior | Spend |
|---|---|---|
| 1 Discovery | Reuses existing discovery artifacts (Node `--discovery-matrix <path>` or `<outDir>/fourteen-segment-discovery-matrix.json`). Missing → **HOLD**. | none |
| 2 Deep research | Reuses settled queue dossiers (`<outDir>/dossiers`). Missing → **HOLD**. | none |
| 3 WHO/HOW | Existing Exa adapter (Checkpoint A pattern). **Requires** `-AuthorizationId`, `-HardCeilingUsd > 0`, `-Manifest` unless `-SkipWhoHow`; also `GDI_EXA_AGENT_LIVE=1`. | Exa, ≤ ceiling |
| 4 Contact | Public-first. Findymail only with `-AuthorizeFindymail` (+ `FINDYMAIL_API_KEY`). Surfe never. | Findymail credits only when authorized |
| 5 Qualification | Strict FQO + entry-point reachability. | none |
| 6 STAGED | Live activates STAGED (unless `-SkipStagedProgression` / `-NoActivateStagedPointer`). CURRENT never changes. | none |
| Airtable research persistence | Optional and separate (see below). `-AuthorizePaidResearch` never unlocks writes. | none |

Fail-closed exit codes from the Node CLI: `2` = missing/invalid authorization parameters (nothing executed); `4` = provider authorization refused (e.g. missing `FINDYMAIL_API_KEY`, `GDI_EXA_AGENT_LIVE` not set); `1` = stage failure or invalid STAGED inventory. Exit 3 is no longer used.

`paid` in `00-ORCHESTRATOR_SUMMARY.json` is true only when WHO/HOW or Findymail actually spent.

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

### Full E2E — Live (founder-authorized, hotel-specific)

```powershell
$env:GDI_EXA_AGENT_LIVE = '1'   # Exa provider env gate (WHO/HOW spend)
pwsh -File scripts/gdi/Invoke-GdiFullE2E.ps1 -HotelId <rec...> -Mode Live -AuthorizePaidResearch `
  -AuthorizationId '<FOUNDER_GO_…>' -HardCeilingUsd <n> `
  -Manifest 'reports/<hotel-tranche>/01-TRANCHE_MANIFEST.json'
```

Required: `-AuthorizationId`, `-HardCeilingUsd` (> 0), `-Manifest`. There are no Wyndham (or any hotel) defaults. Add `-AuthorizeFindymail` to run Findymail in Stage 4 (needs `FINDYMAIL_API_KEY`). Equivalent Node form:

```powershell
node scripts/gdi/run-gdi-full-e2e-v3-stage-cli-v1.mjs --stage FULL --hotel-id <rec...> --mode Live `
  --authorize-paid-research --authorization-id <FOUNDER_GO_…> --hard-ceiling-usd <n> --manifest <path>
```

### Individual stages

```powershell
pwsh -File scripts/gdi/Invoke-GdiDiscovery.ps1 -HotelId <rec...> -Mode DryRun
pwsh -File scripts/gdi/Invoke-GdiDeepResearch.ps1 -HotelId <rec...> -Mode DryRun
pwsh -File scripts/gdi/Invoke-GdiWhoHow.ps1 -HotelId <rec...> -Mode DryRun
pwsh -File scripts/gdi/Invoke-GdiContactResearch.ps1 -HotelId <rec...> -Mode DryRun
pwsh -File scripts/gdi/Invoke-GdiQualification.ps1 -HotelId <rec...> -Mode DryRun
```

Live single stages (reuse semantics; no paid discovery):

```powershell
pwsh -File scripts/gdi/Invoke-GdiDiscovery.ps1     -HotelId <rec...> -Mode Live -AuthorizePaidResearch
pwsh -File scripts/gdi/Invoke-GdiDeepResearch.ps1  -HotelId <rec...> -Mode Live -AuthorizePaidResearch
pwsh -File scripts/gdi/Invoke-GdiQualification.ps1 -HotelId <rec...> -Mode Live -AuthorizePaidResearch `
  -OpportunitiesFile reports/<hotel>/opportunities.json   # or a prior Live 03/04 report in -OutDir
```

Live WHO/HOW uses Checkpoint A (below). The generic CLI also accepts `--stage WHO_HOW --mode Live` only with `--authorization-id`, `--hard-ceiling-usd`, `--manifest` (otherwise exit 2).

### Contact-only continuation (best-effort channels)

```powershell
pwsh -File scripts/gdi/Invoke-GdiContactResearch.ps1 -HotelId <rec...> -Mode DryRun

# Live, public-first (no Findymail / no spend):
pwsh -File scripts/gdi/Invoke-GdiContactResearch.ps1 -HotelId <rec...> -Mode Live -AuthorizePaidResearch `
  -OpportunitiesFile reports/<hotel>/opportunities.json

# Plan Findymail eligibility only (no key needed, no spend):
pwsh -File scripts/gdi/Invoke-GdiContactResearch.ps1 -HotelId <rec...> -Mode Live -AuthorizePaidResearch `
  -AuthorizeFindymail -FindymailDryRun -OpportunitiesFile reports/<hotel>/opportunities.json

# Live Findymail — founder-authorized only; requires FINDYMAIL_API_KEY (exit 4 if missing):
pwsh -File scripts/gdi/Invoke-GdiContactResearch.ps1 -HotelId <rec...> -Mode Live -AuthorizePaidResearch `
  -AuthorizeFindymail -OpportunitiesFile reports/<hotel>/opportunities.json

# Full E2E contact-only continuation (skip WHO/HOW; reuses a prior Live 03-who-how report or -OpportunitiesFile):
pwsh -File scripts/gdi/Invoke-GdiFullE2E.ps1 -HotelId <rec...> -Mode Live -AuthorizePaidResearch `
  -SkipWhoHow -SkipDiscovery -SkipDeep -ContinuationOnly -Resume [-AuthorizeFindymail]
```

Finding one channel must not terminate other channels. Surfe is never auto-invoked. Without opportunities, Live contact has nothing to process (HOLD) — contacts are never invented.

### WHO/HOW Checkpoint A (Live) — hotel-specific auth required

```powershell
pwsh -File scripts/gdi/Invoke-GdiWhoHow.ps1 -HotelId <rec...> -Mode DryRun `
  -AuthorizationId '<FOUNDER_GO_…>' -HardCeilingUsd 5 `
  -Manifest 'reports/<hotel-tranche>/01-TRANCHE_MANIFEST.json' `
  -ContinuationOnly -Resume -PreflightOnly

$env:GDI_EXA_AGENT_LIVE = '1'
pwsh -File scripts/gdi/Invoke-GdiWhoHow.ps1 -HotelId <rec...> -Mode Live -AuthorizePaidResearch `
  -AuthorizationId '<FOUNDER_GO_…>' -HardCeilingUsd 5 `
  -Manifest 'reports/<hotel-tranche>/01-TRANCHE_MANIFEST.json' -ContinuationOnly -Resume
```

No Wyndham-default AuthorizationId or Manifest is permitted.

### Resume

```powershell
pwsh -File scripts/gdi/Invoke-GdiFullE2E.ps1 -HotelId <rec...> -Mode DryRun -Resume
# Live resume: skips stages whose prior LIVE report PASSed (DryRun reports are never reused as Live evidence):
pwsh -File scripts/gdi/Invoke-GdiFullE2E.ps1 -HotelId <rec...> -Mode Live -AuthorizePaidResearch -Resume `
  -AuthorizationId '<FOUNDER_GO_…>' -HardCeilingUsd <n> -Manifest '<path>'
```

Settled Exa jobs are reused by the adapter job store (no duplicate spend for the same authorization id).

### Weekly refresh

```powershell
npm run gdi:weekly-refresh -- --hotel-id <rec...> --dry-run
```

Live weekly refresh = rerun the Live Full E2E command above with the hotel's current manifest and a fresh founder authorization id; budget and Airtable persistence stay separately authorized.

### Airtable recovery / research persistence (separate authorization)

```powershell
# Plan only (dry-run ledger; no writes):
pwsh -File scripts/gdi/Invoke-GdiFullE2E.ps1 -HotelId <rec...> -Mode Live -AuthorizePaidResearch -SkipWhoHow `
  -ContinuationOnly -Resume -PlanResearchPersistence

# Apply — needs ALL of: authorization flag, exact write-set text, apply flag, and
# GDI_RESEARCH_INVENTORY_PRODUCTION_WRITES=1 (otherwise the bridge stays dry-run):
$env:GDI_RESEARCH_INVENTORY_PRODUCTION_WRITES = '1'
pwsh -File scripts/gdi/Invoke-GdiFullE2E.ps1 -HotelId <rec...> -Mode Live -AuthorizePaidResearch -SkipWhoHow `
  -ContinuationOnly -Resume -AuthorizeResearchPersistence `
  -ExactWriteSetAuthorization '<exact write-set authorization>' -ApplyResearchPersistence
```

`-AuthorizePaidResearch` alone never unlocks Airtable writes; persistence flags are accepted only on Full E2E (single-stage Live refuses them, exit 2). Recovery from Airtable-only state remains PARTIAL (see Certification).

### STAGED regeneration

```powershell
# Re-run qualification + STAGED progression from prior Live opportunities (no new spend):
pwsh -File scripts/gdi/Invoke-GdiFullE2E.ps1 -HotelId <rec...> -Mode Live -AuthorizePaidResearch `
  -SkipDiscovery -SkipDeep -SkipWhoHow -SkipContact -ContinuationOnly -Resume `
  -OpportunitiesFile reports/<hotel>/opportunities.json
# Flags: -SkipStagedProgression (never touch STAGED) | -NoActivateStagedPointer (build/validate package only)
```

CURRENT is never changed by these runners.

### Validation-only (no activate, no paid)

```powershell
pwsh -File scripts/gdi/Invoke-GdiValidateFullE2E.ps1 -HotelId <rec...> -RequirePort8080
node scripts/gdi/run-gdi-full-e2e-v3-stage-cli-v1.mjs --stage VALIDATE --hotel-id <rec...> --mode Live
```

`--stage VALIDATE --mode Live` is read-only: it summarizes the existing checkpoint and STAGED package, does not execute stages, never activates STAGED, and needs no paid authorization.

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
- Live: requires `-AuthorizePaidResearch`. WHO/HOW additionally requires `-AuthorizationId`, `-HardCeilingUsd`, `-Manifest` (or `-SkipWhoHow`) and `GDI_EXA_AGENT_LIVE=1`. Findymail additionally requires `-AuthorizeFindymail` + `FINDYMAIL_API_KEY`. Airtable persistence requires its own flags.
- Missing parameters fail closed (exit 2) before any stage runs. Budget exhaustion ≠ COMPLETE.
- Airtable writes and CURRENT remain separately authorized. Surfe is never invoked.

## CAD / PLP / LDI

- Embedded in Discovery / canonical Full E2E opt-in stages (not separate mandatory Live CLIs).
- Leisure wrappers: `scripts/gdi-leisure/Invoke-GdiLeisure*.ps1` + `npm run gdi:leisure-demand-intelligence-v1`.
- CAD: classifier + opt-in stage tests; Phase 3A pilot is read-only.

## Troubleshooting

| Symptom | Action |
|---|---|
| Live exit 2 from Full E2E | Supply the missing parameter named in stderr (`-AuthorizationId`, `-HardCeilingUsd`, `-Manifest`) or pass `-SkipWhoHow` |
| Live exit 4 | Provider authorization refused: set `GDI_EXA_AGENT_LIVE=1` (WHO/HOW) or `FINDYMAIL_API_KEY` (Findymail) — only with founder authorization |
| Discovery / Deep HOLD in Live | Existing artifacts missing; supply `-OutDir` with artifacts or run the original discovery/queue runners (Live never invents paid discovery) |
| Checkpoint A missing auth | Pass hotel-specific `-AuthorizationId`, `-HardCeilingUsd`, `-Manifest` |
| STAGED missing on :8080 | `Invoke-GdiValidateFullE2E.ps1 -RequirePort8080`; reclaim `:8080` via Integration if needed |
| Contact gaps remain | Contact DryRun → best-effort ledger → founder-authorized Findymail/Exa gap continuation |

## Certification

```powershell
node scripts/gdi-canonical-powershell-ops-cert-v1.mjs
npm run test:gdi-canonical-powershell-ops-cert-v1
node scripts/test-gdi-full-e2e-v3-live-orchestrator-v1.mjs
```

Operational certification still requires one demonstrated authorized-spend, hotel-agnostic Live Full E2E run (non-Wyndham) — wiring and fail-closed behavior are tested without spend; DryRun alone does not certify.
