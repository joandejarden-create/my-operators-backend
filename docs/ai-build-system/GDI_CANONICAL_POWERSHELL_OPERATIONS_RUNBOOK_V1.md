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

#### Live FULL — unified flags (Leisure / CAD / historical replay / Findymail persistence)

These flags are accepted by `Invoke-GdiFullE2E.ps1` (→ `_GdiV3Common.ps1`) and the Node CLI; names below match the CLI usage at the time of writing (`node scripts/gdi/run-gdi-full-e2e-v3-stage-cli-v1.mjs --help` is authoritative — if the Live orchestrator changes a name, the CLI wins and this section must be updated).

| PowerShell | Node CLI | Effect |
|---|---|---|
| `-AllowHistoricalReplay` | `--allow-historical-replay` | Replay a historical discovery matrix; default-enables Leisure/CAD assess at $0 |
| `-AuthorizePaidDiscovery` | `--authorize-paid-discovery` | Default-enables the Leisure/CAD spine; the orchestrator still **reuses** discovery artifacts (no paid discovery is invented) |
| `-EnableLeisure` / `-NoLeisure` | `--enable-leisure` / `--no-leisure` | Force the Leisure standard stage on/off (`mock_live`, $0) |
| `-EnableCad` / `-NoCad` | `--enable-cad` / `--no-cad` | Force the CAD standard stage on/off (classification only) |
| `-AuthorizeFindymail` (+ `-FindymailDryRun`) | `--authorize-findymail` (+ `--findymail-dry-run`) | Live Stage 4 Findymail (`FINDYMAIL_API_KEY`); `-FindymailDryRun` plans eligibility with no spend |
| `-FindymailCreditHardCap <n>` | `--findymail-credit-hard-cap <n>` | Shared Stage 4 Findymail credit ceiling for the run (default 40 if omitted; use `15` for proposed Live GO) |
| `-PersistFindymailResults` | `--persist-findymail-results` | Findymail → Airtable **persist plan**; needs `-AuthorizeFindymail` **and** either (`-AuthorizeResearchPersistence` + `-ExactWriteSetAuthorization`) or (`-AuthorizeFindymailPersist` + `-AuthorizeAirtableProductionWrites`) |
| `-ApplyFindymailPersist` | `--apply-findymail-persist` | Applies the plan; also needs env `GDI_FINDYMAIL_PERSIST_PRODUCTION_WRITES=1` |

```powershell
# $0 unified spine check on a hotel with existing artifacts (no WHO/HOW spend, STAGED untouched):
pwsh -File scripts/gdi/Invoke-GdiFullE2E.ps1 -HotelId <rec...> -Mode Live -AuthorizePaidResearch -SkipWhoHow `
  -AllowHistoricalReplay -EnableLeisure -EnableCad -NoActivateStagedPointer -OutDir <isolated-dir>

# Founder-authorized Live FULL (hotel-specific auth id + ceiling + manifest; Findymail optional):
$env:GDI_EXA_AGENT_LIVE = '1'
pwsh -File scripts/gdi/Invoke-GdiFullE2E.ps1 -HotelId <rec...> -Mode Live -AuthorizePaidResearch `
  -AuthorizationId '<FOUNDER_GO_…>' -HardCeilingUsd <n> -Manifest 'reports/<hotel-tranche>/01-TRANCHE_MANIFEST.json' `
  -AuthorizeFindymail -AllowHistoricalReplay -EnableLeisure -EnableCad

# Findymail persistence PLAN (no Airtable write — omit -ApplyFindymailPersist; apply is a separate founder authorization):
pwsh -File scripts/gdi/Invoke-GdiFullE2E.ps1 -HotelId <rec...> -Mode Live -AuthorizePaidResearch -SkipWhoHow `
  -ContinuationOnly -Resume -AuthorizeFindymail -FindymailDryRun -PersistFindymailResults `
  -AuthorizeFindymailPersist -AuthorizeAirtableProductionWrites
```

Notes: Surfe is never invoked. The Findymail credit cap is a stage default (`creditHardCap` = 40); there is currently **no CLI flag** to set a lower cap (e.g. 15) — enforce via the usage ledger or add a flag before relying on a smaller authorization. Live Stage 1/2 **HOLD** when no discovery/deep artifacts exist in `-OutDir`; a truly fresh hotel needs artifacts first.

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
# DryRun (proven; no SerpApi, no provider calls, no data/ writes, STAGED/CURRENT untouched):
npm run gdi:weekly-refresh -- --hotel-id <rec...> --dry-run
```

Implementation: `scripts/gdi-weekly-refresh.mjs`. Accepts `--hotel-id <rec…>` / `--hotel-id=<rec…>` and the legacy `--hotel=<rec…|bethesda>`, plus `--dry-run` / `--dryRun`.

- **DryRun** never calls SerpApi and does not require `SERPAPI_KEY`. The weekly delta is computed from the hotel's existing opportunity bag and the latest frozen weekly discovery (if any; otherwise existing-bag-only is reported as `NONE_DRY_RUN_EXISTING_BAG_ONLY`). Reports go to `reports/gdi-weekly-refresh/<hotelId>/` (`00-WEEKLY_REFRESH_DRY_RUN.json`, `01-baseline.json`, `02-weekly-delta.json`, `03-staged-preview.opportunities.json`). `--dry-run` combined with `--apply-stage` is refused (exit 2).
- **Live weekly discovery** (no `--dry-run`, no reusable frozen discovery) requires `SERPAPI_KEY`; without it the run fails closed with exit 2 and `BLOCKED — SERPAPI_KEY required for live weekly discovery`. `--skip-discovery` reuses a frozen discovery without a key.
- Run ids use the ISO run date (`gdi_weekly_<slug>_YYYYMMDD`); `--run-date YYYYMMDD` overrides (the legacy `20260921` Bethesda baselines are preserved and are reused as frozen discovery).
- Regression: `npm run test:gdi-weekly-refresh-dry-run-v1`.

A full Live weekly refresh of intelligence (new paid research) = rerun the Live Full E2E command above with the hotel's current manifest and a fresh founder authorization id; budget and Airtable persistence stay separately authorized.

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

### Clean Airtable reconstruction certification (read-only)

```powershell
npm run gdi:clean-airtable-reconstruction-cert-v1
# optional: -- --hotel-id <rec...> --report-dir <dir>      (default hotel: Wyndham recnPB3Q02NO8Ma1o)
npm run test:gdi-clean-airtable-reconstruction-cert-v1     # offline regression
```

- **Source** = Airtable Group Demand Opportunities via `listOpportunitiesForHotel` (never STAGED). **Comparison baseline** = the hotel's current STAGED package (read-only; pointer/package hashes verified unchanged).
- Builds an isolated reconstruction package under `reports/gdi-clean-airtable-reconstruction-cert-v1/_isolated/` (never inside a publication-package root) with in-memory channel-evidence recovery + buying-authority annotation.
- Reports field-level parity (ids, names, titles, emails, phones, LinkedIn, authority presence, commercial fields), the durable stores (Airtable GDO as SoT, Findymail usage ledger, immutable publication packages, Airtable research inventory) and `PASS` / `PARTIAL` / `BLOCKED` with the exact missing persistence corrections (`05-MISSING_PERSISTENCE_CORRECTIONS.json`).
- Reads Airtable only; no writes, no provider calls, no spend. A fixture source (`--airtable-fixture`) can never certify PASS.
- Current expected result is **PARTIAL** until inventory membership and buying-authority annotation are persisted (founder-authorized Airtable write).

### Pre-live operational readiness gate (non-mutating)

```powershell
npm run gdi:unified-live-e2e-prelive-readiness-v1     # full gate; writes reports/gdi-unified-live-e2e-prelive-readiness-v1/
npm run test:gdi-unified-live-e2e-prelive-readiness-v1  # fast logic/wiring regression
```

Checks (DryRun / fail-closed / planning only; provider keys stripped from child processes; isolated temp out-dirs): Live fail-closed without authorization · Live SkipWhoHow + historical replay summary structure · Findymail planning · weekly DryRun · reconstruction cert executes · runbook · template freeze (git-clean + required substrings) · Wyndham STAGED = 26 · second-hotel and fresh-hotel DryRun paths · STAGED/CURRENT/ledger bytes unchanged.

Output: `00-PRELIVE_READINESS.json` (exit 0 only when `READY`; otherwise `PARTIAL` with `outstanding`) and `01-FRESH_HOTEL_CERTIFICATION_PROPOSAL.md|json` (**PROPOSED LIMITS — NOT AUTHORIZATION TO SPEND**).

**Pre-live READY ≠ operational certification.** READY means the Live path is wired, fails closed, and is safe to attempt; operational certification still requires one founder-authorized, spend-bearing Live run on a non-Wyndham hotel.

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
npm run test:gdi-weekly-refresh-dry-run-v1
npm run gdi:clean-airtable-reconstruction-cert-v1
npm run gdi:unified-live-e2e-prelive-readiness-v1
```

**DryRun ≠ Live certification.** DryRun, fixture and planning runs prove wiring only. **Pre-live READY ≠ operational certification** — the suite stays `PARTIAL` until a founder-authorized Live run on a non-Wyndham hotel completes.

Operational certification still requires one demonstrated authorized-spend, hotel-agnostic Live Full E2E run (non-Wyndham) — wiring and fail-closed behavior are tested without spend; DryRun alone does not certify.
