# Hotel Intelligence Factory — PowerShell Command Contract (V4)

**Status:** Proposed orchestration contract (design)  
**Rule:** PowerShell never implements research/persistence logic — it only invokes Node with explicit flags.

## Entry points

| Surface | Path |
|---|---|
| PowerShell orchestrator | `scripts/hotel-intelligence-factory.ps1` |
| Node CLI | `node scripts/hotel-intelligence-factory-run.mjs` |
| npm aliases (V4 PR) | `dealality:hif-research`, `dealality:hif-manifest`, `dealality:hif-promote-dry` |

## Global parameters

| Parameter | Required | Description |
|---|---|---|
| `-Mode` | Yes | `Research` \| `CanonicalPromotion` |
| `-Config` | No | Path to JSON config (default `configs/hotel-intelligence-factory/default.v4.json`) |
| `-RunId` | No | Resume existing run; omit to create new |
| `-Stages` | No | Comma list; default mode-appropriate |
| `-DryRun` | No | Default `$true` for Promotion; Research may persist research tables when `-ApplyResearch` |
| `-ApplyResearch` | No | MODE 1 only — write Discovery/Findings/Contracts/Milestones |
| `-ApplyPromotion` | No | MODE 2 only — requires `-Manifest` + founder auth env |
| `-Manifest` | MODE 2 apply | Path to approved creation/update manifest |
| `-BudgetUsd` | No | Metered ceiling (default inherit $25 pilot unless config overrides) |
| `-Resume` | No | Continue from checkpoint |
| `-OutDir` | No | Report root override |

Environment gates (Node-enforced, not PS-enforced alone):

- `HIF_DURABLE_ROOT` — worktree-independent checkpoints/caches/ledgers (recommended: `C:\Dev\dealality\hif-durable`)  
- `HIF_APPLY_RESEARCH=1`  
- `HIF_APPLY_PROMOTION=1`  
- `HIF_PUBLICATION_AUTHORIZED=1` (independent; default off)  
- `HIF_FOUNDER_MANIFEST_SHA256=<exact sha256 of approved manifest file>`  

Canonical Promotion also requires an authorization JSON bound to: manifest hash, allowedActions, canonicalBaseId (`appCCUsuGsE1ifoLk`), recordIdentities, maxBatchSize, runId, expiresAt. Any mismatch **fail closed**.  

## Command examples

### MODE 1 — Research (dry discovery / extract / verify)

```powershell
.\scripts\hotel-intelligence-factory.ps1 `
  -Mode Research `
  -Stages "CONFIG,DISCOVER,EXTRACT,VERIFY,RECONCILE,REPORT" `
  -DryRun
```

### MODE 1 — Persist research (authorized)

```powershell
$env:HIF_APPLY_RESEARCH = "1"
.\scripts\hotel-intelligence-factory.ps1 `
  -Mode Research `
  -Stages "CONFIG,DISCOVER,EXTRACT,VERIFY,RECONCILE,PERSIST_RESEARCH,REPORT" `
  -ApplyResearch
```

### MODE 1 — Resume

```powershell
.\scripts\hotel-intelligence-factory.ps1 -Mode Research -RunId hif_research_20261120_ab12 -Resume
```

### MODE 2 — Build US creation manifest only

```powershell
.\scripts\hotel-intelligence-factory.ps1 `
  -Mode CanonicalPromotion `
  -Stages "CONFIG,RECONCILE,MANIFEST,REPORT" `
  -DryRun
```

### MODE 2 — Promote (forbidden without exact manifest auth)

```powershell
# Will fail closed unless env + manifest hash match
$env:HIF_APPLY_PROMOTION = "1"
$env:HIF_FOUNDER_MANIFEST_SHA256 = "<sha256>"
.\scripts\hotel-intelligence-factory.ps1 `
  -Mode CanonicalPromotion `
  -Manifest .\reports\sec-edgar-contract-timing-v4\HPC_CREATION_PILOT_MANIFEST.approved.json `
  -ApplyPromotion `
  -Stages "CONFIG,MANIFEST,PROMOTE,READBACK,QUEUE_HE,REPORT"
```

## Exit codes

| Code | Meaning |
|---|---|
| 0 | Success (including dry-run complete) |
| 2 | Config / permission gate failure |
| 3 | Budget exceeded |
| 4 | Validation / read-back failure (auto-stop) |
| 5 | Unexpected write detected |
| 10 | Checkpoint corrupt / resume impossible |

## Outputs (every run)

```
data/hotel-intelligence-factory/runs/{runId}/
  checkpoint.json
  cost-ledger.json
  config.fingerprint.json
  logs/events.jsonl
reports/hotel-intelligence-factory/{runId}/
  RUN_REPORT.json
  RUN_REPORT.md
  CHANGES.json          # NEW|UPDATED|UNCHANGED|SUPERSEDED|CONFLICTED
  MANIFEST.json         # when MANIFEST stage runs
```

## Future factory workflow (post-V4)

1. Scheduled RESEARCH (Windows Task Scheduler / CI cron) calling the same `.ps1 -Mode Research`  
2. Founder reviews manifests from report pack  
3. Explicit MODE 2 apply with hashed manifest  
4. Optional admin UI → same Node CLI args  

No second implementation path.
