# PowerShell ↔ canonical workflow status

## Current state

| Path | Status |
| --- | --- |
| `npm run gdi:canonical-full-e2e` → `gdi-canonical-full-e2e-entry-v1` → `executeOriginalFullE2eResearchChain` | **Authoritative research chain** |
| `scripts/gdi-workflow.ps1` → `gdi-universal-operational-workflow-v1` → `gdi-research-to-page-workflow-v1` | **Historical FullRun / Research coordinator** (still used for Validate / SyncAirtable / Review / Publish / Verify) |
| Dual independent research pipeline | **Must not be restored** |

`gdi-workflow.ps1` remains a thin orchestration shell (no competing business logic). Default `Research` / `FullRun` still plans/invokes the legacy `gdi-research-to-page` coordinator unless `-Canonical` is passed.

## Recommended alignment (implemented safely)

Add optional `-Canonical` on `gdi-workflow.ps1` for `Research` / `FullRun` so PowerShell becomes a thin wrapper around `scripts/gdi-canonical-full-e2e-entry-v1.mjs` (dry-run by default; paid flags never auto-authorized). Leave Validate/Sync/Publish on the universal workflow.

- PS1 contains Canonical switch: **true**
- Universal `stageResearch` still documents legacy script: **true**

## Do not

- Reintroduce a second discovery/deep/contact engine inside PowerShell
- Auto-pass `--authorize-paid` / `--allow-paid` from the wrapper
