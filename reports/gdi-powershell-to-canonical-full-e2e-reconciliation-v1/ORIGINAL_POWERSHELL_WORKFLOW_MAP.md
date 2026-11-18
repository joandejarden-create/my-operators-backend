# Original PowerShell-driven workflow map

| Entry | Node target | Role |
|---|---|---|
| `scripts/gdi-workflow.ps1` | `gdi-universal-operational-workflow-v1.mjs` → often `gdi-research-to-page-workflow-v1.mjs` | Operational FullRun / Research / SyncAirtable |
| `scripts/Invoke-GdiScalingCompanies.ps1` | `gdi-scaling-companies-hotel-runner-v1.mjs` | 13-stage scaling Full E2E incl. buyer/contact intel |
| `scripts/gdi-local-tracks/Invoke-GdiLocalDemandTrack.ps1` | `gdi-canonical-full-e2e-entry-v1.mjs` | Already canonical |
| `scripts/gdi-nextgen-hunters/*.ps1` | `gdi-nextgen-hunter-run.mjs` | Discovery-only hunters |

Fourteen-segment Aimbridge/Bethesda live chains historically ran via **Node npm** (`gdi:full-e2e-fourteen-segment-chain` / orchestrator `--run-original-full-e2e-chain`), not a dedicated PS1.

### Scaling stage spine
hotel_profile → discovery → org identity → net-new → commercial screening → ORS longform → **buyer_and_contact_intelligence** → investigation loop → qualification → gold brief → payload → Airtable → share verify
