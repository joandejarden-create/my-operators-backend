# Full E2E Architecture and Script Inventory

## Canonical workspace evidence

- Primary: `C:\Dev\dealality\my-operators-backend`
- Branch/HEAD at audit: `main` / `a4224cc9967f607e30093e35d62cc4ecd89ad019`
- Discipline: `.cursor/rules/gdi-github-branch-merge-discipline.mdc` (PR #344)

## Authoritative contracts (CONFIRMED)

| Component | Path | Role |
|-----------|------|------|
| Full E2E contract | `lib/group-demand-intelligence/workflow/full-e2e-contract-v1.js` | Completion rules; wired via `scripts/gdi-research-to-page-workflow-v1.mjs --mode full-e2e` |
| Fourteen-segment discovery execution | `.../workflow/full-e2e-fourteen-segment-discovery-execution-v1.js` | Segment traversal + executor |
| Executor registry | `.../workflow/full-e2e-fourteen-segment-discovery-executor-registry-v1.js` | All 14 segments → shared `segment-discovery-executor-v1.js` as `PARTIAL_EXECUTOR` |
| Original research chain | `.../workflow/original-full-e2e-research-chain-v1.js` | Chains fourteen-segment discovery |
| Autonomous hotel E2E v2 | `.../workflow/gdi-autonomous-hotel-full-e2e-v2.js` | Alternate hotel Full E2E |
| Hotel template | `.../template/gdi-full-e2e-hotel-template-v1.js` + `npm run gdi:full-e2e-hotel-template-v1` | Repeatable hotel template artifacts |
| Execution observability | `.../observability/*` + `data/group-demand-intelligence/execution-observability/` | PR #336 ledgers |
| Provider inventory | `.../observability/gdi-provider-inventory-v1.js` | Config ≠ execution map |
| Publication packages | `.../bundled-publication-packages/<hotelId>/CURRENT.json` | Customer-visible SoT when CUSTOMER_PUBLISHED |
| Local/external activation gate | `.../share/gdi-local-external-customer-link-activation-gate-v1.js` | Parity gate (YOTEL path) |
| Weekly refresh | `.../workflow/weekly-refresh-contract-v1.js` + `weekly-refresh-orchestrator-v1.js` | Change classes / refresh |

## Classification

| Path | Class |
|------|-------|
| Fourteen-segment discovery executor + registry | **Canonical and active** (PARTIAL per segment) |
| `gdi-research-to-page-workflow-v1.mjs --mode full-e2e` | **Canonical entry** (CONFIRMED contract comment) |
| Exa agent adapters + segment request titles | **Active supporting** |
| Apify fulle2e orchestration + actor registry | **Active supporting / default mock** |
| Hotel campaign scripts (`gdi-yotel-*`, `gdi-bethesda-*`, `gdi-radisson-*`, `gdi-sheraton-*`) | **Campaign-specific** — often bypass “one generic command” for paid/scale |
| Weekly refresh orchestrator | **Partially implemented** for automatic weekly (publication OFF until enabled) |
| Webhound | **Legacy / hard-case learning only** |
| SerpAPI | **UNUSED** in Full E2E inventory |

## Duplicate / competing implementations (CONFIRMED)

- Multiple Full E2E orchestrator modules (`original-full-e2e-research-chain`, `gdi-autonomous-hotel-full-e2e-v2`, `gdi-three-hotel-full-e2e-v1`, many hotel scripts).
- Association/scaling specialized hotel runners coexist with shared segment executor.
- Weekly Change UI statuses vs backend CHANGE_CLASS enums (see Weekly Change audit).

## npm entry points (sample)

- `gdi:full-e2e-hotel-template-v1`, `gdi:full-e2e-certification-execute`, `test:gdi-full-e2e-execution-observability-v1`
- Activation: `gdi:activate-yotel-geneva-35343009-approved`, Radisson activate scripts
- Parity: `gdi:yotel-geneva-external-client-publication-parity-v1`, Bethesda/Radisson drawer parity scripts

## Required result

There **is** a designed canonical fourteen-segment Full E2E architecture, but day-to-day hotel outcomes often come from **authorized campaign scripts + publication packaging**, with historical ledgers showing **PARTIAL / UNKNOWN** segment execution. Preserve the shared executor registry; do not invent a second orchestrator — wire, instrument, and certify the existing one.
