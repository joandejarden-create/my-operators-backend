# Full E2E persistence integration

## Wiring

`runCanonicalFullE2eResearch` → optional `persistFullE2eResearchInventory` (`gdi-full-e2e-research-persistence-bridge-v1`) → `upsertResearchInventoryRecord`.

CLI flags:

- `--plan-research-persistence` (dry-run plan after research)
- `--authorize-research-persistence`
- `--exact-write-set-authorization <token>`
- `--apply-research-persistence`
- `--research-persistence-write-budget <n>`
- `--readback-research-persistence`

## Controls

| Control | Behavior |
|---------|----------|
| Default | No persistence call unless plan/authorize flags set |
| Apply without gates | Forced dry-run + `blockedApply` |
| Env gate | `GDI_RESEARCH_INVENTORY_PRODUCTION_WRITES=1` required |
| GDO | Never written by bridge |
| Publication | Flag rejected on bridge |
| YOTEL legacy 108 | Skip forbidden recreate IDs |
| Ledger | `research-inventory-persistence-ledger.json` in run outDir |

## Status

Automatic production persistence remains **disabled** pending founder activation.

