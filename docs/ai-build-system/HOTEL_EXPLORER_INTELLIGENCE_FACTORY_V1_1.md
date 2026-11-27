# Hotel Explorer Intelligence Factory V1.1

Repeatable research → Airtable → API → Live Hotel Explorer orchestration.

**Principle:** Research once. Persist once. Validate completely. Refresh repeatedly.

PowerShell is a thin wrapper. All logic lives in Node modules under `lib/hotel-intelligence/he-factory/`.

## Commands

### PowerShell

```powershell
.\scripts\hotel-explorer\Invoke-HotelExplorerFactory.ps1 -HotelId recLuxvwwxID7U2B8 -Mode Audit
.\scripts\hotel-explorer\Invoke-HotelExplorerFactory.ps1 -HotelName "Bethesda Marriott" -Mode Full
.\scripts\hotel-explorer\Invoke-HotelExplorerFactory.ps1 -HotelId recLuxvwwxID7U2B8 -Mode Apply -Apply
.\scripts\hotel-explorer\Invoke-HotelExplorerFactory.ps1 -BatchFile configs/hotel-explorer/he50-eight-hotel-batch-v1.json -Mode Audit
```

### Node (CI / scheduler)

```bash
node scripts/hotel-explorer/run-hotel-explorer-factory.mjs --hotel-id recLuxvwwxID7U2B8 --mode Audit
node scripts/hotel-explorer/run-hotel-explorer-factory.mjs --hotel-name "Bethesda Marriott" --mode Full
node scripts/hotel-explorer/run-hotel-explorer-factory.mjs --hotel-id recLuxvwwxID7U2B8 --mode Apply --apply
node scripts/hotel-explorer/run-hotel-explorer-factory.mjs --batch-file configs/hotel-explorer/he50-eight-hotel-batch-v1.json --mode Audit
```

## Modes

| Mode | Behavior |
|---|---|
| Audit | Read-only dispositions (persisted / mapped / missing / conflict / disabled) |
| Research | Reuse saved evidence first; paid only under ceiling + authorization |
| Apply | Controlled Airtable null-fill; default dry-run; requires `--apply` / `-Apply` |
| Validate | Airtable → API → Live (browser optional) |
| Full | Audit → Research → Apply (dry unless -Apply) → Validate |
| Refresh | Revalidate; update only verified changes; preserve history |

## Historical facts (Bethesda)

- **Year Built 1979** — HPC has no `Year Built` column. Value lives in `Opening Date` and must hydrate `hotel.yearBuilt` in Radar/brand-presence DTOs.
- **Last Renovated 2015** — Primary property-wide renovation on `Renovation / Conversion Date`. 2017 guestroom unveil / Cvent year is a separate milestone, not a replacement.

## Gates

`HE_FIELD_REGISTRY_COMPLETE` · `HE_RESEARCH_REUSE_VALIDATED` · `HE_HISTORICAL_FACTS_VERIFIED` · `HE_CANONICAL_PERSISTENCE_VALIDATED` · `HE_AIRTABLE_READBACK_VALIDATED` · `HE_API_HYDRATION_VALIDATED` · `HE_LIVE_RENDER_VALIDATED` · `HE_ELEVEN_TAB_BROWSER_VALIDATED` · `HE_FACTORY_REPEATABILITY_VALIDATED`

Reports: `reports/hotel-explorer/he-factory-v1-1/`

## Safety

- No legacy Hotel Census.
- No Surfe auto-invoke.
- Eight-hotel paid ceiling $50 (not reset by factory).
- Dirty HE / GDI / IC worktrees preserved; develop on focused branch/worktree.
