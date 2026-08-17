# Active Universe → Public-Full Program

Orchestrates getting all **24 Active/Live** Brand Explorer brands to public-full + PVQL-clean.

## Source of truth

- Brand Basics `Brand Status` Active/Live
- `lib/partner-intelligence/brand-explorer-active-universe.js`
- `lib/brand-status-active.js`

Operational cohorts (PRIMARY_RELEASE, Lane 1/2 restore lists, prior 23) are **not** the universe.

## Lanes

1. `pvql-public-full-scrub` — 16 current public-full PVQL failures
2. `restored-pending-validation` — Quality Inn / Radisson / Blu / RED
3. `everhome-remediation` — Everhome targeted fixes
4. `unconfigured-full-build` — BW Premier / BW Signature / Preferred (full Tab Factory)
5. `final-public-full-validation` — inventory + next validation commands

## Run

```bash
npm run brand-explorer-active-universe-to-public-full-program -- --dry-run
npm run brand-explorer-active-universe-to-public-full-program -- --lane pvql-public-full-scrub --dry-run
npm run brand-explorer-active-universe-to-public-full-program -- --lane restored-pending-validation --dry-run
npm run brand-explorer-active-universe-to-public-full-program -- --lane everhome-remediation --dry-run
npm run brand-explorer-active-universe-to-public-full-program -- --lane unconfigured-full-build --dry-run
npm run brand-explorer-active-universe-to-public-full-program -- --lane final-public-full-validation --dry-run
```

## Forbidden

Company Validated, Source Library, Registry, Brand Status, Radisson Collection / Tapestry status, stale 23-brand universe.

## Final acceptance (2026-07-23)

| Check | Result |
|-------|--------|
| Active universe | **24** (`reconcilesTo24=true`) |
| public-full | **24 / 24** |
| `shouldRenderFullProfile` | **24 / 24** |
| PVQL public-full inventory | **24 lockPass** (`reports/_tmp-active-universe-pvql-inventory.json`) |
| Display state | `active_profile_ready` for all 24 |
| OS PRIMARY_RELEASE (7) | all `active_profile_ready` → `no_action` |
| Mandatory release gates | PASS |
| Excluded | `radisson-collection` (Draft), `tapestry-collection-by-hilton` (Under Review) — Brand Status unchanged |

Lane residual remediations (session):
- Lane 4 Recent Momentum announcement cards + Preferred thicken
- BW Premier/Signature `valueOwners.overview` rows (empty Owner Education Overview)
- OS aligned with PVQL: announcement-URL exceptions + live Presentation blocks preferred over stale factory rows

Latest: `reports/brand-explorer-active-universe-to-public-full-program.json`

