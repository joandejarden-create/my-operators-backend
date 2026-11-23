# GDI Design Hotels Local Server Visibility V1 — Founder Report

**Authorization:** GO — $0 local integration and validation  
**Status:** LOCAL PASS on dedicated review server — STOP before customer publication / Airtable persistence  
**Date:** 2026-11-23

## Verdict

Root cause was **not** missing pilot research. Authentic Design Hotels pilot artifacts exist under `reports/gdi-design-hotels-live-research-pilot-v1/`, but the running local servers (`:8080`, `:8091`) did not have (1) hotel selector configs for the three HPC IDs and (2) materialized `data/.../hotels/{hpc}/opportunities.json` research stores (Airtable persistence was dry-run only).

Fix: isolated visibility worktree + configs + canonical V2 dedupe materialization of authentic eligible candidates into local research inventory. Review URL:

**http://127.0.0.1:8097/group-demand-intelligence.html?inventory=research&live=1&founderPreview=1**

## Inventory table (Research mode, post V2 dedupe)

| Hotel | HPC ID | Raw eligible | After dedupe | Saved research inventory (UI) | Deep-selected | Hotel | Competitive | Sales Ready | Named contact coverage | Server source | UI |
|---|---|---:|---:|---:|---:|---:|---:|---:|---|---|---|
| Condesa DF | `rece0NdV9FCnaFlVS` | 87 | 82 | **82** | 4 | 82 | 0 | 0 | 0 verified names in pilot eligible/deep (Contact Research Required is correct) | Temp WT `:8097` @ `aecc3e314` + local materialize | PASS |
| LaLuna | `recaokE8psIbuVeuH` | 66 | 50 | **50** | 4 | 50 | 0 | 0 | 0 verified names in pilot artifacts | same | PASS |
| Memmo Príncipe Real | `rec1t35e3sPerzxYu` | 61 | 56 | **56** | 4 | 56 | 0 | 0 | 0 verified names in pilot artifacts | same | PASS |

Certification benchmarks matched after applying `gdi-cross-segment-opportunity-dedupe-v2` during materialize (87→82 / 66→50 / 61→56).

## Root cause

1. **Wrong worktree on founder ports:** `:8080` still served a 22-hotel selector without Design Hotels configs (API counts 0/0/0).
2. **Research never landed in FS store:** pilot Airtable bridge was dry-run (`AUTHORIZE_RESEARCH_PERSISTENCE_REQUIRED`); no `opportunities.json` existed for the three HPC IDs on the active server checkout.
3. Hotel selector source = `config/group-demand-intelligence/hotels/*.json`; Research inventory source = `data/group-demand-intelligence/hotels/{id}/opportunities.json` via `?live=1` → `FOUNDER_RESEARCH_INVENTORY`.

## Changes made

- Isolated WT: `feat/gdi-design-hotels-local-visibility-v1` from `origin/main` tip `#575` (`aecc3e314`)
- Copied Design Hotels GDI configs (three HPC IDs)
- Added `scripts/gdi-design-hotels-local-research-materialize-v1.mjs` (idempotent; `--force` for rematerialize; no STAGED/CURRENT; no Airtable)
- Applied canonical cross-segment dedupe V2 during materialize
- Booted dedicated local server on **:8097** with founder Research preview flags

## Artifacts missing / inaccessible

- Pilot reports were present in the Repeatable V2 WT / PR #574 lineage; not on `:8080` checkout.
- Named stakeholders: **none** in eligible or deep-selected pilot JSON — UI “Contact Research Required” is authentic, not a regression of #575 contact-name auth.

## Browser validation

- Condesa / LaLuna / Memmo selectable; Research mode nonzero; All/Hotel/Competitive counts match inventory; drawers open with demand evidence; hotel switch replaces inventory; STAGED/PUBLISHED not fabricated.
- Screenshots: `reports/gdi-design-hotels-local-visibility-v1/screenshots/`

## Integration

| PR | State | Notes |
|---|---|---|
| #574 | OPEN / MERGEABLE / CI green | Design Hotels live research + configs |
| #575 | MERGED `aecc3e314` | Contact + architecture cache fix |
| #576 | OPEN / CI green after rebase onto #575 | Universal cohort + dedupe |
| Visibility | This branch / PR (pending) | Local materialize + configs for review |

Primary Integration checkout is **dirty + detached** → do **not** local-merge from GDI chat. Hand #574 → #576 to Integration Controller.

## Remaining blockers

1. IC merge of #574 then #576 (primary dirty: `SKIP_INTEGRATION_DIRTY`)
2. Founder `:8080` still on old WT until configs/materialize land on that checkout or founder uses `:8097`
3. No customer STAGED/PUBLISHED packages (intentional)
4. Residual LaLuna CARPHA near-duplicates may remain when org strings differ sharply (2 titles after V2 vs 5 before); known pair covered by frozen regression

## Governance

No paid research · No Findymail/Surfe · No Airtable production writes · No customer publication · No production deploy

**STOP for founder review.**
