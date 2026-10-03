# Spice Island Beach Resort — Closure

**HPC:** `recKRJjcPnb4tVDDS`  
**ADP:** `adp_spice_island_beach_resort` · 46 active attributes after sync  
**HI_COMPLETE:** YES (ledger refreshed 2026-10-03)

## Why historically RESEARCHED_NO_READY

Prior cycles (`proven-source-replication-v1`, `hidden-demand-source-expansion-v2`) found future lodging-supported events but held them for:

- `surface_eligibility`
- weak WHO / named contact
- `commercial_not_open` / fully placed housing
- boutique **64-suite** luxury fit vs large island incentives

This is primarily a **public-data + fit ceiling**, not HI incompleteness.

## This sprint

| Step | Result |
|------|--------|
| HI completeness | skip-complete → force refresh → HI_COMPLETE |
| ADP attribute sync | 46 active; 1 deactivate; missing indoor meeting totals |
| GDI seed | WEEKLY_READY applied |
| Discovery dry-run (maxQueries=25) | candidates ~19 · trueActionable 0 · VALID_WATCH 1 · DRY_RUN_WOULD_PROMOTE signal |
| Discovery apply (2026-10-03, skip-contact) | queries 25 · fetches 36 · candidates 21 · VALID_WATCH 1 · INSUFFICIENT 1 · INVALID 19 · promotions **HOLD_WATCH 2** · customerVisible **0** · Surfe 0 |

### Apply hold titles

1. Shipbuilding and Aluminum Conference 2026 — `HOLD_WATCH`
2. Tugs, Towboats and Barges 2027 — `HOLD_WATCH`

## Exact bottleneck

Even with lodging-direct pages and a dry-run promote signal, apply refused customer promotion under current gates (surface eligibility / WHO / commercial path / boutique fit). Lowering gates would fake readiness.

## Closure outcome

**B. Documented true public-data ceiling** for Strict Ready volume on this property class under current qualification law.

Future Watch inventory retained (including the 2 HOLD_WATCH from this cycle). No Strict Ready without better public surface/WHO evidence.

## Verdict

**ADP READY — GDI PUBLIC DATA CEILING**
