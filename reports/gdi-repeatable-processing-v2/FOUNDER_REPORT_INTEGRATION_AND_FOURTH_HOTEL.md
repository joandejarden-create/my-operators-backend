# GDI Repeatable Processing V2 — Integration & Universal New-Hotel Acceptance

**Authorization:** GO — $0 integration and validation  
**Date:** 2026-11-23  
**Final state:** `AWAITING_INTEGRATION_CONTROLLER`

## 1) #574 / #575 / #576 integration results

| PR | Title | Ancestry / status | CI | Action |
|---|---|---|---|---|
| **#574** | Design Hotels live research pilot | OPEN, MERGEABLE, independent of #575 | GDI recovery ✅ · gitleaks ✅ | Ready for IC merge **first** |
| **#575** | Customer UI contact + architecture | **MERGED** `aecc3e3146522cdf5da98e341ed0501eb7267348` (2026-11-23) | Green at merge | Done |
| **#576** | Universal cohort + dedupe | Rebased/merged `origin/main` (includes #575); head `4728ed5b8` | Admin/AVA ✅ · GDI recovery ✅ · Radar/ADP ✅ · Spice ✅ · gitleaks ✅ | Ready for IC merge **after #574** |

Preferred order reconciled: **#575 already on main** → merge **#574** → merge **#576**.  
#576 previously failed `html_cache_marker_architecture_v1` against main; fixed by merging #575 tip — architecture offline test PASS locally; GitHub CI green.

Primary Integration checkout is **detached + dirty** → GDI chat did **not** merge. Preserve dirty trees.

## 2) Main merge SHAs

- #575 merge SHA: `aecc3e3146522cdf5da98e341ed0501eb7267348`
- #574 / #576: **not yet merged** (awaiting IC)

## 3) CI results

- #574: SUCCESS (GDI recovery, gitleaks)
- #575: MERGED (historical green including architecture fix)
- #576: SUCCESS on post-rebase head (full required suite listed above)

## 4) Selected fourth HPC hotel

**AC Hotel A Coruña** — `rec2PVBDavppGpenm`  
Outside Design Hotels pilot (Condesa / LaLuna / Memmo). Existing verified HPC; no DesignHotelsLive-specific runner used.

## 5) Real HPC-to-config outcome

With live config temporarily aside (force HPC propose path):

- `ok: true`
- Identity/geography: AC Hotel A Coruña · A Coruña · Spain
- Rooms: **116** (capability/verified inventory)
- Meeting space: **7254 sq ft** (indoor room count unresolved → left null)
- Market: Matogrande / business corridor / A Coruña
- Unresolved (not guessed): `demandMix`, `commercialSegmentPriorities`
- Artifact: `fourth-hotel-hpc-to-config/rec2PVBDavppGpenm.proposal.json`

## 6) $0 CohortPreflight result

```text
ok: true
executeReady: false
spentUsd: 0
contract: gdi-universal-cohort-processing-v2
hotelIds: [rec2PVBDavppGpenm]
blockers: []
```

Command:

```powershell
.\scripts\Invoke-GdiPortfolioProcessing.ps1 -Mode CohortPreflight -HotelId rec2PVBDavppGpenm -BudgetUSD 0 -PerHotelBudgetUSD 0
```

## 7) Cross-segment deduplication verification

Frozen regression `scripts/test-gdi-repeatable-processing-v2.mjs` PASS:

- `carpha_dedupe`
- `advantage_dedupe`
- `selection_no_double_carpha`
- `contact_retain_without_email`
- `hotels_file_parse`
- `pilot_config_self_validate`

Local visibility materialize using the same V2 deduper: Condesa 87→82, LaLuna 66→50, Memmo 61→56. Advantage collapses to one canonical on Memmo. Distinct editions/projects remain separate by design.

## 8) Contact-display regression status

#575 MERGED and present on main tip used by visibility WT. Verified named contacts remain displayable without email when `customerFacingNamed` is authorized; restricted/unverified stay protected. Design Hotels pilot eligible/deep artifacts contain **zero** named stakeholders — Research tiles correctly show “Contact Research Required” (not a #575 regression).

## 9) Exact universal PowerShell command

See `UNIVERSAL_HOTEL_ONBOARDING_SOP.md`. Canonical one-hotel preflight:

```powershell
.\scripts\Invoke-GdiPortfolioProcessing.ps1 -Mode CohortPreflight -HotelId <HPC_RECORD_ID> -BudgetUSD 0 -PerHotelBudgetUSD 0
```

Cohort file:

```powershell
.\scripts\Invoke-GdiPortfolioProcessing.ps1 -Mode CohortPreflight -HotelsFile .\path\to\hotels.txt -BudgetUSD 0 -PerHotelBudgetUSD 0
```

`DesignHotelsLive*` remains available for historical regression only.

## 10) Remaining blockers to routine hotel processing

1. IC merge of #574 then #576 while primary is dirty (`SKIP_INTEGRATION_DIRTY`)
2. Founder authorization for budgeted `CohortExecute` when paid research is desired
3. Separate authorization for research Airtable persistence (still dry-run by default)
4. Optional: write proposed configs into `config/group-demand-intelligence/hotels/` after founder review of unresolved fields
5. Local founder UI on `:8080` still needs Design Hotels materialize or use visibility `:8097`

## Governance

No paid research · No Airtable writes/schema · No publication · No Findymail/Surfe · No deploy · No weekly automation · No broader portfolio research

**STOP for founder review.**

`AWAITING_INTEGRATION_CONTROLLER`
