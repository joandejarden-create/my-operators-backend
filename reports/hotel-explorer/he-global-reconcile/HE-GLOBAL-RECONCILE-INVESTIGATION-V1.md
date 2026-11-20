# Hotel Explorer Global Reconciliation — Investigation v1

**Generated:** 2026-11-20  
**Reconcile worktree:** `C:\Dev\dealality\wt-he-global-reconcile-v1`  
**Branch:** `feat/he-global-reconcile-entry-parity-v1` @ `a800e9cdc` (`origin/main`)  
**Paid spend:** $0  

## Preservation incident (mandatory)

At ~02:28 local, `ws-hotel-explorer` was switched from `feat/he50-full-explorer-population-v1` to `feat/he-beth-phase3-sources-evidence-v1` after a `reset` + stash. HE 5.0 WIP was **not discarded**.

| Location | State |
| --- | --- |
| `ws-hotel-explorer` | Clean on obsolete `feat/he-beth-phase3-sources-evidence-v1` (do not treat as HE home) |
| `stash@{0}` on HE | Still holds HE50 WIP |
| `C:\Dev\dealality\wt-he50-population-preserve-v1` | Preserve worktree with stash **applied** (dirty ≈159) — authoritative HE50 recovery |
| Permanent GDI / IC | Untouched by this restore |

## Server → checkout map

| Port | PID | Checkout / assets | `hotel-detail-panel.js` SHA256 prefix |
| --- | --- | --- | --- |
| **8080** | 45504 | `my-operators-backend` with `DEALALITY_CANONICAL_CHECKOUT=1` → serves **canonical-main** / `origin/main` assets | `24D7A742276F` (main) |
| **8090** | 51516 | HE50 WIP process (still serving pre-switch HE50 panel from memory/cwd) | `F776B2168AA8` (HE50) |
| **8091** | 50024 | Same HE50 panel family | `F776B2168AA8` |
| **8093** | 8448 | Same HE50 panel family | `F776B2168AA8` |

**Root cause:** three entry points are not one build. Radar/canonical 8080 runs merged `main` **without** the uncommitted HE50 Demand/Area Hotels fixes. Founder preview ports run the newer HE50 panel/API behavior.

## Live Bethesda Demand (same tab comparison)

| Entry | Demand Drivers Live | Area Hotels Live |
| --- | --- | --- |
| 8080 | **EMPTY** — `no_demand_anchors_in_airtable_for_geography` (false negative: Continent vs Region AND-match) | “Hotel Census pool unavailable… No other open hotels within 10 mi” |
| 8090 | **8 anchors** (NIH, Walter Reed, etc.) | Mayflower / Renaissance / Falls Church via HPC-native pool |

Access & Connectivity already returns items on both (6) — TI path largely shared; Demand filter was the primary API drift.

## Classifications (initial)

| Symptom | Class |
| --- | --- |
| 8080 vs 8090 Demand empty vs 8 | `API_VERSION_DRIFT` (+ unmerged HE50 fix) |
| Area Hotels Census messaging on 8080 | `UI_VERSION_DRIFT` / legacy path |
| Proposed Contacts richer than Live | `LIVE_PROPOSED_GOVERNANCE_DIFFERENCE` (standing auth: ownershipProposedMustRemainProposed) |
| Guest Sentiment Proposed 3 platforms / Live 0 | `PROVIDER_RESTRICTED` / rights gate |
| Mullen economic owner Proposed-only | `LIVE_PROPOSED_GOVERNANCE_DIFFERENCE` — do **not** promote portfolio listing to verified legal ownership |

## Fixes landed in reconcile worktree (not yet merged)

Ported from HE50 preserve (no permanent HE mutation):

1. `lib/hotel-intelligence/he-demand-access-read-v1.js` — country-only DA filter + Resort name radius inference  
2. `public/js/hotel-detail-panel.js` — Area Hotels HPC-native / Brand Presence (no Scout Census preference)  
3. `api/brand-presence.js` — HPC Brand Family mapping hardening  
4. `lib/travel-infrastructure/import-validation.js` — Airport point type allow  

## Next

1. Parity gate `scripts/test-he-three-entry-point-parity-v1.mjs`  
2. Restart/isolated server on reconcile wt to prove Demand+Area Hotels on one build  
3. Full 12-tab matrix + eight-hotel scorecard  
4. PR → Integration Controller (do not force-merge; do not deploy)  
