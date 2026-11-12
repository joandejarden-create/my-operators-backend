# GDI Tab Persistence, Supervisor Integration & Performance Optimization

**Contract:** Founder production-readiness directive  
**Hotel:** Bethesda Marriott (`recLuxvwwxID7U2B8`)  
**Branch:** `feature/gdi-tab-perf-persistence-supervisor-v1`  
**Base main SHA (pre-merge):** `85857f966` (includes PR #213 / #214)  
**Date:** 2026-11-12

---

## A. Performance baseline (before fixes)

Measured against local `server.js` on `:8080` with `GDI_NEED_DATES_AIRTABLE_LIVE=1`.

| Interaction | Cold | Warm / median | Notes |
|---|---:|---:|---|
| HTML shell | ~2–5 ms | ~2 ms | Static file |
| Opportunities API | ~2.8–16 s (cold spikes) | ~0.73 s median historically; warm ~0.25 s after process cache | ~500 KB list DTO (19 cards) |
| Need-dates dashboard (Airtable) | **~81,000 ms** | ~81 s repeat | Match upserts on every GET |
| Need-dates dashboard (local isolation) | ~64–106 ms | ~36–64 ms | Shadow only |
| Tab 2 first activation (browser) | dominated by Airtable GET (~81 s) | remount + full refetch on every visit | Destroyed on leave Tab 1 |

Baseline smoking gun: Airtable need-dates GET performed **N sequential `upsertNeedPeriodOpportunityMatch` writes** during dashboard assembly (~110 matches for Bethesda).

---

## B. Root-cause ranking (evidence-based)

1. **Airtable match upserts on dashboard GET** — ~81 s → ~0.23–0.83 s after removal (**~100×**). Measured via direct `buildNeedDatesManagementDashboard({ useAirtable:true })`.
2. **Sequential Airtable reads** (periods → profile → history) on Tab 2 path — parallelized; secondary vs #1.
3. **Tab 2 unmount on every Opportunities render** — destroyed calendar/form state and forced full refetch; fixed with keep-alive.
4. **Opportunities critical path waited on hotel catalog before hotel-scoped fetches** — parallelized.
5. **Need-period Airtable re-read on every Opportunities list** (tile icons) when live flag on — 45 s in-process cache added; first warm miss still ~2.4 s, subsequent ~0.25 s.

Not primary: frontend CSS; opportunity eligibility filtering itself (correctness preserved).

---

## C. Performance fixes

| Fix | File(s) | Effect |
|---|---|---|
| Gate match persistence behind `persistMatches === true` (default off on GET) | `need-dates-management-service-v1.js` | Airtable Tab 2: **81 s → ~230–830 ms** |
| Parallelize periods + commercial profile + change history | same | Removes sequential Airtable wait |
| Keep Tab 2 mounted across tab switches; preserve DOM node | `app.js`, `hotel-need-times-priorities-tab.js` | Tab switch **~18–63 ms**; no unnecessary reload |
| Parallel `loadAll` (catalog + flag + summary + opps + profile) | `app.js` | Faster first paint when hotelId known |
| Need-period tile icon cache (45 s TTL) + invalidate on writes | `opportunity-need-date-tile-preview-v1.js`, `read-cache.js` | Warm Opportunities API ~0.25 s |
| Expand soft preference boosts (GROUP_ROOM_NIGHTS, WEEKDAYS, etc.) | `need-period-strategy-selection-v1.js` | Supervisor ranking reflects hotel priorities |

---

## D. Performance comparison (after)

| Metric | Before | After | Target |
|---|---:|---:|---|
| Need-dates Airtable API (median / p95, n=5) | ~81,000 ms | **235 ms / 266 ms** | Tab 2 < 2 s |
| Opportunities API warm median (n=5) | ~728 ms (prior session) | **250 ms** | List < 3 s |
| Opportunities API cold / cache-miss | up to ~16 s | **~2.4 s** (first after restart) | < 3 s usable |
| Tab switch (previously loaded, n=4) | remount + network | **18–63 ms** | < 300 ms |
| Returning to Tab 2 | full reload | **keep-alive, no refetch** | no unnecessary reload |
| Page usable (warm server, browser) | blocked on Tab 2 Airtable path when opened | Opportunities interactive with 19 cards; Tab 2 Airtable banner + calendar usable | page < 2 s / list < 3 s |

Limiting factor remaining: first Opportunities request after process restart still attaches need-date tile previews via Airtable (~2.4 s). Subsequent requests hit tile + read caches.

---

## E. Airtable persistence

**Effective base:** `appa2cE7FTRmIbB32` (AI Demand Positioning via `resolveNeedDateAirtableBaseId`)  
**Live flag:** `GDI_NEED_DATES_AIRTABLE_LIVE=1`  
**Tables verified:**

- `GDI Hotel Need Periods`
- `GDI Need Period Change History`
- `GDI Hotel Commercial Profiles` (+ change history contract)

### Need-period (isolated marker `PERF_AUDIT_TEST_DO_NOT_USE_*`)

1. Created TEST_SCENARIO period on Bethesda — present in Airtable list (`ndp_recLuxvw_mwdrervj_860f`).
2. Dashboard reload with `testScenarioOnly` contained the period.
3. Edited → priority `HIGH`, notes `… edited` persisted.
4. Cancelled → status `Cancelled`.
5. Waterstone period count unchanged (4 → 4) — **cross-hotel isolation OK**.

### Commercial profile

1. Pre-state was `LOCAL_SHADOW` (empty Airtable profile for Bethesda).
2. Wrote `LOCAL_CATERING` + `WEEKDAYS` → read-back `AIRTABLE_AI_DEMAND_POSITIONING` with values confirmed.
3. Restored prior empty priorities — matched prior.
4. UI Tab 2 banner: `Persisted in Airtable AI Demand Positioning · Mode: AIRTABLE_AI_DEMAND_POSITIONING`.

### Error / auth posture

- Hotel-scoped formulas on all need-period reads/writes.
- Commercial profile version conflict returns 409.
- UI save confirmation remains post-persistence (no change to optimistic “Saved” before write).
- Match upserts no longer run on GET (prevents silent write storms).

---

## F. Supervisor consumption

**Path verified:** saved commercial profile → `loadCommercialProfile` → `selectStrategiesForCampaign` (need-date orchestrator / dashboard strategy plan).

### Before/after (deterministic fixture)

| Profile | Top strategy | Soft reasons |
|---|---|---|
| none | association_events (76) / local_catering (76) | — |
| GROUP_ROOM_NIGHTS + WEEKDAYS | association_events **94**, scaling_companies **84** | `hotel_revenue_priority:GROUP_ROOM_NIGHTS`, `hotel_day_preference:WEEKDAYS` |
| LOCAL_CATERING | local_catering_day_meetings **88** | `hotel_revenue_priority:LOCAL_CATERING` |

- Order differs between rooms vs catering profiles: **yes**
- Soft only: strategies outside preference remain available (not hard-filtered)
- Paid research remains blocked (`paidExecutionBlockedUntilFounderGo: true`)

### Need-period matching behavior

- Need periods drive **strategy selection inputs** (`revenueNeed`) and **tile/match presentation** on opportunity cards.
- Need periods do **not** change customer-facing opportunity *eligibility* or founder publication (19 Bethesda cards unchanged).
- Need-period date windows affect match labels (Confirmed / Potential / No Current Match) — presentation + research prioritization, not automatic qualification.

### Remaining integration gap

- Core `runGroupDemandResearch` portfolio engine does **not** re-rank published Opportunities list by commercial profile on every page load (by design — preferences influence need-date research strategy selection / explanations, not publication filter).
- Soft boosts for `GROUP_ROOM_NIGHTS` / weekday preference were incomplete before this PR; now wired in `selectStrategiesForCampaign`.

---

## G. UI verification

- Tab 1 screenshot: Bethesda Opportunities, **All (19)**, Reset View only (no Run Research / Publish).
- Tab 2 screenshot: Airtable persistence banner, 8 active need periods, 2027 calendar, period cards.
- Browser tab-switch instrumentation: keep-alive mounted; switches &lt; 100 ms.
- Console: no blocking errors observed during Tab 1/2 navigation in verification session.
- Mobile: CSS/layout unchanged from PR #213; desktop verified live.

---

## H. Tests

| Suite | Result |
|---|---|
| `test:gdi-hotel-need-times-priorities-tab-v1` | PASS |
| `test:gdi-hotel-toolbar-reset-view-only-v1` | PASS |
| `test:gdi-qualified-customer-opportunity-feed-v1` | PASS |
| `test:gdi-customer-lifecycle-parity` | PASS |
| `test:gdi-hotel-feedback-airtable-persistence-v1` | PASS |
| `test:decision-outcomes` | PASS |

Opportunity count remains **19**. No paid research triggered. No customer auto-publication.

---

## I. GitHub

- **Files modified:**  
  - `lib/group-demand-intelligence/need-date-demand/need-dates-management-service-v1.js`  
  - `lib/group-demand-intelligence/need-date-demand/need-period-strategy-selection-v1.js`  
  - `lib/group-demand-intelligence/need-date-demand/opportunity-need-date-tile-preview-v1.js`  
  - `lib/group-demand-intelligence/read-cache.js`  
  - `public/js/group-demand-intelligence/app.js`  
  - `public/js/group-demand-intelligence/hotel-need-times-priorities-tab.js`  
  - `reports/gdi-tab-perf-persistence-supervisor-v1/FOUNDER-GDI-TAB-PERF-PERSISTENCE-SUPERVISOR-REPORT.md`
- **PR:** (filled after open)
- **CI / merge / final main SHA:** (filled after merge)

---

## J. Operational recommendation

### `PRODUCTION_READY_WITH_LIMITATIONS`

**Ready:** Tab 2 Airtable persistence verified live; match-on-GET write storm removed; tab keep-alive; soft preference supervisor ranking proven; regressions green; 19 Bethesda opportunities unchanged.

**Limitations:**

1. First Opportunities request after server cold start can still be ~2–3 s while need-period tile previews hydrate from Airtable (warm path ~0.25 s).
2. Commercial preferences soft-boost **need-date research strategy selection**, not the published customer opportunity membership set.
3. Need-period dates primarily affect match presentation / research prioritization, not automatic opportunity rejection or qualification.

Restart GDI `server.js` after deploy so the match-upsert removal is live in every process.
