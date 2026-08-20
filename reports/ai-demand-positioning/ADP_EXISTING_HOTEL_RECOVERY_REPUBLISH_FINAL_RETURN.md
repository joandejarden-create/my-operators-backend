# Existing Hotel ADP Recovery — Final Return (Republish + Deploy)

**Date:** 2026-08-20  
**Commit:** `6b98e3a` on `app-shell-left-nav`  
**Diff artifact:** `reports/ai-demand-positioning/adp-existing-hotel-recovery-republish-diff-2026-08-20T22-50-24.json`  
**Backup:** `reports/ai-demand-positioning/recovery-republish-backup/`

## A. REPUBLISH RESULT

| Property | Classification | Status |
|----------|----------------|--------|
| Waterstone Boca Raton | `ACTIVE_OFFICIAL_BASELINE` (`ADP_OFFICIAL_BASELINE_PERIOD_001`) | **PASS** |
| Renaissance Times Square | `ACTIVE_OFFICIAL_BASELINE` | **PASS** |
| Cambridge Beaches Bermuda | `ACTIVE_OFFICIAL_BASELINE` | **PASS** |
| NOW NOW NOHO | `ACTIVE_OFFICIAL_BASELINE` | **PASS** |
| Hotel Phillips Kansas City | `CERTIFIED_STANDALONE` (`ADP_HOTEL_PHILLIPS_BASELINE_PERIOD_001`) | **PASS** |

- No new observation periods
- No new LLM calls
- Presence Index formula unchanged
- No Airtable writes
- Core KPI rates unchanged for all five (no material KPI stop)

## B. OLD VS NEW (material)

### Numerical KPIs (unchanged)

| Property | Consideration | Scenario Presence | Demand Capture | Presence Index |
|----------|---------------|-------------------|----------------|----------------|
| Waterstone | 48.9 → 48.9 | 70.5 → 70.5 | 70.5 → 70.5 | unchanged |
| Renaissance | 16.4 → 16.4 | 40 → 40 | 43.1 → 43.1 | unchanged |
| Cambridge | 70.8 → 70.8 | 98.3 → 98.3 | 98.3 → 98.3 | unchanged |
| NOW NOW NOHO | 3.3 → 3.3 | 12.7 → 12.7 | 88.9 → 88.9 | unchanged |
| Phillips | 34 → 34 | 77.8 → 77.8 | 77.8 → 77.8 | unchanged |

### Naming / entity cleanup (changed)

| Property | Area | Old → New |
|----------|------|-----------|
| Waterstone | Top alt / comps | Eau Palm Beach Resort → Eau Palm Beach Resort & Spa; Boca Raton Resort → The Boca Raton; Four Seasons alias cleanup |
| Renaissance | Top alt / comps | The Knickerbocker Hotel → The Knickerbocker; Marriott Marquis New York → New York Marriott Marquis; removed “This hotel” fragment |
| Cambridge | Top alt | prose-fragment top alt → The Reefs Resort & Club |
| NOW NOW | Top alt / comps | Bowery-led → Beekman / Walker Hotel Greenwich Village ranking shifts |
| Phillips | Top alt / comps | Museum Hotel → Hotel Kansas City (Unbound Collection); no NYC leak |

### Structural / copy (changed)

| Area | Old | New | Reason |
|------|-----|-----|--------|
| Action `expectedImpact` | Numeric promise strings | `null` + neutral `impactNote` | Causality not validated |
| Trend payload | 1 point missing cons/scen rates | date + considerationRate + scenarioPresenceRate | Baseline Trend UI |
| Evidence empty | Silent empty drawers (code path) | Explicit unavailable messaging (UI) | Recovery fix |

Displacement entity counts change because aliases merge (e.g. Eau short + long → one canonical). Not a KPI formula change.

## C. DEPLOY PATH (exact)

```
Local published JSON
  data/ai-demand-positioning/published/<propertyId>/manifest.json
  data/ai-demand-positioning/published/<propertyId>/report-<periodId>.json
  data/ai-demand-positioning/published/<propertyId>/evidence-<periodId>.json
  (+ fixtures/ai-demand-positioning/published/ seed mirror)
        │
        ▼  git commit + push (Railway from GitHub; .railwayignore keeps ADP published + allowlisted runtime)
Railway deploy → node server.js (filesystem in container)
        │
        ▼
published-read-service.js priority:
  1) Airtable Live ONLY if ADP_PUBLISHED_READ_SOURCE=airtable or ADP_AIRTABLE_READ_LIVE=1
     → currently OFF / unconfigured (not used this step)
  2) loadPublishedReport → data/.../published (primary)
  3) else fixtures/.../published (seed fallback)
  4) else compute from runtime period (lean deploys may lack full runtime)
        │
        ▼
GET /api/ai-demand-positioning/property/:propertyId/report
GET /api/ai-demand-positioning/property/:propertyId/evidence
        │
        ▼
public/owner-ai-demand.html (+ share) via ai-demand-positioning.js
```

**Production SoT = filesystem published JSON in the deployed app image.** Airtable is optional overlay only.

## D. PRODUCTION VERIFICATION

### Pre-deploy (measured)

Production was **stale** vs corrected local for the official four (still on older period IDs):

| Property | Prod period (before) | Prod cons/scen | Local corrected |
|----------|----------------------|----------------|-----------------|
| Waterstone | `…20260819144128_fb2e16` | 53.6 / 62.8 | 48.9 / 70.5 (`…a69590`) |
| Renaissance | `…20260819144128_a7cbcc` | 17.1 / 41.5 | 16.4 / 40 |
| Cambridge | `…20260819144130_743505` | 73.3 / 100 | 70.8 / 98.3 |
| NOW NOW | `…20260819211510_a2c3fa` | 3.4 / 3.2 | 3.3 / 12.7 |
| Phillips | `…21bf47` (same period) | 34 / 77.8 match KPIs | impacts/trends still old on prod |

### Post-deploy

Requires Railway finish after push of `6b98e3a`. Re-run prod vs local compare; expect period IDs + KPIs + null impacts + trend rates to match local.

## E. TREND STATUS

- Published trends now include `considerationRate` + `scenarioPresenceRate` on the single baseline point.
- UI shows baseline date + rates + “Awaiting next comparable period” (no invented delta).
- Official four remain on `ADP_OFFICIAL_BASELINE_PERIOD_001`; Phillips stays `ADP_HOTEL_PHILLIPS_BASELINE_PERIOD_001` (not added to official four).

## F. ENTITY STATUS

Centralized customer entity resolution applied on republish:

- Eau / Boca / Four Seasons canonicalization: **yes** (Waterstone)
- Prose-fragment top alternatives reduced: **yes** (Cambridge top alt fixed)
- NYC→KC leak prevention: **yes** (Phillips comps stay KC)
- Residual: Cambridge still shows one prose-like displacement string in lost-demand list — residual entity-quality gap, not KPI

## G. EVIDENCE STATUS

| Path | Status |
|------|--------|
| Displacement evidence links | Working in recovery tests (0 empty modals on audited comps) |
| Intent “Missing” evidence | Works; capped excerpts |
| Explicit empty-state copy | Shipped in UI code |
| Scenario-ID hyperlink UX redesign | **Deferred** (incomplete by design this step) |

## H. REMAINING PRODUCTION DEFECTS

1. **Until Railway deploy completes:** production still serves pre-baseline period snapshots for the official four.
2. **Cambridge residual prose fragment** in displacement entities.
3. **Provider-row denominators:** Consideration correctly uses `comparableObservations` (Phillips 244); some provider presence rows still use full scenario totals (e.g. gemini `total: 63`) — confirm omit-missing display consistency in a follow-up (do not fill failures as zero for rates).
4. **Airtable reconciliation** blocked pending `ADP_AIRTABLE_BASE_ID` + scoped PAT.
5. Deferred product work: Mockup V2, Design System lock, Index display guardrail, section/responsive redesign, New Build ADP, scenario-ID evidence UX.

## I. NEXT RECOMMENDED STEP

**Do not auto-start.** After Railway marks `6b98e3a` deployed:

1. Re-run production API compare for all five properties (periodId + KPIs + impacts + trend rates).
2. Playwright smoke on production owner ADP pages.
3. Only then schedule Airtable reconciliation as a separate credentialed follow-up.
