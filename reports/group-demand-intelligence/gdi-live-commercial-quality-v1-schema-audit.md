# GDI Live Commercial Quality V1 — Schema Audit

Marker: `gdi_live_commercial_quality_v1`  
Starting SHA: `5c3303d31a280f5c93d9d5ada98be2e3237a19ab`  
Branch: `deploy/adp-final-trust-closure-20260910`  
Working tree: **dirty** (~1243 paths) — this task commits **only** focused GDI commercial-quality files.

## Live surfaces

| Surface | URL | FE | API |
|---|---|---|---|
| Auth browse | `/group-demand-intelligence.html` | `app.js`, `dealality-gdi-ui.js` | `GET …/opportunities` |
| Auth detail | drawer `#gdiDrawer` | same | `GET …/opportunities/:id` |
| Share browse/detail | `/group-demand-intelligence-share.html?share=` | `share-app.js` + shared UI | share resolve + opportunities |
| This Week | property-bar select | `weeklyThisWeekFilterHtml` | fields on opp JSON |
| Validation / Action / Outcome | drawer | lifecycle HTML | customer-validation + `/api/decisions/*` + share variants |
| Export | **MISSING pre-V1** | — | — |

Bethesda share token **preserved**: `gdisht_47c25d74c79216021fb36150` in `config/client-share/gdi-share-registry/active-tokens.json`.

## Concept → status

| Concept | Status | Canonical field(s) | Notes |
|---|---|---|---|
| opportunityId | EXISTS | `id` / `opportunityId` | Stable; must survive weekly refresh |
| event name | EXISTS | `title` | Airtable `opportunityName` |
| organization | EXISTS | `organizationName` | |
| series | PARTIAL | `eventSeriesKey` / V4 `eventSeriesId` | Unify via live CQ normalize |
| cycle | PARTIAL | `opportunityType=FUTURE_CYCLE` / V4 `eventCycleId` | |
| dates | EXISTS | `eventStartDate`, `eventEndDate` | |
| eventDateStatus | PARTIAL | V4 only | Promote to live projection |
| eventDateGranularity | **MISSING** | — | **Add** EXACT/RANGE/MONTH/YEAR/UNKNOWN |
| location | PARTIAL | `eventLocationSummary`, `destinationStatus` | |
| venue | PARTIAL | `venueStatus`, `venueSourcingStatus` | Name often missing in UI |
| sourcing | PARTIAL | dual taxonomies | Keep both; reconcile action |
| room demand | EXISTS | `roomDemandStatus` | Extend lodging thesis labels |
| attendance | PARTIAL | `estimatedAttendance` vs `attendance` | Alias both |
| peak rooms | PARTIAL | `estimatedPeakRooms` vs `peakRooms` | Alias both |
| WHO/contact | EXISTS | `primaryContact` | Hierarchy display |
| suggested action | PARTIAL | `recommendedAction` | Reconcile before display |
| sources | PARTIAL | `sources[]` | Sourceless guard |
| firstSeen / lastSeen | PARTIAL | weekly-delta fields | Preserve on correction |
| weekly status | EXISTS | `weeklyDeltaState`, `isNewThisWeek` | **List DTO gap** — fix |
| validation / action / outcome | EXISTS | Decision Events + share-validation | Extend reason taxonomy |
| hotelDemandThesis | PARTIAL | V4 vs `hotelOpportunityThesis` | Alias for UI |
| eventAliases | PARTIAL | WHO only | Add on opportunity |
| NEW_TO_GDI | PARTIAL | V4 flag | Display separately from NEW_TO_HOTEL |

## Do not duplicate

- Keep Decision / Validation / Action / Outcome in `lib/decision-outcomes/`
- Reuse V4 guards: local/no-room, overflow catchment, future-cycle invention, source-required, series identity
- Prefer `recommendedAction` (UI) over inventing a second action field; store reconciler output as `recommendedAction` + `actionBasis` + `actionReconciledV1`

## Gaps this V1 closes

1. Date granularity + year-floor ban  
2. Future-cycle UNCONFIRMED display  
3. Action reconciler (venue-lock / overflow / local / past)  
4. List DTO commercial + weekly fields  
5. Detail commercial evidence sections + related opportunities  
6. CSV export with stable Opportunity ID  
7. Hotel validation reason taxonomy (mapped into Decision layer)  
8. Bethesda live correction (dry-run → apply) without rediscovery  
