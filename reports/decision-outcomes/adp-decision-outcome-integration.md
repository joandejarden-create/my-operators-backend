# ADP ↔ Decision & Outcome Integration

## Decision creation

`ensureAdpFindingDecision` (`lib/decision-outcomes/adp-bridge.js`):

- Creates (or idempotently returns) a Decision for an **actionable** ADP finding/recommendation.
- Module: `ADP`; type: `POSITIONING_ACTION`.
- Subject: `ADP_FINDING` (or `DEMAND_SEGMENT` when a segment is provided); `subjectId` = finding id.
- Idempotent on hotel + finding + `recommendationVersion`.
- Evidence snapshot includes `findingId` + freeze time; methodology `adp_finding_confidence_v1`.

Also exposed as `POST /api/hotels/:hotelId/decisions/ensure-adp`.

## Validation / action / outcome enums

Defined in `lib/decision-outcomes/types.js`:

**Validation**

- Type: `HOTEL_RESPONSE`
- Values: `AGREE`, `DISAGREE`, `ALREADY_ADDRESSING`, `ALREADY_COMPLETED`, `NOT_ACTIONABLE`, `NEED_HELP`, `UNSURE`

**Action**

- `WEBSITE_CONTENT_UPDATED`, `BRAND_SITE_CONTENT_REQUESTED`, `OTA_CONTENT_UPDATED`, `LOCAL_CONTENT_UPDATED`, `STRUCTURED_DATA_UPDATED`, `REVIEW_REPUTATION_ACTION`, `SOURCE_CITATION_ACTION`, `MARKETING_ACTION`, `BRAND_ESCALATION`, `OTHER_ACTION`, `NO_ACTION`

**Outcome**

- `PRESENCE_IMPROVED`, `CONSIDERATION_IMPROVED`, `SEGMENT_POSITION_IMPROVED`, `CITATION_IMPROVED`, `COMPETITOR_DISPLACEMENT_IMPROVED`, `NO_MEASURABLE_CHANGE`, `DECLINED`, `TOO_EARLY_TO_MEASURE`, `UNKNOWN`

Bridge helpers: `recordAdpHotelResponse`, `recordAdpAction`.

## Measurement suggestion (no causation)

`suggestAdpMeasurementOutcome`:

- Compares optional before/after metrics (or `tooEarly: true`).
- Sets `temporalAssociation: "movement_observed_after_action"` when a delta is observed.
- Sets `causalConfidence: UNKNOWN` always by default.
- Outcome note language: movement observed after action — **does not prove** the recommendation caused the change.
- Optional `write: true` appends an Outcome event via `recordOutcome` (`sourceSurface: adp_measurement_reconciliation`).

## Customer UI note

Canonical bridge + API ensure path are in place. **ADP customer UI for validation/action/outcome remains light** relative to GDI sections 19–20 — tracked as a non-blocking gap in the release verdict.
