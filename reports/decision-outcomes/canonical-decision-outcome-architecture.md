# Canonical Decision & Outcome Architecture

**Status:** Implemented (filesystem v1)  
**Schema:** `decision_outcome_v1`  
**Founder admin:** `/decision-outcomes-audit.html` → `GET /api/admin/decision-outcomes/audit`

## Purpose

Shared Decision & Outcome layer for GDI, ADP, and future modules. Permanent law: **never collapse** RECOMMENDATION / VALIDATION / ACTION / OUTCOME into one field or one overwriteable row.

## Storage (Airtable primary + filesystem audit mirror)

**Canonical durable store:** Deal Capture MVP Airtable (`AIRTABLE_BASE_ID`).

| Table | Id (MVP) |
|-------|----------|
| Decisions | `tblCtv34VZWTApQ5W` |
| Decision Events | `tblLVeeHQ0iyMHFqn` |

Filesystem under `data/decision-outcomes/hotels/{hotelId}/` remains an **audit mirror** after successful Airtable writes (not a second source of truth). See `reports/decision-outcomes/airtable-persistence-architecture.md`.

```
data/decision-outcomes/   # optional mirror
  hotels/{hotelId}/
    index.json
    decisions/{decisionId}.json
    events/{decisionId}.jsonl
```

- Decision row = mutable metadata (status summary, recommendation text, ids).
- Events = append-only history of validation / action / outcome.
- Current UI state is a **projection** over events, not the sole stored truth.

## Library modules (`lib/decision-outcomes/`)

| Module | Role |
|--------|------|
| `types.js` | Enums: modules, decision types, subjects, lifecycle, GDI/ADP validation·action·outcome, causality |
| `store.js` | Paths, idempotency key, load/save decisions, append events, hotel boundary |
| `service.js` | `createDecision`, `recordValidation` / `recordAction` / `recordOutcome`, list/get/timeline |
| `projection.js` | `projectCurrentState`, `deriveLifecycle` from event history |
| `gdi-bridge.js` | `ensureGdiOpportunityDecision`, auth + share ingest dual-writes |
| `adp-bridge.js` | `ensureAdpFindingDecision`, hotel response / action / measurement suggestion |
| `metrics.js` | Hotel / module / GDI·ADP phase-1 metric helpers |
| `index.js` | Public exports |

## API routes

Mounted in `server.js` (Memberstack + Dealality user):

| Method | Path |
|--------|------|
| `POST` | `/api/decisions` |
| `GET` | `/api/decisions/:decisionId` |
| `GET` | `/api/decisions/:decisionId/timeline` |
| `POST` | `/api/decisions/:decisionId/validations` |
| `POST` | `/api/decisions/:decisionId/actions` |
| `POST` | `/api/decisions/:decisionId/outcomes` |
| `GET` | `/api/hotels/:hotelId/decisions` |
| `GET` | `/api/hotels/:hotelId/decisions/metrics` |
| `GET` | `/api/hotels/:hotelId/subjects/:subjectId/decision` |
| `POST` | `/api/hotels/:hotelId/decisions/ensure-gdi` |
| `POST` | `/api/hotels/:hotelId/decisions/ensure-adp` |
| `GET` | `/api/admin/decision-outcomes/audit` |

Handlers live in `api/decision-outcomes.js`.

## Lifecycle

Derived in `deriveLifecycle` (not a single overwrite):

```
RECOMMENDED → VALIDATED → ACTION_PLANNED | ACTION_TAKEN → OUTCOME_RECORDED
```

Also: `OUTCOME_PENDING`, `CLOSED` (e.g. `NO_ACTION` after validation). Outcome present always wins → `OUTCOME_RECORDED`.

## Idempotency

`buildDecisionIdempotencyKey` = SHA-256 (first 24 hex) of:

```
hotelId|productModule|decisionType|subjectId|recommendationVersion
```

Same key → return existing decision (`created: false`). New recommendation version → new decision (preserves prior recommendation text).

## Causality (ADP measurement)

`CAUSAL_CONFIDENCE` defaults to **`UNKNOWN`** for ADP measurement reconciliation. Metric movement is temporal association only — never asserted as proof that the recommendation caused the change. See `suggestAdpMeasurementOutcome` in `adp-bridge.js`.

## Non-collapse invariants

- Multiple validations of different types coexist; latest-by-type is projected.
- Recording an outcome does not erase or rewrite prior validations.
- Recording an action does not imply an outcome (`latestOutcome` stays null until an outcome event exists).
- Hotel boundary enforced on read/write (`assertHotelBoundary`); metrics/lists are hotel-scoped.
