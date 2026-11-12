# GDI Hotel Validation / Action / Outcome — Airtable Persistence

**Status:** IMPLEMENTED · sandbox-verified · focused PR  
**Contract reuse:** Decision & Outcome (`Decisions` + `Decision Events`) + share-validation latest store  
**No parallel feedback system**

## 1. Exact Airtable tables and fields used

| Layer | Base | Table | Role |
|-------|------|-------|------|
| Canonical | `appa2cE7FTRmIbB32` (`AIRTABLE_INTELLIGENCE_BASE_ID`) | `Decisions` | One Decision per hotel × GDI opportunity (`subjectId`) |
| Canonical | same | `Decision Events` | Append-only `VALIDATION` / `ACTION` / `OUTCOME` events |
| Fast UI latest | filesystem `share-validation.json` | n/a | Latest Hotel Validation projection per opportunity |
| Founder local | filesystem `founder-reviews.json` | n/a | `ASSIGN_*` only — never customer-published |

Existing Decision Event fields (unchanged schema): `eventId`, `decisionId`, `hotelId`, `subjectId`, `eventType`, `eventSubtype`, `eventValue`, `reason`/`note`, `userId`/`userRole`, `eventDate`/`reportedAt`/`createdAt`, `sourceSurface`, `rawEventJson`.

Hotel Validation values dual-write via `ingestGdiShareValidation` → typed VALIDATION events (familiarity, commercial value, contact person, email, phone). Action / Outcome use `recordAction` / `recordOutcome`.

## 2. New fields created

**No new Airtable columns.** Subsection stamps are API projections:

- `hotelValidationUpdatedAt`
- `salesActionUpdatedAt`
- `salesOutcomeUpdatedAt`

Documented in `docs/decision-outcomes-airtable-fields.md`. Local share-validation also stores `hotelValidationUpdatedAt` alias of `validatedAt` plus append-only `submissionHistory`.

## 3. Backend API changes

- Auth `POST …/customer-validation` — all validation fields + optional local founder review; returns `hotelValidationUpdatedAt`; 502 + rollback if Airtable dual-write fails; 409 on concurrency conflict.
- Share `POST …/validation` — same section contract; no founder review; dual-write must succeed for `ok: true`.
- Share/auth Action & Outcome — return section stamp only; identical re-submit → `created:false` / no history spam; 409 on stale expected stamp.
- Detail/list read-back merges validation stamp into commercial progression DTO; Action/Outcome types exposed for form prefill.

## 4. Frontend save and read-back

- Shared panel (`dealality-gdi-ui.js`) used by auth + share.
- Save / Save + Next / Save Action / Save Outcome: loading state, duplicate-click guard, navigate only after successful persist.
- Founder review control gated by `canFounderReview` (auth yes, share no).
- Prefills Action/Outcome from persisted progression.

## 5. Three independent last-saved timestamps

Muted `.gdi-last-saved` under each subsection:

- Hotel Validation → `gdiSvLastSaved`
- Action → `gdiDoActionLastSaved`
- Outcome → `gdiDoOutcomeLastSaved`

UTC ISO from server; displayed in viewer local timezone. “Not yet submitted” when absent. Only the saved subsection’s stamp updates.

## 6–7. Persistence verification (sandbox)

`npm run test:gdi-hotel-feedback-airtable-persistence-v1` (filesystem Decision Outcomes; no production customer records):

- Validation save → history append → identical re-submit does not bump stamp
- Action save leaves validation stamp unchanged
- Outcome save leaves action + validation stamps unchanged
- Conflict tokens reject stale writes
- Failed Airtable path rolls back local validation stamp via `restoreShareValidationItemAfterFailedPersist`
- UI markup contains three last-saved nodes

Also green: `test:gdi-customer-lifecycle-parity`, `test:decision-outcomes`.

## 8. Failed-save handling

- Dual-write failure → HTTP 502, local validation rolled back, UI keeps selections and shows error (no “Saved”).
- Save + Next does not navigate on failure.
- Buttons disabled during in-flight request.

## 9. Historical feedback / audit

- Decision Events remain append-only (canonical history for GDI learning).
- `share-validation.json` keeps latest values for fast UI + `submissionHistory` with previous values / submitter / section.
- Identical re-submits do **not** append misleading activity.

## 10. GDI learning integration

`hotelFeedbackToWeeklyLearningItem` → existing `feedbackToResearchTask` pipeline:

| Signal | Learning effect |
|--------|-----------------|
| Already known | suppress new-discovery label |
| Worth pursuing now | high follow-up priority |
| Wrong person / wrong email | contact enrichment |
| Not relevant / not worth pursuing | suppress recommendations (hotel-specific) |
| Actively pursuing / RFP received / Won / Booked | preserve pursuit; avoid conflicting automation |

Research evidence, opportunity inventory, and publication state are never overwritten by these writes.

## 11. Regression tests and results

| Test | Result |
|------|--------|
| `test:gdi-hotel-feedback-airtable-persistence-v1` | PASS |
| `test:gdi-customer-lifecycle-parity` | PASS |
| `test:decision-outcomes` | PASS |

UI verification artifact: `reports/gdi-hotel-feedback-airtable-persistence-v1/UI-VERIFICATION.html`.

## 12. GitHub PR and merge status

See PR opened from branch `gdi-hotel-feedback-airtable-persistence-v1` (this deliverable). Merge after required checks/approvals.
