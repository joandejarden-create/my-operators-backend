# GDI ↔ Decision & Outcome Integration

## Decision creation

`ensureGdiOpportunityDecision` (`lib/decision-outcomes/gdi-bridge.js`):

- Creates (or idempotently returns) a Decision for a hotel GDI opportunity.
- **Skips** when `opportunity.priority === "DISQUALIFIED"` (`skipped: "disqualified"`).
- Subject: `GROUP_OPPORTUNITY` / opportunity id; type: `OPPORTUNITY_PURSUIT`.
- Recommendation from `recommendedAction` → thesis → summary → title fallback.
- Freezes evidence snapshot summary + `confidenceMethodologyVersion: gdi_evidence_confidence_v1`.

Also exposed as `POST /api/hotels/:hotelId/decisions/ensure-gdi`.

## Dual-write: auth feedback + share validation

| Surface | Legacy store | Canonical ingest | Behavior |
|---------|--------------|------------------|----------|
| Authenticated GDI feedback | `feedback.json` via `saveFeedbackItem` | `ingestGdiAuthFeedback` | Maps familiarity / commercial → Validation; mappable sales outcomes → Action and/or Outcome |
| Share validation | `share-validation.json` | `ingestGdiShareValidation` | Validation events only (familiarity, commercial, contact/email/phone) |

Wired in `api/group-demand-intelligence.js`:

- Feedback POST dual-writes after legacy save; failure is logged, legacy response still succeeds.
- Share validation POST dual-writes the same way.

Legacy stores remain source of truth for existing GDI UX; Decision & Outcome is additive.

## Action / Outcome UI (detail sections 19–20)

Authenticated GDI opportunity drawer (`public/js/group-demand-intelligence/app.js`):

- **§18** — Hotel validation (feedback) — dual-written server-side.
- **§19. Action** — select + “Record Action” → `ensure-gdi` then `POST /api/decisions/:id/actions`.
- **§20. Outcome** — select (+ optional loss reason) → `ensure-gdi` then `POST /api/decisions/:id/outcomes`.

Copy in UI: action does not imply outcome; revenue fields not required.

## Share surface

Share path dual-writes **validation only**. Action/outcome are **not** exposed on share by default (`ingestGdiShareValidation` comment + no §19/§20 controls in `share-app.js`).
