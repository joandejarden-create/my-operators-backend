# GDI Priority + Action Window assignment contract v1

**Version:** `gdi_priority_action_assignment_v1`  
**Module:** `lib/group-demand-intelligence/priority-action-window/assignment-contract-v1.js`

## Four independent dimensions

| Dimension | Field(s) | Customer meaning | Must not be used for |
|-----------|----------|------------------|----------------------|
| **Prospect value / hotel fit** | `priority` = `HIGH_PRIORITY` / `MEDIUM_PRIORITY` / `WATCHLIST` | How strong this hotel’s pursuit value is | Timing, contact gaps, packaging |
| **Recommended action + timing** | `bookingWindowStatus` = `CONTACT_NOW` / `QUALIFY_NOW` / `WATCH` / `TOO_EARLY` | What to do next and when | Fit strength, packaging |
| **Commercial packaging class** | `prospectReviewSection` = `PRIORITY` / `EXPLORATORY` | Core prospect vs exploratory account (publication register) | **Not** High/Medium/Watch |
| **Customer-recorded work** | `actionStatus`, validation, outcomes, `decisionId` | What the hotel already did | Research assignment |

UI labels for packaging (avoid colliding with the Priority filter):

- `PRIORITY` → **Core prospect**
- `EXPLORATORY` → **Exploratory account**

## Priority rules (value / fit)

| Priority | Evidence required | Explicit non-reasons |
|----------|-------------------|----------------------|
| **High** | Strong scored fit (≥75 or ≥72 + capture band) + evidence ≥55 + usable contact + confirmed open buying / active RFP + Primary/Overflow type | Section=PRIORITY alone; unknown sourcing; stay-to-play; thesis-only without open buying |
| **Medium** | Credible hotel-fit thesis **or** `hotelRelevanceDisposition=READY_FOR_CARD` **or** fit ≥50; not too-early-only | Missing contact (contact is enrichment); unknown sourcing |
| **Watch** | No credible fit thesis/score, **or** future-cycle / too-early with credible fit but no pursue window | Default stamp because import forgot to assign |

## Action Window rules (timing / motion)

| UI label | Code | Evidence required | Explicit non-reasons |
|----------|------|-------------------|----------------------|
| **Pursue Now** | `CONTACT_NOW` | Confirmed open buying or active RFP / open sourcing with usable contact | Stay-to-play alone; housing provider alone; unknown sourcing; packaging section |
| **Contact** | `QUALIFY_NOW` | Usable contact + credible fit; investigate sourcing / relationship | Claiming confirmed rooms |
| **Watch** | `WATCH` | Insufficient fit for Contact, **or** no usable contact (enrichment), **or** intentional monitor | Unknown sourcing alone when fit+contact exist |
| **Watch–Too Early** | `TOO_EARLY` | Future cycle / planning window not open (≥2y with unknown planning) | Event date alone without planning evidence |

### Special cases

- **Venue partnership / referral:** Action = `QUALIFY_NOW` (relationship development). Do not invent an event date.
- **Placed host + possible overflow:** Action = `QUALIFY_NOW`, not Pursue Now.
- **No usable contact:** Action = `WATCH` with `enrichmentNeeded=true` — **not** a timing story. Priority may still be Medium if fit is credible.
- **Unknown / TBA sourcing:** Does **not** force Watch when fit + contact exist → Contact.

## Historical vs reviewed-package paths

| Path | How priority/window were set | Failure mode |
|------|------------------------------|--------------|
| **Legacy / scoring** (`classifyPriority` in `scoring.js`, qualification precision) | Computed from fit, evidence, booking window, qualification gates | Can produce High / Contact Now when gates pass |
| **Reviewed package import** (`reviewed-prospect-import-v1.js`) | Previously stamped `WATCHLIST` + `WATCH` whenever fields were empty — “honest browse defaults” | San Diego / Mayflower: all facing cards Watch/Watch despite READY_FOR_CARD + usable contacts + theses |
| **Contract v1 (this)** | Evidence assignment after contact package merge; only upgrades default Watch stamps; never demotes evidence-backed High / Pursue Now without `force` | — |

## Precedence

1. Customer feedback / actions / outcomes / `decisionId` — never overwritten by assignment.
2. Evidence-backed stored High / Contact Now / Qualify Now / Too Early — preserved unless `force`.
3. Default Watch / empty fields — reassessed by contract v1.
4. Visibility flags (`customerVisible`, watch-lane publication invariants) — **do not** set commercial priority/window.

## Persistence

On apply (when authorized):

- `priority`, `bookingWindowStatus`
- `priorityReason`, `bookingWindowReason`
- `priorityActionAssignmentVersion`, `priorityActionAssessedAt`
- `prospectCommercialClassLabel` (derived display)
