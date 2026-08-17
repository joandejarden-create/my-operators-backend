# Scout Signal Watchlist (Phase 3)

**Purpose:** Persist selected Scout opportunity signals for review, assignment, and outreach workflow — without auto-saving every generated signal.

**Table:** `Scout Opportunity Signals` on Deal Capture Platform (`AIRTABLE_BASE_ID_ALT`)

**Schema ensure:**

```bash
node scripts/ensure-scout-opportunity-signals-table.mjs --dry-run
node scripts/ensure-scout-opportunity-signals-table.mjs --apply
```

---

## Safety rules

| Rule | Detail |
|------|--------|
| No auto-save | Only explicit `POST /save` writes a signal |
| Hotel Census | Read-only for generation; **never written** by Scout watchlist |
| Radar / Brand Explorer | Not modified by Scout watchlist |
| No AI enrichment | Phase 3 is rules-based signals + human review only |
| Idempotent upsert | `Signal ID` is the unique key; re-save updates the same row |
| PATCH scope | Review fields only — not core signal facts |

---

## Review statuses

| Status | Use |
|--------|-----|
| New | Default on first save |
| Watchlist | Tracking for follow-up |
| Researching | Active diligence |
| Ready for Outreach | Qualified for owner/operator contact |
| Dismissed | Not pursuing |
| Deal Created | Linked to deal workflow (manual) |

---

## Endpoints

| Method | Path | Writes? |
|--------|------|---------|
| GET | `/api/scout/opportunity-signals` | No — annotates `saved*` metadata when a signal exists |
| POST | `/api/scout/opportunity-signals/save` | Yes — Scout Opportunity Signals only |
| GET | `/api/scout/opportunity-signals/saved` | No |
| PATCH | `/api/scout/opportunity-signals/:signalId` | Yes — review fields only |

### POST save body

```json
{
  "signal": { "...full generated signal object..." },
  "reviewStatus": "Watchlist",
  "internalNotes": "",
  "assignedTo": ""
}
```

### PATCH body (allowed fields)

- `reviewStatus`
- `internalNotes`
- `assignedTo`
- `createDeal` (boolean)

**Not patchable:** Signal Type, Country, Market, Submarket, Hotel Name, Priority Score, Reason, Supporting Metrics JSON.

---

## Watchlist workflow

1. **Generate** — `GET /api/scout/opportunity-signals` with filters (read-only Hotel Census).
2. **Review** — user selects signals in UI (future).
3. **Save** — `POST /api/scout/opportunity-signals/save` with full signal payload + review status.
4. **Manage** — `GET /saved` for queue; `PATCH /:signalId` for status/notes/assignment.
5. **Re-generate** — `GET /api/scout/opportunity-signals` returns `saved: true` and review metadata on matching `signalId`.

---

## STR geography

Saved signals store `Market` and `Submarket` as the official STR geography fields (same as Phase 1–2). No separate STR Market / STR Submarket columns.

---

## Testing

```bash
node scripts/test-scout-signal-watchlist.mjs
```

---

## Related

- `docs/scout-market-coverage.md` — Phase 1 coverage + Phase 2 signals overview
- `lib/scout/scout-signal-watchlist.js` — Airtable I/O
- `lib/scout/opportunity-signals.js` — read-only generation (unchanged logic)
