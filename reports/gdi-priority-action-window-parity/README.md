# GDI Priority + Action Window parity

## Deliverables

| File | Contents |
|------|----------|
| `CURRENT_STATE_RECONCILIATION.md` | Trace + five-hotel facing reconciliation |
| `ASSIGNMENT_CONTRACT.md` | Explicit shared rules |
| `SIMULATION.md` | Before/after counts + change causes |
| `IMPLEMENTATION_PROOF.md` | Code + tests + repeatability |
| `five-hotel-facing-snapshot.json` | Frozen facing rows |
| `simulation-report.json` | Full simulation |
| `production-change-set.json` | **Prepared patches — not applied** |

## Direct answers

### A. Are all five hotels using the same rules?

**No today.** Bethesda / Waterstone / Renaissance largely use legacy scoring + qualification (`classifyPriority` and prior stamps). San Diego and Mayflower facing cards were published through the **reviewed-package import**, which left `priority` / `bookingWindowStatus` empty and then stamped **WATCHLIST + WATCH**. Contract v1 is now wired into future reviewed imports; production reclassification of existing rows is prepared but **not applied**.

### B. Why does San Diego show 21 Watch / 21 Watch?

Every one of the 21 customer-facing opportunities has:

- `priority = WATCHLIST` and `bookingWindowStatus = WATCH` from import defaults  
- while also having `hotelRelevanceDisposition = READY_FOR_CARD`, a hotel-fit thesis, and a usable contact route  

The counters are **consistent with stored fields**, but the stored fields are **not evidence-assigned research decisions**. They are empty-field defaults so browse facets were not “All(N) with every bucket 0”.

Publication “19 Priority + 2 Exploratory” is **`prospectReviewSection` packaging**, not the Priority High/Medium/Watch filter. Live facing packaging is **17 Core + 4 Exploratory**.

### C. Did visibility workarounds affect commercial assignments?

**No for SD/Mayflower stamps.** Watch-lane visibility helpers treat `WATCHLIST` as a publication lane for Ready/Watch audits; they did not write the import defaults. Commercial assignment fields were set by reviewed-import defaults (and legacy scoring elsewhere).

### D. Which specific corrections are justified?

1. **SD ×21 and Mayflower ×11:** upgrade default Watch → **Medium + Contact (`QUALIFY_NOW`)** — usable contact + credible fit/relevance; unknown sourcing must not force Watch; do **not** invent High or Pursue Now.  
2. **Bethesda ×~10 action-only:** upgrade default `WATCH` action → **Contact** where fit+contact exist; **preserve** existing High / Pursue Now / Too Early.  
3. **Waterstone ×1:** same action upgrade pattern.  
4. **Renaissance:** no patches.  
5. **UI packaging pill:** rename “Priority” → **“Core prospect”** so it does not collide with the Priority filter.  
6. **Do not** bulk demote legacy High cards or change publication eligibility to reconcile labels.

### E. What production changes would result?

If `production-change-set.json` is applied (43 patches):

| Hotel | Facing Priority H/M/W | Facing Action P/C/W/TE |
|-------|----------------------|------------------------|
| San Diego | 0 / **21** / 0 | 0 / **21** / 0 / 0 |
| Mayflower | 0 / **11** / 0 | 0 / **11** / 0 / 0 |
| Bethesda | 4 / 15 / 13 (unchanged) | 8 / **18** / 4 / 2 |
| Waterstone | 1 / 6 / 2 | 0 / **9** / 0 / 0 |
| Renaissance | 0 / 4 / 4 | 0 / 8 / 0 / 0 |

Customer feedback, actions, outcomes, and `prospectReviewSection` preserved. **Await explicit apply authorization** before writing Airtable.

## Commands

```bash
node scripts/test-gdi-priority-action-window-assignment-v1.mjs
node scripts/gdi-priority-action-window-simulate-v1.mjs
```
