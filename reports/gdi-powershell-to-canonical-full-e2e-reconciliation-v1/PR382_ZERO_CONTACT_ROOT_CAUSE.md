# PR #382 zero-contact root cause

## Record-level evidence

| Metric | Value |
|---|---|
| Processed opportunities | 16 |
| Rows with contactsInvestigated > 0 | 10 |
| Rows with salesReadyContacts > 0 | 6 |
| Rows with primaryContactName on processed | 0 |
| Sum contactsInvestigated | 25 |
| Sum salesReadyContacts | 10 |
| Scorecard claimed zero named | true |
| State files with primary names (after recovery read) | 10 |

## Causes (ordered)

1. **PRESENT_BUT_NOT_INVOKED (reporting)** — `namedContact()` on stripped processed rows.
2. **Honest demand gates** — EXCLUDE/WATCHLIST dispositions keep sales-ready at 0 even with named entry points (do not weaken).
3. **Not** "contact research skipped".
4. **Not** missing Surfe — intentional.
5. Soft factor: evidence quality / geography for some Exa hits limited commercial demand class, so entry points exist without sales-ready promotion.

## Combination answer

Primarily (reporting serialization) + (demand qualification rejecting EXCLUDE/WATCHLIST). Contact research executed.
