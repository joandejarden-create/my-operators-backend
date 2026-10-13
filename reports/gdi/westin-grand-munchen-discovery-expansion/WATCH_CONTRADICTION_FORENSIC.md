# Watch Contradiction Forensic — Westin Grand München

## Contradiction

Report said VALID FUTURE WATCH = 0 but TOP WATCH = IAA TRANSPORTATION 2026 (customer-facing WATCH).

## Trace — IAA TRANSPORTATION 2026

| Field | Value |
|-------|-------|
| Opportunity ID | `gdi_opp_iaa_transportation_2026_4_f9ilhwc9` |
| Before CFS | WATCH |
| Destination | **Hannover** |
| Dates | 2026-09-15 → 2026-09-20 |
| Valid Future Watch (before) | false (STALE) |
| Reasons | past_or_stale_cycle |
| Munich relevance | **NO** |
| Classification | **B_NOT_VALID_WRONG_DESTINATION** |
| After CFS | CLOSED |
| After Valid Watch | false |

## Verdict

**B — IAA is not a Valid Future Watch; customer-facing WATCH label was wrong.**

Root causes stacked:
1. **Wrong destination** — IAA TRANSPORTATION 2026 is in **Hannover**, not Munich.
2. **Past cycle vs NOW=2026-10-07** — event end 2026-09-20 → STALE under production Watch gate.
3. **Report snapshot bug** — prior e2e SUMMARIES picked customerFacingState=WATCH as "top watch" even when `isValidFutureWatch` failed (stale narrative, not gate count bug).

Valid Future Watch count was **correctly 0**. The report label was the defect.

## Other invalid Watch labels cleared

- `gdi_opp_munich_security_conference_2026_21_f9ilhwc9` Munich Security Conference 2026 — STALE → CLEAR_INVALID_WATCH_LABEL
- `gdi_opp_stadtgeburtstag_2027_und_2028_8_f9ilhwc9` Stadtgeburtstag 2027 und 2028 — RESEARCH_BACKLOG_NOT_WATCH → CLEAR_INVALID_WATCH_LABEL
- `gdi_opp_sounds_of_royalty_13_f9ilhwc9` Sounds of Royalty — STALE → CLEAR_INVALID_WATCH_LABEL
- `gdi_opp_typo3_conference_3` TYPO3 Conference — RESEARCH_BACKLOG_NOT_WATCH → CLEAR_INVALID_WATCH_LABEL
- `gdi_opp_independent_hotel_show_munich_2026_10_f9ilhwc9` Independent Hotel Show Munich 2026 — STALE → CLEAR_INVALID_WATCH_LABEL
- `gdi_opp_leadership_retreat_17_f9ilhwc9` Leadership Retreat — ENTITY_INVALID → CLEAR_INVALID_WATCH_LABEL
- `gdi_opp_euralarm_s_annual_event_3_f9ilhwc9` Euralarm's Annual Event — STALE → CLEAR_INVALID_WATCH_LABEL

| Metric | Before | After |
|--------|--------|-------|
| Valid Future Watch gate | 0 | 0 |
| customerFacing *WATCH* labels | 8 | 0 |
