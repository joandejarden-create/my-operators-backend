# GDI commercial research improvement V1

Zero-write shadow evaluation after targeted engine fixes. No opportunity store, publication registry, share token, or Airtable record was written. Aimbridge research-only dossiers were not converted to customer opportunities.

Baseline commit: `828563da6795eac64320e54ae8c0cd305ba9e236` on `main`.

## Root causes

1. **Independent evidence was present but unread correctly for inferred demand.** Location detection required the literal word `city` / `downtown` / `held at`, so facts such as `Washington, DC — March 15–16, 2027` failed. Scale patterns ignored `150–250 peak rooms` and `~450 teams`. Source titles and structured attendance / peak-room fields were ignored.
2. **Named host-hotel facts were not treated as confirmed demand.** Text such as `Crystal Gateway Marriott named for March 7–10, 2027` stayed speculative because the explicit-demand regex required narrower “must book / stay-to-play” wording.
3. **Association buyer roles were discarded on dedupe.** `contactName` + short remit label `Meetings / housing` was kept; `primaryContact.role` `Senior Manager, Events and Member Services` was dropped. That made a true DIRECT_BUYER look irrelevant.
4. **Association / scaling demand gates accepted generated thesis wording.** `hotelDemandThesis` / `evaluateRealisticHotelDemandThesis` matched narrative fields even when the independent-evidence classifier said SPECULATIVE.
5. **Functional event mailboxes were mis-labeled as person-email misattribution.** Example: `collegeshowcase@loudounsoccer.com` blocked Account Development / Sales Ready despite being a valid ask-for route.
6. **Placeholder evidence values (`Official source`) were not rehydrated from `sourceTitle`.**

These were qualification / connection defects in the existing engine, not a missing research system.

## Engine improvements

| Area | Change |
| --- | --- |
| Demand classifier | Broader location / scale / activity recovery; named host-hotel confirmation; historical host + future timing/location → RELIABLE_INFERRED; source titles + structured attendance/peak rooms as independent context |
| Association buyers | Prefer richest job-title / remit label; keep better title on dedupe; penalize scraped residue |
| Shared eligibility | Enrich engine namedBuyers with primaryContact role; expand functional mailbox locals (`tournament`, `showcase`, …) |
| Remit classifier | Strong lodging/meetings remit labels may pass without a generic director/manager shape |
| Demand gates | Association + scaling realistic-demand gates now require the independent evidence classifier |
| Rehydrate | Placeholder `Official source` values recover `sourceTitle` into quote |
| Shadow audit | `personFrom` uses the same richer role selection |

## Before / after (identical 649-row evaluator)

| Metric | Before (fact-text audit) | After |
| --- | --- | --- |
| Opportunities assessed | 649 | 649 |
| CONFIRMED_DEMAND | 3 | 12 |
| RELIABLE_INFERRED_DEMAND | 0 | 15 |
| SPECULATIVE_DEMAND | 643 | 620 |
| CONTRADICTORY_OR_NEGATIVE | 3 | 2 |
| Sales Ready (shadow) | 0 | 15 |
| Verified relevant contacts (DIRECT_BUYER + ENTRY_POINT) | labeled but not progressing | 85 |
| Account Development (shadow) | 0 | 0 (rows that clear demand+buyer+route become Sales Ready) |

### Priority hotels

| Hotel | Confirmed | Reliable inferred | Speculative | Sales Ready |
| --- | --- | --- | --- | --- |
| Bethesda Marriott | 10 | 6 | 41 | 7 |
| Renaissance New York Times Square | 1 | 5 | 66 | 4 |
| Hilton New York Times Square | 0 | 4 | 41 | 4 |
| Wyndham San Diego Bayside | 0 | 0 | 82 | 0 |
| Waterstone | 0 | 0 | 55 | 0 |
| Spice Island (research store; published 7 untouched) | 1 | 0 | 76 | 0 |
| YOTEL Geneva | 0 | 0 | 99 | 0 |

### Aimbridge research-only

Sheraton Guadalajara Expo, voco Cancún, and Wyndham Alltra remain RESEARCH_ONLY. No `opportunities.json` import. Dossier publication status stays `NOT_AUTHORIZED`.

## Evidence / contact recovery examples

- **NADO 2027 overflow** — host-hotel fact → CONFIRMED; buyer role recovered to Senior Manager, Events and Member Services → DIRECT_BUYER → Sales Ready.
- **ACTS TS27** — dates + city + peak rooms / attendance → RELIABLE_INFERRED; Events Manager → Sales Ready.
- **AHIMA Advocacy 2027** — historical host + 2027 city/dates → RELIABLE_INFERRED; Spargo sponsorship contact remains research continuation (not a lodging buyer).
- **Loudoun College Showcase** — stay-to-play CONFIRMED; functional showcase mailbox no longer blocks; Tournament Director retained as entry point → Sales Ready.
- **Potomac Memorial** — CONFIRMED demand retained; Tournament Director still lacks evidenced demand-link for Sales Ready (correctly stays research continuation).

## Regression

- `npm run test:gdi-evidence-demand-mapping-v1` — PASS
- `npm run test:gdi-complete-hotel-inventory-audit-v1` — PASS
- `npm run test:gdi-full-e2e-path-restoration-v1` — PASS
- `npm run test:gdi-foundation` — 2 unrelated local UI failures (`dealality-app-shell` / Evidence Confidence copy) from dirty workspace assets; not caused by these engine edits

Publication safeguards held: no store writes, Spice Island’s seven approved published ids unchanged, share registries untouched, Bethesda research inventory not merged into published inventory.

## Remaining limitations

- Most of the 620 speculative rows still lack recoverable independent fact text (many only store `Official source` + URL). New paid or targeted research is required before those can qualify.
- Wyndham San Diego Bayside and Waterstone have contacts but almost no independent demand text in the store.
- International / multilingual hotels (YOTEL, Spice Island research rows, Aimbridge dossiers) still need segment-aware evidence extraction, not only classifier fixes.
- Sales Ready here is **shadow evaluation only**. Customer publication still requires separate authorization.
- Account Development is thin because the shared gate collapses to Sales Ready once demand + buyer + route clear; a distinct mid-layer for “demand OK, hotel-fit incomplete” remains future work.

## Proposed paid-research budget (awaiting authorization)

Do **not** spend without explicit founder approval.

| Scope | Purpose | Bound |
| --- | --- | --- |
| Bethesda continuation set (12 ranked future rows with contacts) | Recover missing excerpts / lodging language from known URLs | ≤ $75 Exa/ORS |
| Renaissance + Hilton NYTS speculative rows with DIRECT_BUYER contacts | Fill independent activity / scale evidence | ≤ $100 |
| Wyndham San Diego Bayside top 20 contact-bearing rows | Evidence recovery only (no publish) | ≤ $75 |
| Aimbridge dossiers (Guadalajara / Cancún / Punta Cana) | Multilingual evidence + contact pass; research-only | ≤ $125 |

Total proposed ceiling: **$375**. Zero publish. Zero production store mutation.

## Scorecard

| Layer | Result |
| --- | --- |
| Engine execution | PASS (zero-write shadow) |
| Research quality | IMPROVED but NOT fully verified — 27 substantiated rows; 620 still speculative |
| Commercial qualification | IMPROVED in shadow — 15 Sales Ready; not a customer-publication certification |
| Customer delivery | NOT re-certified in this pass |
| Publication safety | PASS |
| Full E2E | NOT VERIFIED |
