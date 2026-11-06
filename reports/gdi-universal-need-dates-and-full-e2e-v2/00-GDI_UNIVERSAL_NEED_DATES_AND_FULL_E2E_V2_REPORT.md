# GDI_UNIVERSAL_NEED_DATES_AND_FULL_E2E_V2_REPORT

**Status:** `GDI_UNIVERSAL_NEED_DATES_AND_FULL_E2E_V2_BLOCKED` (partial delivery — see blockers)  
**Branch:** `feat/gdi-universal-need-date-intelligence-v1`  
**PR:** #152 (open; not merged)  
**Related research PR:** #151 (open; dirty worktree preserved; not merged)

## Founder correction applied

- The prior **18** hotels are the GDI **research-selectable** set (`listGdiSelectableHotels`), not Dealality’s full hotel inventory.
- Need Dates no longer treat that selector as the universal denominator.
- Canonical Need-Date inventory denominator = **GDI-eligible config registry** (`listCanonicalGdiEligibleHotels`) → **27** census `rec…` hotels on this branch (provisional `gdi_hotel_*` aliases excluded).
- GDI research status and need-date status are independent layers.

## A. Canonical hotel coverage

| Metric | Count |
| --- | ---: |
| Canonical GDI-eligible inventory | 27 |
| GDI research-selectable subset | 18 |
| Research completed (local artifacts) | 15 |
| Research not started | 12 |
| HOTEL_CONFIRMED need dates | 1 |
| MARKET_ESTIMATED need dates | 1 |
| NOT_ESTABLISHED | 25 |

Source: `hotel-layer-audit.json`

## B. Airtable reconciliation

| Item | Value |
| --- | --- |
| Authoritative base | AI Demand Positioning `appa2cE7FTRmIbB32` |
| Authoritative table | **GDI Hotel Need Periods** |
| Access | Read-only (`GDI_NEED_DATES_AIRTABLE_LIVE=1` for recovery script) |
| Writes | **None** |

### Original two hotels

1. **Bethesda Marriott** (`recLuxvwwxID7U2B8`)
   - **8** Active `HOTEL_REPORTED` periods recovered from Airtable
   - Periods: Nov–Dec 2026; Jan–Feb 2027; Jul–Aug 2027; Nov–Dec 2027
   - Local UI demo stub (`need-periods.json` with 2 synthetic local IDs) **quarantined** → `need-periods.local-demo-quarantined.json`
   - Authoritative local mirror: `need-periods.airtable-mirror.json` (8 periods)
   - Precedence: **Airtable mirror > non-demo local FS**; demo stubs ignored

2. **Waterstone Resort & Marina Boca Raton** (`recgMYovrrZDJMqzX`)
   - Airtable rows: **4** founder **TEST_SCENARIO / lifecycle** records — **not** hotel-confirmed
   - Authoritative hotel-confirmed count: **0**
   - Local `market-estimated-need-periods.json` remains a segregated hypothesis (auth-only; share demotes)
   - Must not be presented as hotel-entered occupancy gaps

### Conflicts / unresolved

- Waterstone has **no** hotel-confirmed Airtable calendar; only test scenarios + local market estimate.
- Three eight-cohort hotels are **absent** from this branch’s GDI config registry (see §C).
- Full Dealality census inventory (beyond GDI-eligible configs) is **not** adopted as Need-Date denominator (avoids duplicate/unrelated census rows per founder guidance).

## C. Eight-hotel research cohort

| Hotel | Config | Need-date | Research | Local opps | Gap |
| --- | --- | --- | ---: | ---: | --- |
| Bethesda Marriott | yes | HOTEL_CONFIRMED | COMPLETED | 54 | — |
| Wyndham San Diego Bayside | yes | NOT_ESTABLISHED | COMPLETED | 82 | need dates |
| YOTEL Geneva Lake | yes | NOT_ESTABLISHED | COMPLETED | 99 | need dates |
| Nobu London Portman Square | yes | NOT_ESTABLISHED | NOT_STARTED | 0 | research + need dates |
| Mayflower Hotel | yes | NOT_ESTABLISHED | NOT_STARTED | 0 | research + need dates |
| Falls Church Marriott Fairview Park | **no** | — | — | — | NOT_IN_CANONICAL_GDI_CONFIG_ON_THIS_BRANCH |
| Grand Fiesta Americana Chapultepec | **no** | — | — | — | NOT_IN_CANONICAL_GDI_CONFIG_ON_THIS_BRANCH |
| Fiesta Americana Condesa Cancún | **no** | — | — | — | NOT_IN_CANONICAL_GDI_CONFIG_ON_THIS_BRANCH |

No paid research was run. Opportunity counts are local filesystem inventories only.

## D. Three-hotel detailed verification

### Spice Island (`recKRJjcPnb4tVDDS`)

- Need-date: **NOT_ESTABLISHED** (Airtable 0; no estimates invented)
- Local auth report UI: gray calendar-minus + legend (visibility remediation)
- Published seven: untouched (no republish)
- Research inventory continuation: **blocked** without paid research authorization

### Bethesda (`recLuxvwwxID7U2B8`)

- Need-date: **HOTEL_CONFIRMED** (8 Airtable periods)
- Local opps file: 54 (prior Full E2E figures preserved as research history; not discarded)
- Demo local stub quarantined; Airtable is authority

### Waterstone (`recgMYovrrZDJMqzX`)

- Need-date: **MARKET_ESTIMATED** (local hypothesis only; Airtable test scenarios excluded from confirmed)
- Share surfaces demote unpublished estimates → Not Established
- Prior Full E2E qualified set preserved in local inventory policy; no paid re-run

## E. Screenshots

Prior visibility remediation screenshots remain valid under  
`reports/gdi-universal-need-date-intelligence-v1/screenshots/visibility-remediation/`.

## F. Tests

| Gate | Result |
| --- | --- |
| `test:gdi-hotel-layer-model-and-inventory-policy-v2` | run in commit validation |
| `test:gdi-universal-need-date-intelligence-v1` | existing |
| `test:gdi-universal-need-date-visibility-remediation-v1` | existing |
| `test:gdi-full-e2e-contract` | existing |
| Paid research / live discovery re-runs | **not executed** (restriction) |

## G. Files changed (this wave)

- `lib/.../gdi-hotel-layer-model-v2.js`
- `lib/.../gdi-full-e2e-research-inventory-policy-v1.js`
- `lib/.../gdi-universal-need-date-intelligence-v1.js` (Airtable mirror precedence)
- recovered `need-date-airtable-*-v3.js` modules (+ schema/waterstone pilots missing from branch)
- `scripts/gdi-recover-need-date-airtable-mirror-v2.mjs`
- `scripts/test-gdi-hotel-layer-model-and-inventory-policy-v2.mjs`
- Airtable mirrors under `data/.../need-periods.airtable-mirror.json`
- Reports under `reports/gdi-universal-need-dates-and-full-e2e-v2/`

## H. Commit / PR

Updated on PR #152 (focused follow-up commits). PR #151 untouched.

## I. Production impact

- **No** Railway deploy, publish, Airtable writes, paid research, or share-token changes.
- Production customer reports remain unchanged until founder-authorized deploy of presentation/API.
- Immutable published Spice seven untouched.

## Blockers requiring founder authorization

1. **Paid research** to continue Spice/Bethesda/Waterstone contact enrichment and unresolved Full E2E gaps.
2. **Canonical config onboarding** for Falls Church Marriott Fairview Park + both Fiesta Americana properties before Need-Date/GDI layers can attach.
3. **Railway deploy** of PR #152 for production/published visibility of Need-Date strip.
4. Optional: confirm whether broader Hotel Census (beyond GDI-eligible configs) should ever become Need-Date inventory — currently **declined** to avoid duplicate/unrelated records.
