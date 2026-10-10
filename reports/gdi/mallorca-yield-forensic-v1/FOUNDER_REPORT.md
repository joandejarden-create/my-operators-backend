# FOUNDER REPORT — Mallorca GDI Yield Forensic V1

## Verdict

Publication was already correct (0/0). Yield failed earlier: **IDV2 spines returned zero accounts/controllers**, seeded campaigns were **mostly destination noise with closed lodging elsewhere**, and the only strong Sheraton lodging signals were **hotel-hosted** or **incomplete external packages**. No new Ready/Watch passed gates — correctly.

## Castillo funnel (authoritative)

| Metric | Count |
|--------|------:|
| RAW SIGNALS (queries) | 30 |
| PAGES FETCHED | 20 |
| CAMPAIGN CANDIDATES | 10 |
| CAMPAIGNS ADMITTED (original) | 10 |
| HIGH_QUALITY (re-audit) | 0 |
| NAMED CHILD ACCOUNTS | 0 |
| SPINE CONTROLLERS | 0 |
| SPINE ACCOUNTS | 0 |
| PROVEN TRAVELING ENTITIES | 0 |
| STRONG-INFERENCE TRAVEL | 0 |
| CONTROLLERS (manual) | 2 |
| DIRECT LODGING (Son Vida) | 0 |
| COMPLETE_STRONG | 0 |
| COMPLETE_PLAUSIBLE | 0 |
| VALID WATCH | 0 |
| READY | 0 |
| FIRST MAJOR COLLAPSE | **spine_accounts** |

Top drop reasons: spine empty → no named accounts → closed lodging lists → venue without lodging → generic INVALID discovery.

## Sheraton funnel

| Metric | Count |
|--------|------:|
| RAW SIGNALS (queries) | 30 |
| PAGES FETCHED | 33 |
| CAMPAIGNS | 10 |
| HIGH_QUALITY (re-audit) | 1 |
| NAMED CHILD ACCOUNTS | 0 |
| SPINE CONTROLLERS / ACCOUNTS | 0 / 0 |
| STRONG-INFERENCE TRAVEL | 2 |
| DIRECT LODGING | 2 |
| COMPLETE_PLAUSIBLE | 1 |
| VALID WATCH | 0 |
| READY | 0 |
| FIRST MAJOR COLLAPSE | **campaign quality + Watch gate** (hotel-hosted / incomplete packet) |

## Recovery

| Path | New customer Valid Watch/Ready |
|------|--------------------------------|
| Account-first | NONE |
| Controller-first | DMC graph only — NONE published |
| Historical | NONE |

## Final customer truth

Castillo Ready/Watch: 0→0 · Sheraton Ready/Watch: 0→0 · Newly published: **NONE**

## Global GDI changes from this audit

1. External-demand invariant (hotel-hosted ≠ opportunity)
2. Campaign admission: reject hotel-hosted without external entity; signal-only for closed weak-fit lists
3. E2E must use `isValidFutureWatch` (already repaired)
4. Spines must not report WIRED_AND_USED when outputs are empty — treat empty as yield defect

## Biggest remaining Mallorca bottleneck

**Named current-cycle external accounts with Son Vida lodging path** — market is DMC/tour-operator mediated; public web rarely names the client before hotel selection.
