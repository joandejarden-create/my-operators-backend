# FOUNDER REPORT — Toronto ADP Canonical Data + Calculation Forensic V1

## Verdict

**YES — Toronto Core observations were actually pulled (252/252 real scenario×provider cells).**  
**YES — Metrics reconcile to raw Core observations through the same canonical ADP calculation path as control hotels.**  
**YES — Loyalty is isolated from Core.**  
**YES — Period-family resolution prefers Core for customer/evidence surfaces.**  
**NO metric repair / no new paid period required.**

## Observation integrity

| Field | Value |
|------|------|
| CORE PERIOD | `adp_period_adp_radisson_blu_toronto_downtown_20261104094215_c8bd8b` |
| CORE SCENARIOS | 63 (expected 63) |
| CORE PROVIDERS | 4 |
| CORE OBSERVATIONS | 252 (expected 252) |
| UNIQUE SCENARIO/PROVIDER PAIRS | 252 (expected 252) |
| DUPLICATES | 0 |
| MISSING PAIRS | 0 |
| REAL | 252 |
| SYNTHETIC | 0 |
| CROSS-HOTEL | 0 |

## Metric reconciliation

| Metric | Stored | Recomputed | Num | Den | PASS/FAIL |
|--------|--------|------------|-----|-----|-----------|
| AI Consideration | 33.2 | 33.2 | 73 | 220 | PASS |
| Scenario Presence | 38.1 | 38.1 | 24 | 63 | PASS |
| #1 Appearance | 44.7 | 44.7 | 21 | 47 | PASS |
| Top-3 Appearance | 85.1 | 85.1 | 40 | 47 | PASS |
| Reality Coverage | 19 | 19 | 4 | 21 | PASS |
| Reality Gap | 66.7 | 66.7 | 11+0.5×6 | 21 | PASS |

Partial attributes do **not** count in Reality Coverage numerator (recognized-only). They contribute **0.5** each to Reality Gap numerator under `RG_WEIGHTED_PARTIAL_V2`.

## Business missing

| Field | Value |
|------|------|
| Business missing (API / UI grain) | 16 |
| Expected | 16 |
| Period | `adp_period_adp_radisson_blu_toronto_downtown_20261104094215_c8bd8b` |
| ALL 16 FROM CORE c8bd8b? | YES |
| Loyalty contamination? | NO |

## Displacement (canonical stored)

| Competitor | Stored | Expected |
|------------|--------|----------|
| Bisha Hotel | 24 | 24 |
| Hotel X Toronto (By Library Hotel Collection) | 20 | 20 |
| 1 Hotel Toronto (King West) | 19 | 19 |
| Hazelton Hotel | 19 | 19 |
| Shangri-La Hotel, Toronto | 16 | 16 |

Top Observed Alternative: **Hotel X Toronto (By Library Hotel Collection)** (basis: OVERALL_UNIQUE_PER_OBSERVATION_PRESENCE; mentions/presence 63).  
Hotel X leads overall unique-per-observation presence even when Bisha leads displacement-absent counts — different metric definitions.

## Citations

| Metric | Stored | Recomputed |
|--------|--------|------------|
| Owned | 9.5% | 9.5% |
| External | 90.5% | 90.5% |
| Misclassified (marriott as owned) | 0 expected | 0 |

Top domains (stored): hotels.com, expedia.com, marriott.com, ca.hotels.com, tripadvisor.com.  
`marriott.com` appears because models cite Marriott competitor / OTA-adjacent pages in Toronto competitive answers — classified EXTERNAL, not Toronto owned.

## Reality Gap inventory

| State | Count |
|------|------|
| Tracked tested | 21 |
| Recognized | 4 |
| Partial | 6 |
| Missed | 11 |
| Source conflicts preserved | 1 (Choice vs DTHA meeting inventory) |

Top recognized: Meeting & Event Space; Upper-Upscale Positioning; Waterfront Location; Radisson Brand.  
Top missed: Ballroom; Near Convention Center; Pet-Friendly; Family-Friendly; EV Charging; Urban Hotel; Choice Hotels Distribution; Suite Inventory; Lifestyle Hotel Positioning; City Views.

## Choice Privileges

| Field | Value |
|------|------|
| Rank | #1 of 6 |
| Peer set | `bps_adp_radisson_blu_toronto_downtown_choice_privileges_adequate5_v1` |
| Family | LOYALTY_BPP |
| Peers | Radisson Blu Toronto Downtown; The Saint James Hotel, an Ascend Collection Hotel; Radisson Hotel Toronto Airport West; Park Inn by Radisson Brampton, ON; Park Inn by Radisson Toronto-Markham; Country Inn & Suites by Radisson, Toronto Mississauga, ON |
| Portfolio AI Presence | 45.5% (51/112) |
| #1 Appearance (portfolio grain) | 0.0% |
| Top-3 Appearance | 92.9% (26/28 scenarios) |

## Period integrity

| Check | Result |
|------|--------|
| CORE CURRENT | c8bd8b |
| LOYALTY CURRENT | 308ba1 |
| Report Core sections use Core? | YES |
| Portfolio uses Loyalty? | YES |
| Core drawers use Core? | YES (API) |
| Latest-file Loyalty fallback removed? | YES (`loadLatestCustomerPeriod` + `resolveEvidencePeriodForProperty`) |

## Canonical process

| Check | Result |
|------|--------|
| Same path as Bethesda? | YES |
| Same path as Waterstone (when profile present)? | YES |
| Toronto-specific calculation shortcuts? | NO |

## Final

CAN WE TRUST THE TORONTO ADP BASELINE AS A CANONICAL CERTIFIED ADP RUN?

**YES — as a founder-reviewed staged official Core baseline (`certified: false` until customer publication).**  
Evidence shows the 252 Core observations are real, hotel-specific, and computed through the canonical ADP pipeline with Loyalty isolated. Do not create a new period.
