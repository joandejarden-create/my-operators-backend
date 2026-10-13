# Founder Report — Hilton NY Times Square Controlled ADP Rerun

## Executive verdict
Sept 27’s extreme low score (3.5%) was **partly an identity alias bug** plus a **thinner scenario universe**. Fresh Oct 5 measurement (65×4, entity_v2) yields **21.5% consideration / 38.5% scenario presence** — low score **not fully reproduced**. Hilton vs Renaissance remain **not formally comparable** on full universe (Ren still has `prop_rts_*`).

## RETURN — ADP

| Field | Value |
|---|---|
| HILTON IDENTITY VERIFIED | YES |
| HILTON ALIAS ISSUE FOUND | YES (fixed entity_v2) |
| HILTON / RENAISSANCE SAME SCENARIO UNIVERSE | NO |
| HILTON SCENARIO COUNT (Sept27 published) | 50 |
| HILTON SCENARIO COUNT (live Oct5) | 65 |
| RENAISSANCE SCENARIO COUNT (Sept02 published) | 65 |
| RENAISSANCE SCENARIO COUNT (live builder) | 80 |
| SCENARIO COUNT DIFFERENCE ROOT CAUSE | Actual scenario-set difference: Ren property-specific `prop_rts_*` (+ Hilton Sept27 omitted capability layer) |
| HILTON EXPECTED PROVIDER RESPONSES (live) | 260 |
| HILTON SUCCESSFUL PROVIDER RESPONSES (live) | 260 |
| HILTON FAILED/TIMED-OUT (live) | 0 |
| HILTON CONSIDERATION BEFORE (Sept27 stored) | 3.5% (7/200) |
| HILTON CONSIDERATION AFTER (Sept27 alias reparse) | 8.5% (17/200) |
| HILTON CONSIDERATION LIVE (Oct5 entity_v2) | **21.5% (56/260)** |
| HILTON SCENARIO PRESENCE BEFORE | 12% (6/50) |
| HILTON SCENARIO PRESENCE AFTER (Sept27 reparse) | 26% (13/50) |
| HILTON SCENARIO PRESENCE LIVE | **38.5% (25/65)** |
| HILTON CHATGPT PRESENCE (live) | 15/65 |
| HILTON GEMINI PRESENCE (live) | 9/65 |
| HILTON PERPLEXITY PRESENCE (live) | 18/65 |
| HILTON CLAUDE PRESENCE (live) | 14/65 |
| RENAISSANCE CONSIDERATION (Sept02) | 7.7% published / ~11.9% stored recompute scope |
| RENAISSANCE SCENARIO PRESENCE (Sept02) | 24.6% |
| HILTON LOW SCORE REPRODUCED | NO (extreme 3.5% not reproduced); moderate presence confirmed |
| LOW SCORE CLASSIFICATION | MIXED → identity bug inflated Sept27 low; live = PARTIALLY_CONFIRMED competitive weakness vs Marquis, not measurement failure |
| MARRIOTT.COM TOP-SOURCE ROOT CAUSE | Competitive-universe citation frequency (live top sources: tripadvisor/hotels; marriott.com still #4) |
| MARRIOTT.COM MISATTRIBUTED AS HILTON SOURCE? | NO (data) / prior label misleading |
| HILTON.COM OWNED SOURCE CLASSIFICATION CORRECT | YES (live Owned Sources 6.2%) |
| SOURCE REPORT LABEL FIX REQUIRED | YES — applied |
| COMPETITOR SCENARIO DENOMINATORS CORRECT | YES |
| PROVIDER-RESPONSE COUNTS MISLABELED AS SCENARIOS | NO on displacement/surprise badges (scenario grain); mentions≠scenarios |
| NEW HILTON PERIOD CREATED | YES — `adp_period_adp_hilton_times_square_20261005111601_190412` |
| NEW RENAISSANCE CONTROL PERIOD CREATED | NO (deferred; universe still unequal without excluding prop_rts) |
| HILTON / RENAISSANCE FORMALLY COMPARABLE | NO |
| ADP METHODOLOGY CHANGED? | NO |
| ADP THRESHOLDS CHANGED? | NO |

## Top root causes
1. **IDENTITY_BUG** (Sept27) — missing “Hilton Times Square” alias → false negatives (fixed entity_v2).
2. **SCENARIO_BIAS / MEASUREMENT_DIFFERENCE** — Ren exclusive `prop_rts_*`; published 50 vs 65.
3. **REAL_POSITIONING_DIFFERENCE** — Marquis displacement; limited formal meeting inventory vs full-service TS peers.

## Live period
- Period ID: `adp_period_adp_hilton_times_square_20261005111601_190412`
- Certified + published with entity_v2 force-reparse
- Sept27 retained historically (not overwritten)

## ChatGPT / Claude zeros (Sept27)
Were **not** provider failures (50/50 ok). Partly identity misses; live shows non-zero all providers.
