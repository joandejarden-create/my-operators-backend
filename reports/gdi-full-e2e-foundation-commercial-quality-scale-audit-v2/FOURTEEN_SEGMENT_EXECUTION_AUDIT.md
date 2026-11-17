# Fourteen-Segment Execution Audit

## Registry evidence (CONFIRMED)

Source: `lib/group-demand-intelligence/workflow/full-e2e-fourteen-segment-discovery-executor-registry-v1.js`

All 14 canonical segment IDs map to shared runner `segment-discovery-executor-v1.js` with classification **`PARTIAL_EXECUTOR`**. Specialized runners exist for associations and scaling.

## Historical execution ledgers (CONFIRMED)

Under `data/group-demand-intelligence/execution-observability/`:

| Hotel | Run | Verdict | Segments completed | Providers |
|-------|-----|---------|--------------------|-----------|
| YOTEL `recrPQcZg7SFARRb2` | `hist_yotel_geneva_exa_30usd_scale_v1` | `FULL_E2E_PARTIAL` | 0 (all UNKNOWN) | `exa_agent` |
| Bethesda `recLuxvwwxID7U2B8` | `hist_bethesda_full_e2e_template_v1` | `FULL_E2E_PARTIAL` | 0 (all UNKNOWN) | none listed |
| Radisson `recH9h3McgMNFQEla` | `hist_radisson_toronto_exa_campaigns_v1` | (historical) | UNKNOWN pattern | see ledger |

Per-segment rows carry `errorOrSkipReason: historical_segment_execution_not_fully_ledgered`. UNKNOWN must remain UNKNOWN — **do not relabel as EXECUTED**.

## Customer-visible segment mix (YOTEL package — CONFIRMED)

Published package fingerprint `35343009…`, 42 opportunities by `opportunityType`:

| Segment / type | Count |
|----------------|------:|
| education_scientific | 6 |
| scaling_companies | 6 |
| projects_infrastructure | 4 |
| corporate_meetings_incentives | 4 |
| sports_entertainment | 4 |
| associations_conferences_events | 4 |
| local_catering_day_meetings | 3 |
| professional_event_planners | 3 |
| government_public_sector | 3 |
| travel_trade_group_intermediaries | 2 |
| association_events (alias) | 1 |
| preferred_lodging_partnerships | 1 |
| social_special_events | 1 |
| corporate_celebrations_private_events | 0 in this package |
| relocation_extended_stay | 0 in this package |

**Interpretation:** Published inventory spans many segments, but that proves **qualification/publication coverage**, not that Full E2E live discovery executed each segment with multi-provider depth in a ledgered run.

## Matrix (summary)

| Segment | Implementation | Configured strategy | Live ledger evidence | Weakness |
|---------|----------------|---------------------|----------------------|----------|
| associations_conferences_events | PARTIAL + specialized runner | assoc + exa | UNKNOWN hist / present in YOTEL inventory | Event-heavy; strong conventional coverage |
| corporate_accounts_scaling | PARTIAL + scaling runner | scaling strategies | UNKNOWN / present as scaling_companies | Forward-looking signals uneven |
| corporate_meetings_incentives | PARTIAL | destination corporate | UNKNOWN / present | |
| government_public_sector | PARTIAL | government queries | UNKNOWN / present | Procurement depth UNVERIFIED |
| sports_entertainment | PARTIAL | base sports | UNKNOWN / present | Apify preferred but mock default |
| education_scientific | PARTIAL | medical/pharma | UNKNOWN / present | |
| projects_infrastructure | PARTIAL | project workforce | UNKNOWN / present | Key forward-looking bet |
| social_special_events | PARTIAL | venue discovery | UNKNOWN / thin YOTEL | Apify family |
| local_catering_day_meetings | PARTIAL | local meetings | UNKNOWN / present | |
| corporate_celebrations_private_events | PARTIAL | — | UNKNOWN / missing YOTEL | Likely under-researched |
| travel_trade_group_intermediaries | PARTIAL | — | UNKNOWN / thin | |
| relocation_extended_stay | PARTIAL | — | UNKNOWN / missing YOTEL | Forward-looking gap |
| preferred_lodging_partnerships | PARTIAL | — | UNKNOWN / thin | |
| professional_event_planners | PARTIAL | — | UNKNOWN / present | |

## Root causes of weak coverage (evidence-ranked)

1. **Orchestration/instrumentation:** Historical campaigns not fully ledgered per segment (CONFIRMED).  
2. **Provider defaults:** Paid Exa dominates authorized campaigns; Apify mock/fail-closed (CONFIRMED).  
3. **Query/strategy concentration on published events** (LIKELY from inventory mix + Exa events strategies).  
4. **Budget** (LIKELY for paid multi-provider breadth).  
5. Over-restrictive qualification (NOT primary for YOTEL — inventory exists but Qualification Needed).

## Required result

The platform **plans** all 14 segments and **can publish** multi-segment inventories, but cannot currently certify that a Full E2E run **executed** all applicable segments with providers without relying on UNKNOWN historical gaps. Next work: live ledger emission on the canonical path, not a new taxonomy.
