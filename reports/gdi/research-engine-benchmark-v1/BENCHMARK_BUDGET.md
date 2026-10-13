# Benchmark Budget Caps

Chosen to mirror production-economical GDI native bounds (not open-ended agent loops).

| Cap | Per hotel / lane | Rationale |
|---|---|---|
| max_query_count | 30 | Mallorca native query budget observed |
| max_source_fetch | 40 | slightly above Sheraton 33 returned |
| max_research_iterations | 8 | recursive lane depth bound |
| max_domain_hops | 5 | recursive navigation bound |
| max_pdf_opens | 10 | document-first bound |
| max_time_minutes | 25 | interactive research ceiling |
| max_token_spend_estimate_usd | 0.50 | gpt-4o-mini extract economics |
| max_serp_spend_estimate_usd | 0.30 | 30 × ~$0.01 |
| max_webhound_spend | **$0 this pack** | credit preservation |
| stop_on | prove OR exhaust OR wrong_market OR no_lodging_motion OR budget | PART 31 |

## This pack actual spend
| Category | Cost |
|---|---|
| Webhound live | $0 (SKIPPED) |
| Alternate model live lanes | $0 (not executed) |
| SerpAPI re-run | $0 (reuse forensic) |
| OpenAI re-run | $0 (reuse forensic) |
| **Total benchmark spend** | **$0 new** |
| Historical Webhound (reference only) | $5 Mallorca session (prior) |
