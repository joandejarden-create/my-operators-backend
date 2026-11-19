# Authorization gate — gdi_20opp_live_research_prep_v1

## Current status
- Prep harness mode: `mock`
- Live public auth env: `GDI_20OPP_LIVE_PUBLIC_FOUNDER_AUTH`
- Paid budget auth env: `GDI_20OPP_LIVE_PAID_BUDGET_FOUNDER_AUTH`
- Paid execution: **blocked** in this prep (even if paid env present)
- Actual paid spend this run: **$0**

## Exact authorized commands
```bat
# Step A — $0 live official_page only (still denies Exa/SerpAPI/PDL/Surfe):
set GDI_20OPP_LIVE_PUBLIC_FOUNDER_AUTH=1
npm run gdi:20opp-live-research-prep-v1 -- --mode live_public

# Step B — PAID ≤$20 total / ≤$4 per case — REQUIRES SEPARATE FOUNDER AUTH (not enabled by this prep PR):
# set GDI_20OPP_LIVE_PAID_BUDGET_FOUNDER_AUTH=<founder-authorization-id>
# npm run gdi:20opp-live-research-prep-v1 -- --mode paid --authorize-paid-research
# (Currently hard-refuses paid execution even if env is set.)
```

## Allowlists
- Always: mock, offline_recovery, deterministic_ors
- Live public $0: official_page, direct_public
- Paid (separate auth, currently refused): exa, firecrawl, serpapi, surfe, pdl, parallel, jev
