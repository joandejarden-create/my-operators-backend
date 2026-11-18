# Fourteen-segment live execution plan

Hotel: Bethesda Marriott (`recLuxvwwxID7U2B8`)

## Entry

```bash
npm run gdi:canonical-full-e2e -- --hotel recLuxvwwxID7U2B8 --mode live --authorize-paid-discovery --authorize-paid-research --authorize-exa-agent-live --budget-ceiling-usd 18
```

gdi-workflow.ps1 FullRun defaults to canonical dry-run and refuses live paid apply; use npm runner with explicit flags.

## Segments (14)

1. `associations_conferences_events`
2. `corporate_accounts_scaling`
3. `corporate_meetings_incentives`
4. `government_public_sector`
5. `sports_entertainment`
6. `education_scientific`
7. `projects_infrastructure`
8. `social_special_events`
9. `local_catering_day_meetings`
10. `corporate_celebrations_private_events`
11. `travel_trade_group_intermediaries`
12. `relocation_extended_stay`
13. `preferred_lodging_partnerships`
14. `professional_event_planners`

## Discovery

- One initial retrieval per segment
- Distinct traceable Exa `agent_run_*` IDs
- Per-segment + global stage budgets enforced

## Triage (before deep)

- Reject geography-incompatible demand for Bethesda-first pursuit
- Reject unsupported / speculative noise
- Exclude duplicate activities / cross-segment org duplicates from paid deep
- Preserve WATCHLIST / research-continuation intelligence

## Selective deep

- Only triage survivors
- Org-level paid deep dedupe ON
- No indiscriminate deep on all raw discovery rows
- Investigate named contacts when commercially relevant

## Persistence (after research)

1. Dry-run research inventory bridge → exact write manifest
2. Founder approves Authorization B + exact ops
3. Apply bounded CREATE/UPDATE with read-back
4. GDO remains separately gated
