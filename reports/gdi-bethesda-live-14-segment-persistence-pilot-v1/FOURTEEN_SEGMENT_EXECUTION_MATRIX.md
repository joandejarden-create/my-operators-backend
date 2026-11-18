# Fourteen-segment execution matrix

Canonical registry: `full-e2e-fourteen-segment-discovery-executor-registry-v1`  
Shared executor: `runSegmentDiscoveryExecutor` @ `segment-discovery-executor-v1.js`  
Taxonomy SoT: `listFourteenWorkflowSegmentIds()` (14 IDs — not invented)

| Segment | Classification | Strategy | Paid provider | Specialized runner | Executor invoked | Fresh retrieval | Extracted | Blocked paid | Public hits | Useful public demand | Inventory/drawer path |
|---------|----------------|----------|---------------|--------------------|------------------|-----------------|----------:|--------------|------------:|----------------------|-----------------------|
| `associations_conferences_events` | PARTIAL_EXECUTOR | assoc_strategies_plus_segment_executor | exa_agent | association-events-hotel-runner | true | 0 | 0 | true | 0 | false | yes |
| `corporate_accounts_scaling` | PARTIAL_EXECUTOR | scaling_strategies_plus_segment_executor | exa_agent | scaling-companies-hotel-runner | true | 0 | 0 | true | 2 | true | yes |
| `corporate_meetings_incentives` | PARTIAL_EXECUTOR | destination_corporate_demand_strategies | exa_agent | — | true | 0 | 0 | true | 0 | false | yes |
| `government_public_sector` | PARTIAL_EXECUTOR | government_manual_plus_gdi_queries | exa_agent | — | true | 0 | 0 | true | 0 | false | yes |
| `sports_entertainment` | PARTIAL_EXECUTOR | base_of_demand_sports_entertainment | exa_agent | — | true | 0 | 0 | true | 2 | true | yes |
| `education_scientific` | PARTIAL_EXECUTOR | medical_assoc_plus_pharma_base | exa_agent | — | true | 0 | 0 | true | 1 | true | yes |
| `projects_infrastructure` | PARTIAL_EXECUTOR | base_of_demand_project_workforce | exa_agent | — | true | 0 | 0 | true | 0 | false | yes |
| `social_special_events` | PARTIAL_EXECUTOR | venue_discovery_queries | exa_agent | — | true | 0 | 0 | true | 0 | false | yes |
| `local_catering_day_meetings` | PARTIAL_EXECUTOR | local_meetings_catering_queries | exa_agent | — | true | 0 | 0 | true | 0 | false | yes |
| `corporate_celebrations_private_events` | PARTIAL_EXECUTOR | destination_recognition_celebration_filter | exa_agent | — | true | 0 | 0 | true | 0 | false | yes |
| `travel_trade_group_intermediaries` | PARTIAL_EXECUTOR | professional_planner_queries | exa_agent | — | true | 0 | 0 | true | 0 | false | yes |
| `relocation_extended_stay` | PARTIAL_EXECUTOR | relocation_manual_templates | exa_agent | — | true | 0 | 0 | true | 0 | false | yes |
| `preferred_lodging_partnerships` | PARTIAL_EXECUTOR | preferred_lodging_prompt_builder | exa_agent | — | true | 0 | 0 | true | 2 | false | yes |
| `professional_event_planners` | PARTIAL_EXECUTOR | professional_event_planner_prompt_builder | exa_agent | — | true | 0 | 0 | true | 0 | false | yes |

## Eligibility / qualification rules (shared)

- Sales-ready requires CONFIRMED_DEMAND or RELIABLE_INFERRED_DEMAND + hotel fit + named commercial entry point.
- Research/WATCHLIST results persist to research inventory without GDO/publication promotion.
- Zero-cost mode without paid auth: `BLOCKED_PAID` / `EXECUTION_BLOCKED` (fail-closed).

## Newer / expanded segments (repo history)

Most recently emphasized in discovery targeting / Exa mission work (`25bd356b9` PLP/PEP/LCM targeting; scaling + preferred lodging + planner prompt builders):

- `preferred_lodging_partnerships`
- `professional_event_planners`
- `relocation_extended_stay`
- `corporate_accounts_scaling`
- `projects_infrastructure`
- `corporate_meetings_incentives`
- Plus sports / education-scientific / government (examined via public sources in this pilot)

## Notes

- Canonical registry IDs: **14/14**
- Paid Exa discovery: **BLOCKED** for all 14 (no founder paid budget this run)
- Public no-cost web search used as supplemental real discovery evidence
- Full E2E tests cover fourteen-segment traversal, discovery executor registry, and research persistence bridge
