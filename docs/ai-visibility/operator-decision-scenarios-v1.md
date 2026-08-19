# Operator Decision Scenarios V1

Registry: `OPERATOR_DECISION_SCENARIO_REGISTRY_V1`  
Code: `lib/ai-visibility/operator-intelligence/scenarios.js`

Scenarios are **owner-decision** prompts, not a list of the current nine operators.

| ID | Name | Owner decision | Regional scope |
|---|---|---|---|
| `op_scenario_full_service_uu_operator_selection_v1` | Full-service upper-upscale operator selection | Who should operate an upper-upscale full-service hotel? | Global + CALA variant |
| `op_scenario_luxury_operator_selection_v1` | Luxury hotel operator selection | Who is commonly considered to operate a luxury hotel? | Global + CALA variant |
| `op_scenario_lifestyle_boutique_operator_selection_v1` | Lifestyle / boutique operator selection | Which operators are considered for independent lifestyle or boutique hotels? | Global + CALA variant |
| `op_scenario_owner_control_flexibility_v1` | Owner control / flexibility | Which management companies suit owners wanting greater control? | Global |
| `op_scenario_third_party_management_v1` | Third-party management | Which third-party hotel operators are commonly considered? | Global + CALA variant |
| `op_scenario_brand_agnostic_operation_v1` | Brand-agnostic operation | Which operators can manage hotels across multiple brand systems? | Global |
| `op_scenario_independent_hotel_operation_v1` | Independent hotel operation | Which management companies are considered for independent hotels? | Global + CALA variant |
| `op_scenario_conversion_repositioning_v1` | Conversion / repositioning | Which operators are considered when converting or repositioning a hotel? | Global |
| `op_scenario_commercial_revenue_capability_v1` | Commercial / revenue capability | Which operators are associated with strong commercial and distribution capability? | Global |
| `op_scenario_resort_operation_v1` | Resort operation | Which management companies are considered for resort hotels? | CALA / LATAM emphasis |
| `op_scenario_cala_latam_regional_capability_v1` | CALA / LATAM regional capability | Which operators are considered for hotels in CALA / Latin America? | CALA / LATAM |
| `op_scenario_institutional_platform_alignment_v1` | Institutional platform | Which operators are considered by institutional owners seeking scalable platforms? | Global |

## Remington Hospitality (CALA) eligibility notes

Operator Master `rec6UB6RpMKSs2tAo`. Monitored scope = CALA (not a fake standalone entity).

| Scenario family | Status | Rationale |
|---|---|---|
| Full-service upper-upscale | CONDITIONALLY_ELIGIBLE | Third-party manager with branded full-service evidence; label enterprise U.S. scale separately |
| Luxury | CONDITIONALLY_ELIGIBLE | CALA portfolio includes luxury/resort assets; not a pure luxury-only operator |
| Lifestyle / boutique | ELIGIBLE | Manages independent/boutique assets per governed profile |
| Owner control / flexibility | ELIGIBLE | Third-party management model |
| Third-party management | ELIGIBLE | Confirmed operating model |
| Brand-agnostic | ELIGIBLE | Multi-brand management platform |
| Independent hotel | ELIGIBLE | Independent/boutique management cited in governed profile |
| Conversion / repositioning | CONDITIONALLY_ELIGIBLE | Enterprise repositioning capability; CALA conversion evidence partial |
| Commercial / revenue | ELIGIBLE | Institutional third-party platform |
| Resort | ELIGIBLE | CALA resort portfolio (governed assignments) |
| CALA / LATAM regional | ELIGIBLE | Primary reason for monitored inclusion |
| Institutional platform | ELIGIBLE | U.S.-systems operator with CALA regional leadership |

Truth coverage = PARTIAL. Do not infer unsupported capability from AI responses. Census reads = 0.

Prompt origin V1 = `SCENARIO` only. Do not seed operator names except in later comparative research prompts (none in V1 core/extended).
