# RESEARCH_LEARNING_REGISTRY

> Packet 2.8A · Learning registry documentation  
> Data: `reports/hotel-intelligence/packet-2.8/RESEARCH_LEARNING_REGISTRY.json`  
> Module: `lib/hotel-intelligence/learning/research-learning-registry.js`  
> Policy: **CANDIDATE by default · never auto-mutate production prompts**

## Registry metadata

| Field | Value |
|--------|--------|
| `registry_version` | `research-learning-registry-v1` |
| `auto_promote` | `false` |
| Records | **5** (all `status: CANDIDATE`) |
| Source runs | All 5 Webhound sessions in Packet 2.8 inventory |

## Record schema (conceptual)

`learning_id`, `run_id`, `hotel_id`, `hotel_archetype`, `jurisdiction`, `template_id`, `template_version`, `problem`, `successful_method`, `failed_methods[]`, `source_hierarchy[]`, `query_patterns[]`, `document_patterns[]`, `entity_patterns[]`, `temporal_patterns[]`, `negative_screens[]`, `validation_rules[]`, `stop_condition`, `native_reproduction` (`YES`|`PARTIAL`|`NO`|`UNKNOWN`), `native_reproduction_method`, `recommended_playbook_change`, `new_eval`, `status` (`CANDIDATE`|`VALIDATED`|`PROMOTED_TO_PLAYBOOK`|`REJECTED`), `auto_promote_forbidden`, timestamps.

## Current CANDIDATE records

| learning_id | Hotel | Template | Native | Theme |
|-------------|-------|----------|--------|-------|
| `learn_kgpv_full_hi_adjacent_asset` | KGPV | FULL_HOTEL_INTELLIGENCE | PARTIAL | Adjacent-asset / MX-PUBCO |
| `learn_kgpv_co_conversion_slip` | KGPV | CHANGE_OPPORTUNITY | PARTIAL | Announced conversion slip |
| `learn_cambridge_private_opaque` | Cambridge | FULL_HOTEL_INTELLIGENCE | PARTIAL | Private opaque + tourism order |
| `learn_sheraton_gdl_profeco_propco` | Sheraton GDL | FULL_HOTEL_INTELLIGENCE | PARTIAL | MX private PropCo / PROFECO |
| `learn_voco_reflag_package_owner` | voco/Real Inn | FULL_HOTEL_INTELLIGENCE | PARTIAL | Same-asset reflag + package owner |

## Promotion rule (required before production)

1. Case evidence supports the method  
2. Method is generic enough to transfer  
3. Negative cases understood  
4. Eval created  
5. Regression suite passes (golden four fixtures)

Workflow:

```
RUN → LEARNING CANDIDATE → REVIEW / EVAL → PLAYBOOK UPDATE
  → PROMPT VERSION UPDATE (if needed) → REGRESSION → PROMOTION
```

`canPromoteLearning()` must refuse when `auto_promote_forbidden` or status ≠ validated path.

## Integration with Packet 2.8A

- External runs produce learning records (golden five mined)  
- 25-hotel pilot learnings: **N/A** until research executes  
- Validated learnings feed native playbooks **before** future escalations — not yet PROMOTION status for the five CANDIDATES
