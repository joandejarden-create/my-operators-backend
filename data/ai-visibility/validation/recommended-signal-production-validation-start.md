# Recommended Signal — Production Validation Workstream Start

**Status:** NOT_READY
**Classifier:** ai_visibility_recommendation_classifier_v4_1
**Evidence:** ai_visibility_recommendation_evidence_v4_1
**Resolver (unchanged):** ai_visibility_entity_resolver_v2_1_contextual

## Baseline (Clean DEV, n=290)

| Metric | Value |
|---|---|
| TP | 47 |
| TN | 211 |
| FP | 1 |
| FN | 31 |
| Precision | 97.92% |
| Recall | 60.26% |
| F1 | 74.60% |

Gate remains Precision ≥ 98% and Recall ≥ 98%.

## Error taxonomy (FP+FN=32)

- **shortlist_wording**: 6
- **implicit_recommendation**: 0
- **recommendation_in_table_list**: 3
- **conditional_recommendation**: 0
- **qualified_recommendation**: 2
- **comparator_mistaken_for_recommendation**: 0
- **descriptive_mention_mistaken_for_recommendation**: 0
- **negative_recommendation**: 0
- **parent_sibling_confusion**: 0
- **recommendation_scope_mismatch**: 13
- **multi_entity_sentence_ambiguity**: 8
- **other**: 0

## Definition gap

Product contract for AI_SIGNAL_RECOMMENDED treats shortlist / affirmative option membership as TRUE. Current v4.1 production mapping excludes associated_option / consideration-set cues. Resolve definition before recall remediation.

## Next step

**RECOMMENDED_SIGNAL_DEFINITION_REVIEW_REQUIRED**

- Do not enable Recommendation Share
- Do not start First Recommendation work
- Do not generate Recommended holdout this phase
- Presence remains PRODUCTION_VALIDATED / unchanged
