# Recommended Signal Definition Study

**Version:** ai_signal_recommended_definition_lock_v1
**Status:** RECOMMENDED_SIGNAL_DEFINITION_LOCK_PASS
**LOCK_READY:** YES

## Locked question

> Does the response affirmatively place this specific canonical entity into the decision set for the user's stated hotel decision?

## Associated option audit

- TOTAL: 47
- AFFIRMATIVE_CONSIDERATION_OPTION: 43
- CONTEXTUAL_ASSOCIATED_ENTITY: 4
- AMBIGUOUS: 0

## Discussed FN audit (pred=discussed, old Recommended TRUE)

- TOTAL: 25
- RECOMMENDED_TRUE: 17
- RECOMMENDED_FALSE: 1
- AMBIGUOUS: 7

## DEV semantic study

- N: 290
- PROPOSED_TRUE: 120
- PROPOSED_FALSE: 158
- AMBIGUOUS: 12
- Old vs proposed disagree (unambiguous): 43 (oldF→propT 43; oldT→propF 0)

## Current v4.1 under locked definition (unambiguous only)

- N: 278 (excluded ambiguous 12)
- TP 48 / TN 158 / FP 0 / FN 72
- Precision 100.00% · Recall 40.00% · F1 57.14%
- **Not certified** — study only

## Error taxonomy under locked definition

- **shortlist_wording**: 25
- **implicit_recommendation**: 4
- **recommendation_in_table_list**: 16
- **conditional_recommendation**: 0
- **qualified_recommendation**: 2
- **comparator_mistaken_for_recommendation**: 0
- **descriptive_mention_mistaken_for_recommendation**: 0
- **negative_recommendation**: 0
- **parent_sibling_confusion**: 0
- **recommendation_scope_mismatch**: 17
- **multi_entity_sentence_ambiguity**: 8
- **other**: 0

## Next step

**READY_FOR_RECOMMENDED_BINARY_CLASSIFIER_REMEDIATION**
