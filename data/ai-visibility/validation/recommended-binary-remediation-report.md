# Recommended Binary Classifier Remediation

**Classifier:** ai_visibility_recommended_binary_v1
**Rules:** ai_visibility_recommended_binary_rules_v1_3
**Definition:** ai_signal_recommended_definition_lock_v1
**DEV gate:** FAIL

## DEV Before (v4.1)

- N 273 · TP 49 TN 150 FP 3 FN 71
- P 94.23% · R 40.83% · F1 56.98%

## DEV After (binary v1)

- N 273 · TP 72 TN 137 FP 16 FN 48
- P 81.82% · R 60.00% · F1 69.23% · Spec 89.54%

## Remaining errors

- **shortlist_wording**: 0
- **scope_inheritance**: 0
- **table_list_inheritance**: 9
- **multi_entity_structure**: 4
- **implicit_recommendation**: 0
- **qualified_recommendation**: 0
- **comparator**: 0
- **descriptive_mention**: 0
- **negative_exclusion**: 0
- **prompt_intent_error**: 0
- **other**: 51

## Regression

- Positive 10 / Negative 8 → **PASS**

## Next

**RECOMMENDED_BINARY_CLASSIFIER_REMEDIATION_CONTINUE**

Status: **RECOMMENDED_BINARY_CLASSIFIER_REMEDIATION_REVIEW_REQUIRED**
