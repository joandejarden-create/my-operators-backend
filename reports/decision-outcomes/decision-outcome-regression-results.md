# Decision & Outcome Regression Results

**Command:** `npm run test:decision-outcomes`  
**Script:** `scripts/test-decision-outcomes.mjs`  
**Result:** **All checks PASSED** (re-run confirmed).

## Named checks (conceptual 1–18)

| # | Check name | What it asserts |
|---|------------|-----------------|
| 1 | `1_decision_idempotency` | Same hotel\|module\|type\|subject\|version returns same decision; second create is not new |
| 2–4 | `2_3_4_append_only_validation_action_outcome` | Multiple validation/action/outcome events append; history length preserved |
| 5 | `5_current_state_projection` | Projection shows latest action/outcome and lifecycle `OUTCOME_RECORDED` |
| 6 | `6_gdi_ensure_skips_disqualified` | DISQUALIFIED opportunities skip Decision creation |
| 7 | `7_hotel_isolation_boundary` | Cross-hotel access throws `hotel_boundary` |
| 8 | `8_adp_finding_links_decision` | ADP ensure creates Decision linked to finding id |
| 9 | `9_recommendation_version_preserved` | Version bump creates new Decision; v1 text preserved |
| 10 | `10_evidence_snapshot_preserved` | Evidence snapshot + methodology version round-trip |
| 11–12 | `11_12_outcome_does_not_overwrite_validation` | Outcome does not erase prior commercial validation |
| 13 | `13_action_does_not_imply_outcome` | Action alone → `ACTION_TAKEN`, `latestOutcome` null |
| 14 | `14_adp_metric_movement_no_causation` | Measurement write keeps `causalConfidence: UNKNOWN` |
| 15 | `15_cross_hotel_private_outcomes_never_leak_via_boundary` | Metrics API is hotel-scoped |
| 16 | `16_gdi_ingest_auth_feedback` | Auth feedback ingest creates decision + ≥2 events |
| 17–18 | `17_18_repeated_ensure_no_duplicate` | Repeated ADP ensure does not duplicate Decision |

## Scenario fixtures (also PASS)

| Fixture | Scenario |
|---------|----------|
| `fixture_b_known_lead_lost` | Known lead → contacted → LOST (timeline length 4) |
| `fixture_c_not_relevant` | NOT_RELEVANT + NO_ACTION → VALIDATED or CLOSED |
| `fixture_d_adp_improved` | Agree → website update → presence improved (UNKNOWN causality) |
| `fixture_e_adp_disagree` | DISAGREE + NO_ACTION |
| `fixture_f_adp_too_early` | TOO_EARLY_TO_MEASURE outcome |

## Verdict recommendation

**READY WITH SPECIFIC NON-BLOCKING GAPS**

| Gap | Severity |
|-----|----------|
| ADP customer UI for validation/action/outcome still light vs GDI §§19–20 | Non-blocking |
| Airtable persistence deferred (filesystem / GDI-pilot pattern for now) | Non-blocking |
| Predictive analytics deferred | Non-blocking |

Founder admin surface: `/decision-outcomes-audit.html`.
