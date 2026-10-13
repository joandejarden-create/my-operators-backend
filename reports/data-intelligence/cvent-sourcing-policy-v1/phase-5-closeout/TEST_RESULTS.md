# Source Policy Test Results — Phase 5

**Command:** `node scripts/test-source-policy-cvent-v1.mjs`  
**Result:** PASS (exit 0)

```
PASS 1_cvent_rooms_cannot_persist_canonical
PASS 2_cvent_meeting_cannot_persist_canonical
PASS 3_cvent_description_not_customer_display
PASS 4_adp_cvent_only_attribute_blocked
PASS 5_gdi_cvent_only_hotel_fit_scoring_blocked
PASS 6_cvent_event_evidence_still_allowed
PASS 7_discovery_research_candidate_created
PASS 8_independent_first_party_can_promote
PASS 9_mixed_provenance_follows_verified
PASS 10_unknown_stays_unknown_without_verification
{
  "ok": true,
  "passed": 10,
  "sourcePolicyVersion": "source-policy-v1"
}

```
