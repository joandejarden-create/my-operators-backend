# Discovery Admission Rules — isGdiResearchLeadWorthPursuing()

## Must have
1. VALID ENTITY
2. TARGET MARKET RELEVANCE
3. PLAUSIBLE FUTURE GROUP MOTION
4. **AT LEAST TWO** of:
   - LODGING_HINT
   - BUYER_ORGANIZER_HINT
   - REPEAT_ROTATION_SIGNAL
   - PROCUREMENT_RFP_SIGNAL
   - HOUSING_ACCOMMODATION_SIGNAL
   - KNOWN_GROUP_TRAVEL_PATTERN
   - COMPETITOR_HOTEL_USE
   - NAMED_DELEGATION_CREW_TEAM
   - FUTURE_DECISION_WINDOW
   - EVENT_SERIES_OVERNIGHT_DEMAND

## Reject
- company expansion with no travel/group motion
- generic conference listing
- historic event with no future path
- generic university activity / organization HQ
- construction without workforce lodging angle
- festival with no identifiable lodging buyer
- corporate news with no meeting/travel motion

## Module
`lib/group-demand-intelligence/opportunity-discovery-v5/research-lead-gate.js`
