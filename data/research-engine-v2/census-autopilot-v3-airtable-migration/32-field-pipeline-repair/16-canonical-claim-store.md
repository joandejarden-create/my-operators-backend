# Canonical claim store

Module: `lib/research-engine-v2/census-autopilot-v3/claim-store.js`
Version: `census-autopilot-v3.0.1-claim-store`

Shape: property_identity_id → field → claim[]

Each claim: value, source, source_type, source_url, retrieved_at, confidence, match_confidence, rights_status, research_run, temporal_validity, status

API: upsertClaim, mergeClaimStores, resolveBestEligibleClaim
