# Root Cause

## First pipeline stage where data diverged
**Customer entity resolution → competitive set / ranking / displacement aggregation**

Raw parse extracted hundreds of hotel names (The Charles Hotel ×49+).  
`resolveCustomerFacingEntity` returned `unbound_fail_closed` for all Munich hotels because `PROPERTY_ENTITY_REGISTRY` had **no** `adp_westin_grand_munchen` entry.

## Root cause class
**MULTIPLE:** ENTITY_RESOLUTION_BUG (missing market registry) + DERIVED_ARTIFACT_BUG (empty customer competitive universe) + CERTIFICATION_GAP (CERTIFIED despite empty competitive set while raw competitors existed) + ATTRIBUTE_PIPELINE_BUG (wellness/large_ballroom missing from ATTRIBUTE_DEFINITIONS)

| Flag | |
|------|--|
| COMPETITOR EXTRACTION BUG | **NO** (extraction worked) |
| ENTITY RESOLUTION BUG | **YES** (missing Munich registry → fail-closed) |
| ATTRIBUTE PIPELINE BUG | **YES** (2 profile attrs not in dictionary) |
| PERSISTENCE BUG | **NO** (persisted empty derived correctly from broken bind) |
| API BUG | **NO** (API faithfully served empty competitiveSet) |
| UI BUG | **NO** (UI correctly rendered empty/subject-only) |
| CERTIFICATION GAP | **YES** |
