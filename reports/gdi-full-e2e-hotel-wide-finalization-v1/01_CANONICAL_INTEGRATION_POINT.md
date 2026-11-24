# Exact canonical integration point

**Module:** `lib/group-demand-intelligence/workflow/gdi-canonical-full-e2e-entry-v1.js`  
**Function:** `runCanonicalFullE2eResearch`  
**Hook:** after original research chain + optional research-persistence plan, **before** summary write / return.

**Stage module:** `lib/group-demand-intelligence/workflow/gdi-hotel-wide-finalization-stage-v1.js`  
**Entry:** `runHotelWideFinalizationStage`

**Escape hatch (tests only):** `opts.skipHotelWideFinalization === true`  
**Candidate package write:** `opts.writeHotelWideCandidatePackage === true` (pointer still never activated)

FQO contract reused: `gdi-finalized-qualified-opportunity-contract-v1` (no parallel engine).
