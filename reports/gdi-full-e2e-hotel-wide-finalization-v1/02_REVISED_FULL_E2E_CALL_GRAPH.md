# Revised Full E2E call graph

```
preflightCanonicalFullE2e
  └─ assessCanonicalLiveReadiness (live only)
beginFourteenSegmentExecutionRun
buildReusePriorIntelligenceLedger
executeOriginalFullE2eResearchChain
  ├─ fourteen-segment discovery
  ├─ per-opp deep / WHO / HOW / eligibility (cohort may be subset)
  └─ chain artifacts
finalizeFourteenSegmentExecutionRun
persistFullE2eResearchInventory (opt-in dry-run)
★ runHotelWideFinalizationStage   ← NEW MANDATORY TERMINAL
  ├─ collectHotelWideCanonicalInventory
  │    RESEARCH_STORE ∪ CHAIN_PROCESSED ∪ SEED ∪ STAGED ∪ CURRENT
  ├─ assignHotelWideDisposition (FQO six-gate) for EVERY id
  ├─ HOTEL_COMPLETION_MANIFEST.json
  └─ optional writePackage CANDIDATE (no writePointer)
write canonical-full-e2e-summary.json
return (ok requires hotel-wide reconciliation success)
```

Cohort / allowlist never limits the hotel-wide audit.
Review ≠ paid spend. Agent OFF without explicit auth.
