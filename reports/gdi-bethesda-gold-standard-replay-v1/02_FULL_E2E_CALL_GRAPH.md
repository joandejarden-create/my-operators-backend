# 2. Current Full E2E call graph

**Entrypoint:** `lib/group-demand-intelligence/workflow/gdi-canonical-full-e2e-entry-v1.js` → `runCanonicalFullE2eResearch`

```
preflightCanonicalFullE2e
  → executeOriginalFullE2eResearchChain   (original-full-e2e-research-chain-v1)
       CHAIN_STAGES:
         HOTEL_PROFILE
         DISCOVERY
         DEEP_RESEARCH
         CONTACT_RESEARCH          ← nests WHO → ALT → HOW stage events
         COMMERCIAL_QUALIFICATION
         SALES_INTELLIGENCE_OUTPUT ← presentation / classification / recommendation
         PERSISTENCE_AND_CONTINUATION
  → research persistence bridge (dry-run default)
  → runHotelWideFinalizationStage        (mandatory unless isolated-test skip)
       inventory merge RESEARCH ∪ CHAIN ∪ SEED ∪ STAGED ∪ CURRENT
       FQO evaluateFinalizedQualifiedOpportunity per row
       dispositions + HOTEL_COMPLETION_MANIFEST
       writePackage CANDIDATE only (no writePointer)
```

## Founder-requested stages vs reality

| Requested | Actual | Notes |
|-----------|--------|-------|
| DISCOVERY | YES (chain) | Paid requires auth; dry-run reuses/skips |
| DEMAND VALIDATION | PARTIAL | Embedded in deep research + commercial qualification; no standalone stage name |
| HOTEL FIT | PARTIAL | Thesis fields + FQO gate `HOTEL_GEO_COMMERCIAL_FIT` (length-based) |
| WHO BUYER RESEARCH | YES (nested) | Live Exa only when authorized |
| ALT CONTACT RESEARCH | YES (nested) | |
| PERSON VERIFICATION | PARTIAL | Name/plausibility helpers; not a separate stage ID |
| HOW PUBLIC CONTACT | YES (nested) | |
| COMMERCIAL QUALIFICATION | YES | |
| OPPORTUNITY CLASSIFICATION | PARTIAL | Inside sales-intelligence / presentation builders |
| COMMERCIAL RECOMMENDATION | PARTIAL | Field writers + FQO action gate |
| HOTEL-WIDE RECONCILIATION | YES | Terminal stage |
| FQO CERTIFICATION | YES | Inside hotel-wide |
| PRESENTATION PROJECTION | YES | `projectOpportunityForPresentation` at candidate write |
| VERSIONED STAGED CANDIDATE | YES | Pointer **not** activated |

Dry-run does **not** equal live production WHO/HOW provider execution.
