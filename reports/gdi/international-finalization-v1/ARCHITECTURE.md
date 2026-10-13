# International Finalization Engine V1 — Architecture

Version: `gdi_international_finalization_v1`

## Principle
Convert actionable Watch / COMPLETE_PLAUSIBLE into truthful dispositions (Ready / Watch / No-fit / Closed / Waiting) **without lowering Ready standards**.

## Flow
```
candidate → eligibility → selection process → target hotel fit → lodging decision
→ pillar audit → evidence ask → outreach draft → packet recompute → disposition
```

## Module
`lib/group-demand-intelligence/international-finalization/`

## Capability
- **FINALIZATION_ENGINE_IMPLEMENTED**: true
- **TARGET_HOTEL_FIT_ENGINE_IMPLEMENTED**: true
- **HOTEL_SELECTION_PROCESS_MODEL_IMPLEMENTED**: true
- **LODGING_DECISION_MODEL_IMPLEMENTED**: true
- **CONTROLLER_EVIDENCE_REQUEST_ENGINE_IMPLEMENTED**: true
- **OUTREACH_EVIDENCE_GENERATOR_IMPLEMENTED**: true
- **HOTEL_SUPPLIED_RESPONSE_REQUALIFICATION_IMPLEMENTED**: true
- **FINALIZATION_NEXT_BLOCKER_ENGINE_IMPLEMENTED**: true
- **READY_THRESHOLD_CHANGED**: false
- **WATCH_THRESHOLD_CHANGED**: false
- **APIFY_USED**: false

## Frozen
International Discovery V2 spines, Demand Controllers, Ready/Watch gates, Pursuit, monitors — unchanged.
