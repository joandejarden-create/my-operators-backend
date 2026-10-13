# Hotel-supplied evidence QA

- Ingestion path: Pursuit `recordPursuitResponse` → `appendHotelSuppliedEvidence` (skipped for `isSyntheticTest`) → `classifyControllerOutreachResponse` → `requalifyAfterHotelSuppliedResponse` / IDV2 feedback loop
- Production persistence requires response authority check
- Synthetic fixtures: **never** persisted as production evidence
- Ready auto-promote: **NO**
- Emails sent by this pilot: **NO**
