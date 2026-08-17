# V4 Restart — First-100 Enhanced Audit Plan

**Prerequisite:** Coordinated repair authorized + applied + post-write audit PASS.  
**V4 remains PAUSED until then.**

## Procedure

1. Apply authorized coordinated repair (separate authorization).
2. Post-write audit of repaired 400 keys (semantic + affiliation + geography).
3. Resume V4 with **first 100 production mutations** only.
4. For each of the 100, automatically verify post-write:
   - correct property
   - correct field
   - correct semantic type
   - correct source
   - correct geography
   - correct affiliation
   - expected vs actual
5. If **any** semantic quality violation → **HARD CIRCUIT BREAK**.
6. If all 100 pass → continue standing V4 without Joan approval for that checkpoint.

## Circuit break conditions

- Parent-as-brand write
- Descriptor/marketing City
- Country-as-city
- Medium/Low identity brand write
- Cvent/legacy as production evidence
- Unexpected insert / identity mismatch
