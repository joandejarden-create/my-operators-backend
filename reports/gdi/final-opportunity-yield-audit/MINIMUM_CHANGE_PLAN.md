# Minimum Change Plan

## P0
1. **Wire campaign → child decomposition for one generator at a time**  
   - Root: B  
   - Files: `demand-campaigns/*` + call into `ten-bases-of-demand-v1/decomposers.js` (or thin wrapper) from a controlled job — not broad `runGroupDemandResearch` rediscovery  
   - Risk: medium (wrong children if SERP noise)  
   - Yield: high — unlocks account-level pipeline

2. **Keep surface FUTURE_WATCH ≠ exhibitor fix**  
   - Root: G/I  
   - Files: `customer-surface-revalidation-v1.js` `isExhibitorStyle`  
   - Risk: low  
   - Yield: restores AidEx/CQ-facing consistency

3. **Stop hardcoding campaign `customerReady` counters**  
   - Root: G  
   - Files: `yotel-ten-generators.js` seed; recompute from live gates  
   - Risk: low  
   - Yield: truth alignment

## P1
4. **Require Complete Demand Packet before customer-ready** on child writes  
   - Root: A/C/D  
   - Files: promote path + packet schema  
   - Risk: medium (fewer ready)  
   - Yield: quality

5. **Jev advisory loop on admitted children only** (already policy-gated)  
   - Root: C/D  
   - Files: `jev-active-advisor.js` hooked after admission  
   - Risk: low  
   - Yield: faster pillar closure

## P2
6. Comp-set / multilingual / feeder as optional deepeners after child admission — not discovery front-doors.

## Explicit non-changes
- Do not lower thresholds
- Do not redesign 10 Bases broadly before campaign→child wire proves yield
