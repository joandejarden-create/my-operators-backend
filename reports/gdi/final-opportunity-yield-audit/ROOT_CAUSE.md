# Root Cause Classification

## Ranked
1. **B. CHILD DECOMPOSITION NOT WIRED** — Campaigns visible; decomposers never invoked from live path.
2. **G. SNAPSHOT/API/UI DIVERGENCE** — AidEx: FS/campaign counters + raw Airtable ready vs CQ projection DQ via FUTURE_WATCH→exhibitor false positive (fixed this audit).
3. **A. DISCOVERY QUALITY** — Ten-bases report children are mostly noise and orphaned from the YOTEL 10.

## Also present
- **C. BUYER RESOLUTION WEAK** on YOTEL path (role paths only in canary)
- **D. LODGING EVIDENCE WEAK** (canary lodging mostly UNKNOWN)
- **F. CANONICAL PERSISTENCE GAP** historically for 9/10 generators (repaired as campaigns; children still missing until canary)
- **H/I** surface/readiness interactions amplified AidEx API 0-ready

## Not primary
Thresholds were not the blocker. Child decomposition simply did not run.
