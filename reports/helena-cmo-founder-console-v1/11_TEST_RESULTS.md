# 11 — Test Results

Command: `npm run test:helena-cmo-founder-console-v1`

| ID | Test | Result |
|---|---|---|
| FC-01 | Brief loads Manual Week 1 | PASS |
| FC-02 | Top 3 + ≤3 decisions | PASS |
| FC-03 | UNKNOWN / DATA_GAP preserved; no fabricated accounts | PASS |
| FC-04 | Tier 1 present; EXECUTE/recurring/Cursor OFF | PASS |
| FC-05 | APPROVE → APPROVED_PREPARE_ONLY | PASS |
| FC-06 | Decision local persist without EXECUTE/OS sync | PASS |
| FC-07 | Attention counts action items | PASS |
| FC-08 | UI + API files exist | PASS |

**Overall:** 8/8 PASS  
Machine copy: `test-results.json`

## Manual QA checklist (browser)

1. Admin → Helena CMO visible in left nav  
2. Founder Brief loads Week 1 paragraph + Top 3  
3. ≤3 decisions obvious with Review & act  
4. UNKNOWN/DATA_GAP visible on scoreboard  
5. Evidence tab optional; no Cursor required  
6. Approve PREPARE does not claim publish  
7. Missing week JSON → error state (not blank)
