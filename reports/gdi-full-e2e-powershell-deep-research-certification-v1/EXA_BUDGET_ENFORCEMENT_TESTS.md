# Exa budget enforcement tests

Generated: 2026-11-18T17:50:09.242Z

## Verdict: **FIXED_AND_VERIFIED**

### Discovery vs deep separation (PR #382 5>4 defect)

```json
{
  "version": "gdi-segment-stage-call-budget-v1",
  "discoverySettled": true,
  "fifthDiscoveryBlocked": true,
  "fifthDiscoveryReason": "SEGMENT_STAGE_CALL_CAP_REACHED",
  "deepAllowedAfterFourDiscovery": true,
  "discoveryCalls": 4,
  "deepCalls": 1,
  "totalOperations": 5,
  "legacySharedCapWouldBreach": true,
  "correctedCapRespected": true
}
```

| Check | Result |
| --- | --- |
| Corrected caps respected | true |
| Legacy shared-cap would breach | true |
| Zero-budget blocks | true |
| USD cap blocks | true |
| Chain accepts `segmentStageCallBudget` | true |

Module: `lib/group-demand-intelligence/research/gdi-segment-stage-call-budget-v1.js`
