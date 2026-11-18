# Failure recovery tests

| Step | Result |
|------|--------|
| Forced write failure | FAILED=1, ok=false |
| Retry CREATE | {"candidatesBeforeDedupe":1,"candidates":1,"DUPLICATES_RESOLVED":0,"WOULD_CREATE":0,"WOULD_UPDATE":0,"UNCHANGED":0,"CREATED":1,"UPDATED":0,"FAILED":0,"SKIPPED_FORBIDDEN":0,"SKIPPED_BUDGET":0} |
| Second retry idempotent | action=UNCHANGED |

Acceptance: recoverable failure + retry create + no duplicate on second retry = **true**
