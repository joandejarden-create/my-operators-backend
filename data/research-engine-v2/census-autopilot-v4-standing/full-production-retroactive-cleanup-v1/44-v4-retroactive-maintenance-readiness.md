# V4 Retroactive Maintenance Readiness

**V4: PAUSED — do not resume in this task**

## Confirmed design requirements

1. Persistent queues cover **NEW** property work and **EXISTING** production remediation.
2. Queues: ADDRESS · CITY · STATE · MARKET_REGISTRY · SUBMARKET · COORDINATE · PHONE · ROOMS · AFFILIATION · RIGHTS_BLOCKED · STEWARD.
3. Autopilot revisits until verified / ladders exhausted / rights blocked / N/A / steward.
4. New adapters may reopen unresolved fields; exhausted fields stop until new evidence.

## Status after SAFE cleanup

- Full-table SAFE apply: EXECUTED
- Remediation queues: GENERATED (`41-retroactive-remediation-queues.json`)
- Systemic Country-as-Market: 0
- `[object Object]` Address: 0

## Resume gate

V4 may request **final restart authorization** only after founder review of post-cleanup audit + queues.
Current verdict: **NEEDS MORE WORK** (paused; continuous maintenance design ready, restart not authorized).
