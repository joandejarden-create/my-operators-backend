# Root Cause

## First stage where Sheraton records disappeared

They never entered the customer publication path.

1. E2E `dispositionFromFit` labeled seed packages as `VALID_FUTURE_WATCH` without `isValidFutureWatch`
2. Promote persisted `customerVisible:false`
3. Watch gate: `RESEARCH_BACKLOG_NOT_WATCH`
4. Surface: `DOWNGRADE_TO_DEMAND_GENERATOR`
5. Facing filter → 0 → API 0 → UI empty

## ROOT CAUSE CLASS

**QUALIFICATION_REPORT_MISMATCH**
