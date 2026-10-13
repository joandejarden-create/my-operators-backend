# FOUNDER REPORT — Mallorca GDI Customer Publication Forensic

## Verdict

The Mallorca E2E **report over-claimed** Valid Watch=2. Real gates say **0**. Empty customer UI is correct. Nothing valid was lost in a broken publisher.

## Sheraton

| Field | Value |
|-------|-------|
| CANONICAL READY | 0 |
| CANONICAL VALID WATCH | 0 |
| WATCH 1 | VI Sheraton Mallorca Golf Tournament 2026 |
| WATCH 1 STATUS | NOT_VALID_WATCH (`RESEARCH_BACKLOG_NOT_WATCH`) |
| WATCH 1 CUSTOMER VISIBLE | NO |
| WATCH 1 PUBLISHED | NO |
| WATCH 1 API | NO |
| WATCH 1 UI | NO |
| WATCH 1 FINAL | **REJECT** |
| WATCH 2 | Golf Planet Holidays 2027 |
| WATCH 2 STATUS | NOT_VALID_WATCH (`RESEARCH_BACKLOG_NOT_WATCH`) |
| WATCH 2 CUSTOMER VISIBLE | NO |
| WATCH 2 PUBLISHED | NO |
| WATCH 2 API | NO |
| WATCH 2 UI | NO |
| WATCH 2 FINAL | **DOWNGRADE_INTERNAL** |
| FINAL READY / WATCH | 0 / 0 |
| API READY / WATCH | 0 / 0 |
| UI READY / WATCH | 0 / 0 |

## Castillo

| Field | Value |
|-------|-------|
| CANONICAL READY / VALID WATCH | 0 / 0 |
| HOLD_WATCH customer-facing? | **NO** |
| API READY / WATCH | 0 / 0 |
| EMPTY STATE CORRECT | **YES** |

## Root cause class

**QUALIFICATION_REPORT_MISMATCH**

## What should be visible now

- Sheraton: empty customer GDI (correct)
- Castillo: empty customer GDI (correct)
- Future E2E cannot finish claiming Valid Watch without `isValidFutureWatch`: enforced
