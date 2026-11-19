# HE-INTEL-001 P3B-M1 — Founder readiness report

**PR:** [#428](https://github.com/joandejarden-create/my-operators-backend/pull/428) — **OPEN**  
**HEAD:** `4f07617ab28c701946c27c353b826442d72cbe3a`  
**Status:** OPEN — **STOP before merge / live paid execution**  
**Paid budget:** proposed **$5.00** / **$2.50** per hotel — **NOT AUTHORIZED**

## Pilot
| Hotel | HPC ID |
| --- | --- |
| Bethesda Marriott | `recLuxvwwxID7U2B8` |
| Wyndham San Diego Bayside | `recnPB3Q02NO8Ma1o` |
| The Ritz-Carlton, Mexico City | `recvVZ5unh4d3Krzi` |

Six-hotel cohort retained for later evaluation.

## Readiness validated (mocks / offline)
| Item | Result |
| --- | --- |
| Provider adapters → P1 gateway | PASS |
| Atomic shared-budget reserve | PASS |
| Concurrent overspend blocked | PASS |
| Failed request release | PASS |
| Durable op-id resume (no double spend) | PASS |
| Cross-arm leakage forbidden | PASS |
| Seed facts excluded from scored new intel | PASS |
| Inferred contacts rejected | PASS |
| Production graph write forbidden | PASS |
| Paid without founder auth | REFUSED |
| Surfe / PDL | FORBIDDEN |

## Planned paid ops (not authorized)
See `buildP3BM1PlannedOperations()` — expected max provider USD ≈ **$0.204** across 3 hotels (Exa lite+contents, Parallel search, Parallel task core), headroom under $5. Native USD **UNKNOWN** (credit-capped). Retries default **0**.

## Proposed execution command
```bash
npm run run:he-intel-001-p3b-m1-live -- --founder-auth FOUNDER_AUTH_HE_INTEL_001_P3B_M1_PAID_V1 --confirm-paid-execution --authorized-budget-usd 5
```
Readiness stub still refuses live network spend even if auth shape is present.

## Founder authorization gate
Requires `FOUNDER_AUTH_HE_INTEL_001_P3B_M1_PAID_V1` + `--confirm-paid-execution` + budget ≤ $5 **in a subsequent execution authorization** — not this PR.

## Local gates
- `test:he-intel-001-p3b-m1-readiness-v1` — 10/10
- P1 / P2 / P3A regressions — PASS
- `dry-run:he-intel-001-p3b-m1-readiness` — ok

**AWAITING_INTEGRATION_CONTROLLER**
