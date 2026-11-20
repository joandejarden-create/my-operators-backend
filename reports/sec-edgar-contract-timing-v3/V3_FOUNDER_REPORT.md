# Hotel Contract Timing Intelligence V3 — Scale Pilot

**Mode:** APPLY · **CUSTOMER_BLOCKED** · Do not merge without IC

## Prior waves excluded
- Baseline JPMCC **57** + V2 incremental **19** (accessions + finding keys)
- V3 new findings reported separately from both

## Calibration note
First 200 docs (8-K/10-K EX-10-heavy ranking) yielded **0** incremental dates and stopped early. Discovery was re-ranked **424B / Franchise Expiration column first**; the 500-doc scale wave below is that corrected run (prior noise accessions remain checkpointed and excluded).

## This wave (new only vs JPMCC 57 + V2 19)
| Metric | Value |
|---|---:|
| Documents OK | 500 |
| Independent deals | 50 |
| New unique dates | **405** |
| New hotels | 357 |
| Franchise / Management / License | 405 / 0 / 0 |
| Future 2027–2035 | **5** |
| Dates/100 docs | 81 |
| Excl. top deal /100 | **68.6** |
| Horizons 6/12/24/36m | 0/0/0/0 |

## Form mix (corrected wave)
424B2 178 · 424H 180 · 424B5 31 · 424B3 9 · 424B4 2 · 10-K 100

## Current-status classes
- EXPIRED_STATUS_UNCONFIRMED: 400 (historical dates; silence ≠ continuity)
- SUPERSEDED: 1
- HISTORICALLY_VERIFIED: 4
- CURRENTLY_CORROBORATED: 0

## Agreement-type gap
Management / lease / license structured expirations remain **near-zero** on this CMBS schedule path. Next sequential PR should target EX-10 term extractors + REIT schedules with separate yield reporting — do not infer HMA expirations from manager names in collateral tables.

## Budget
Spent **$0.024** of $25 (Exa metered; SEC-API.io subscription calls=98). No plan upgrades.

## Persistence
Created 14 · Read-back 80 · Dupes prevented 132 · CUSTOMER_BLOCKED · no HPC auto-create

## Scalability
**PROMISING_REPEATABLE** — 50 independent deals; excl.-top-deal yield 68.6/100 remains high (not a single megadoc). Concentration exists (top deal 62 dates) but does not explain the wave alone.

## Governance recommendation
PR #495 foundation (V1–V3 modules + this evidence) is **merge-ready for Integration Controller**. Further 500–1000 doc waves should ship as **sequential PRs** after merge, not indefinite extension of #495.

## Future examples (2027–2035)
1. **The Westin Book Cadillac** — FRANCHISE_EXPIRATION 2032-12-31 · SUPERSEDED · JP MORGAN CHASE COMMERCIAL MORTGAGE SECURITIES CORP (Filer)
2. **Homewood Suites Mobile East Bay Daphne** — FRANCHISE_EXPIRATION 2032-03-31 · HISTORICALLY_VERIFIED · JP MORGAN CHASE COMMERCIAL MORTGAGE SECURITIES CORP (Filer)
3. **Hampton Inn Mobile East Bay Daphne** — FRANCHISE_EXPIRATION 2032-03-31 · HISTORICALLY_VERIFIED · JP MORGAN CHASE COMMERCIAL MORTGAGE SECURITIES CORP (Filer)
4. **Hilton Garden Inn Mobile East Bay Daphne** — FRANCHISE_EXPIRATION 2032-03-31 · HISTORICALLY_VERIFIED · JP MORGAN CHASE COMMERCIAL MORTGAGE SECURITIES CORP (Filer)
5. **Holiday Inn Express Trinidad** — FRANCHISE_EXPIRATION 2035-01-10 · HISTORICALLY_VERIFIED · JP MORGAN CHASE COMMERCIAL MORTGAGE SECURITIES CORP (Filer)
