# FOUNDER REPORT — Publication Trigger Monitor V1

## Verdict

Reusable **publication-trigger monitoring** is live for the five frozen AC/Radisson campaigns. Monitors watch **known official sources only**, hash content, classify semantic change, and invoke the **shared second-generation decomposition** only when meaningful new evidence appears.

Baseline checks captured content hashes for BioCultura, RIF, CIELO, and AUTOAMERICAS without Ready inflation. IAPS official host currently returns HTTP 403 to automated fetch — monitor stays ACTIVE as `SOURCE_UNREACHABLE` and retries on schedule (no broad SERP).

## Active monitors

| Campaign | Status | Priority | Next check | Window |
|----------|--------|----------|------------|--------|
| IAPS | ACTIVE | 3 | 2026-10-28 | 2026-12-15→2027-05-31 |
| BIOCULTURA | ACTIVE | 2 | 2026-10-10 | 2026-10-01→2027-03-01 |
| RIF | ACTIVE | 2 | 2026-10-14 | 2026-11-01→2027-03-01 |
| CIELO | ACTIVE | 1 | 2026-10-10 | 2026-10-01→2026-12-10 |
| AUTOAMERICAS | ACTIVE | 2 | 2026-10-28 | 2027-01-01→2027-04-01 |

## Gates

| Hotel | Ready | Watch | Pursuits |
|-------|-------|-------|----------|
| AC | 0 (was 0) | 2 | 2 |
| RAD | 0 (was 0) | 3 | 3 |
| YOTEL | 6 | campaigns 10 | — |
| Bethesda | Ready 28 | — | — |

## What this does not do

- Broad SERP every cycle
- Apify
- Invent participant lists
- Lower Ready/Watch thresholds
- Auto-promote on publication alone
- Treat AUTOAMERICAS Dominican Fiesta as a Radisson opportunity
