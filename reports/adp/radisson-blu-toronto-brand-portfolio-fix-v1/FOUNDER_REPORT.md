# FOUNDER REPORT — ADP Brand & Portfolio Position Rendering Fix V1

## Verdict

**YES — Radisson Blu Toronto Downtown now displays the actual Choice Privileges portfolio measurement instead of the stale “measurement is being established” fallback.**

All portfolio metrics come only from Loyalty_BPP `…308ba1` (28 scenarios / 112 observations). Toronto remains founder-reviewed staged and not customer-published.

## Root cause

`resolveBrandPortfolioPosition` ignored durable/staged measurement-available BPP and fell back to awaiting copy because customer publication was false. SPA readiness also rejected `STAGED_COMPARISON_CERTIFIED`.

## Toronto portfolio (resolved)

| Field | Value |
|------|------|
| Lens | Choice Privileges |
| Period | …308ba1 |
| Scenarios | 28 |
| Observations | 112 |
| Peer count | 6 |
| Portfolio AI Presence | 45.5% (51/112) |
| Portfolio Rank | #1 of 6 |
| #1 Appearance | 0.0% (0/28) |
| Top-3 Appearance | 92.9% (26/28) |
| Positive evidence | 51 |
| Missing evidence | 61 |
| Portfolio displacement | 3 |

## Governance

| Field | Value |
|------|------|
| Founder reviewed | YES |
| Founder approved (customer) | NO |
| Customer ready | NO |
| Customer published | NO |
| Portfolio measurement available | YES |
