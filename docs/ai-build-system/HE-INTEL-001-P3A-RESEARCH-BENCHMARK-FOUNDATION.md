# HE-INTEL-001 P3A — Research Method Benchmark Foundation

**Contract:** `HE_INTEL_001_RESEARCH_METHOD_BENCHMARK_V1` (`1.0.0-p3a-foundation`)  
**Paid budget:** **$0** (no paid Exa / Parallel / SerpAPI / Surfe / PDL)  
**Parent:** Research Engine V1 + Airtable Graph V1 (reuse only; no competing framework)  
**Graph base (reference only):** `appCCUsuGsE1ifoLk` — **no production canonical writes in P3A**

## Purpose

Provider-neutral evaluation harness to decide which research methods deliver the highest-quality **verified** Hotel Intelligence at the lowest **actual** cost.

## Cohort

Frozen six-hotel evaluation (exact HPC IDs from pilot manifest):

| # | Hotel | HPC ID |
| --- | --- | --- |
| 1 | Bethesda Marriott | `recLuxvwwxID7U2B8` |
| 2 | Wyndham San Diego Bayside | `recnPB3Q02NO8Ma1o` |
| 3 | Renaissance New York Times Square | `recG66DQJKP2c0UNh` |
| 4 | The Ritz-Carlton, Mexico City | `recvVZ5unh4d3Krzi` |
| 5 | Hyatt Centric Playa del Carmen | `recBJZwMvgbdG7y4F` |
| 6 | Bruma Wine Resort | `recrTWMTHXQpA5geC` |

Full frozen pilot remains **16** experiences / **15** canonical hotels.

Fixture: `fixtures/hotel-explorer/HE-INTEL-001-p3a-six-hotel-benchmark-cohort-v1.json`

## Modes

| Mode | Seed | Isolation |
| --- | --- | --- |
| `INDEPENDENT_DISCOVERY` | Same limited hotel identity | No cross-arm discovery sharing |
| `INCREMENTAL_PRODUCTION` | Same frozen verified prior snapshot + unresolved gaps | Priors tagged `KNOWN_PRIOR`; new facts `DISCOVERED` |

## Arms (feasibility)

Exa · Parallel Task · Parallel Search · SerpAPI · Dealality native · Hybrid waterfall · Mock (offline)  
Brave = not ready (no adapter/key). Surfe/PDL excluded.

## Quality

Separated layers: discovered claim → supporting source → independent verification → confidence → acceptance → graph mapping → publication eligibility.  
No evidence → **UNKNOWN** (never accepted). Inferred emails rejected. Provider self-confidence is not proof. Blind adjudication supported.

## Cost ledger

Search requests, tokens, credits, failures/retries, org-level and per-hotel incremental costs, actual vs estimated, UNKNOWN labels. Shared budget cannot be bypassed by parallel arms. P3A hard cap = **$0**.

## Proposed minimum paid budget (P3B — not authorized)

Hard cap **$5.00** USD (M1 mini-pilot). See `proposedMinimumPaidBenchmarkBudget()`.

## Commands

```bash
npm run test:he-intel-001-p3a-research-benchmark-v1
npm run dry-run:he-intel-001-p3a-research-benchmark
```

## P3B plan

Founder authorize M1 → enable paid gateway under shared ledger → 3-hotel mini-pilot Mode A/B → blind scoreboard → amend M2 only after review. STOP conditions include unknown pricing under USD cap, leakage, Surfe, and production graph writes.
