# HIF V6 — Weekly Regional Coverage & Research Yield Scorecard

Permanent operating scorecard inside existing HIF reporting infrastructure.

**Questions answered:** Where are we finding hotels? Verified contracts? Owners? Commercially useful opportunities? Where should the next research dollar go?

## Commands

```bash
npm run test:hif-weekly-regional-yield-scorecard-v6
npm run hif-weekly-regional-yield-scorecard -- --week 2026-W47
```

## Layout

| Path | Purpose |
|---|---|
| `snapshots/YYYY-Www.json` | Immutable weekly totals |
| `deltas/*_to_*.json` | Week-over-week change (reclassification ≠ new) |
| `exports/*-FOUNDER_OVERVIEW.md` | Founder executive overview |
| `exports/*-GAPS.json` | Regional / source gaps |
| `exports/*-RECOMMENDATIONS.json` | Yield-based investment recommendations |
| `ledgers/REGIONAL_COVERAGE_YIELD_REGISTER.json` | Coverage register mirror |
| `AIRTABLE_ADDITIVE_PROPOSAL.md` | Optional schema — **not applied** |

## Rules

- Geography = physical hotel location; source jurisdiction separate
- Coverage ≠ qualification ladder (9 independent rungs)
- Track A / Track B compared; combined totals deduplicated
- CUSTOMER_BLOCKED; no publication bypass
- Zero findings ≠ proof no opportunities exist
- Worldwide eligibility; no research-horizon filter

## Baseline (2026-W47)

Seeded from V5 under V6 taxonomy: US / North America only with actual research; 8 intensive regions still needing adapters. `newlyDiscoveredHotelsThisWeek = 0` on seed (cumulative inventory, not a discovery week claim).
