# Existing schema reuse vs minimal additions

## Reuse as-is (no schema change)

| Asset | Role |
|-------|------|
| Group Demand Opportunities | Layer B — qualified GDO only |
| Demand Generators | Global org identity |
| Demand Programs / Signals | Program & signal authority |
| Private Event Venues (+ signals, hotel venue fit) | Venue identity |
| Hotel Demand Generator Fit | Hotel↔DG commercial fit when qualified |
| GDI Research Runs | Run provenance summaries (optional) |
| Commercial payload durability (PR #366) | Preserve thesis / contacts / evidence on GDO writes |

## Minimal addition (founder schema approval required)

| Asset | Why |
|-------|-----|
| **GDI Hotel Research Opportunities** | Layer A durable Airtable mirror for research-only / watchlist / exploratory hotel opportunities without implying GDO qualification |

## Explicitly rejected misuses

- Forcing research-only rows into GDO to match counts
- Research Targets as opportunity catch-all
- Research Runs as opportunity registry
- Hotel commercial profiles / need-period tables as opportunity store
- Creating a second competing GDI platform

## Aggregate dry-run (this run)

- Hotels inventoried: **19**
- Local research opportunities: **659**
- Research rows pending schema table: **659**
- Qualified GDO CREATE candidates: **0**
- Qualified GDO already present / unchanged: **509**
- Conflicts quarantined: **2**
