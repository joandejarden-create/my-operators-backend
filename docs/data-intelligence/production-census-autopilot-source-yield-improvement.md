# Production Census Autopilot — Source Yield Improvement

## Problem

Multi-queue orchestration worked, but controlled High yield was ~1 because:

1. Description fetch budget was burned on Hilton/Marriott/Choice **403** pages  
2. IHG descriptions/amenities were **already populated**  
3. Address dry-run **skipped** records that already had coordinates (most Hilton/Choice blanks)  
4. When geocode was deferred, confirmed official addresses were **dropped** (proposal=null)

## Fix principles

- Keep High-only Autopilot would-writes  
- Prefer fetchable families (IHG); circuit-open blocked families after repeated 403s  
- Address-only High proposals from VIC High street addresses + official page snippets without geocode  
- Report yield taxonomy + apply recommendation (&lt;10 → improve extractors; ≥10 → bundle-bound apply)

## Operating guidance

```bash
npm run census:autopilot -- --region CALA --scope active-brand-setup --mode controlled \
  --strategy fastest-safe --run-until-complete --batch-size 250
```

Artifacts per run:

- `source-yield-diagnostic.{md,json}`
- `approval-bundle.json` (includes `apply_recommendation`)
- `webhound-candidates.json` (learning only; never production writes)

## Known remaining blockers

- Marriott/Hilton/Choice **description** pages still bot-blocked from Node fetch  
- IHG rooms often lack published `numberOfRooms`  
- Geocode remains soft-deferred until Mapbox Permanent / Google storage terms  

## Status

`production_census_autopilot_source_yield_improved_ready_for_controlled_review`
