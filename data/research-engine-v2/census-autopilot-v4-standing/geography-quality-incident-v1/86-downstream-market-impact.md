# Downstream Impact — Market / Submarket

## Systems that read Market / Submarket

| System | Dependency | Blank safer than Country-as-Market? |
| --- | --- | --- |
| Hotel Property Census views/filters | Display + filter | **YES** — wrong Country filters mislead |
| Brand Explorer / census affiliation | Soft geography context | YES |
| Market Intelligence / Radar | Market corridors | YES — wrong Market poisons density |
| Nearby Supply / comparables | Market grouping | YES |
| Operator Explorer seeds | Optional geography | YES |
| Frontend Census UI | Display | YES if empty state shown |
| Airtable formulas | Unknown — verify before clear apply | **Check live formula refs** |

## Recommendation

1. Prefer `SAFE_MARKET_CORRECTION` when deterministic.
2. Prefer `SAFE_MARKET_INVALID_CLEAR` over retaining Country-as-Market.
3. Store `market_resolution_status=UNRESOLVED` outside the Market text field.
4. Before apply: scan Airtable formula fields for `Market` references (manual steward step).

## Breaking risk

Low for display filters; medium if formulas assume Market always equals Country for rollups — those formulas are themselves wrong and should be fixed.

**Verdict:** Invalid Market can safely be cleared **if** empty-state UI + optional status field / claim store are ready. No code path found that requires Country-as-Market to remain.
