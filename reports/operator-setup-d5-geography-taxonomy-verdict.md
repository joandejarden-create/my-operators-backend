# D.5 Geography Taxonomy Verdict

## Chosen model: **Option C (+ B companion)**

**Keep CALA / product `Active Countries` scoped separately from Global Presence.**

| Layer | Role | Fit use |
| ----- | ---- | ------- |
| `Active Countries` | Verified **current** countries in Dealality product taxonomy (CALA-weighted + US/Spain where tracked) | Primary geo match for CALA deals |
| `Active Regions` (**recommend add**) | Global region summary (APAC, MEA, Europe, Africa, North America, LatAm/Caribbean) | Global operator context; not a substitute for countries |
| `specificMarkets` | Free-text overflow / footnotes | Not Fit-canonical |
| Market Presence + Assignments | **Canonical OE source of truth** | Always prefer OE over Setup typing |

### Why not A alone

Expanding `Active Countries` to every Shangri-La country would bloat a CALA-product select and mix Dealality Market geography with global brand footprint.

### Why not B alone

Regions without country truth are too coarse for Fit geo scoring; regions complement countries.

### `Other` policy

**`Other` is invalid as a Fit geography input.** Replace with: empty Active Countries (no taxonomy match) + Global Presence note in `specificMarkets` / future `Active Regions`, after Market Presence enrichment where Dealality tracks those countries.

### Shangri-La resolution

1. Clear `Active Countries = Other`.
2. Set `Market Presence Type` to **No known presence** for the **CALA Active Countries** lens (honest: no taxonomy countries).
3. Keep `specificMarkets` global footprint note (APAC/MEA/Europe/Africa; ~106 hotels / 22 countries YE2025).
4. Future: add `Active Regions` = Asia Pacific; Middle East; Europe; Africa (and enrich Market Presence before any country writes).
