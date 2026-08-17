# City Resolver V4

**Version:** `city-resolver-v4-2026-08-08`  
**Module:** `lib/research-engine-v2/census-autopilot-v3/geography/city-resolver-v4.js`

## Priority

1. Official structured locality  
2. Official property address parse (BR/AR/CR specialized)  
3. Official URL locality slug (IHG / Choice structure)  
4. Retain plausible production City  
5. Research-only SerpApi address locality (not production-eligible)

## Never

Hotel title · marketing name · brand · description · Cvent · legacy Census · Country/State as City

## Layers preserved

`official_locality` · `municipality` · `city` · `tourism_destination`

Census **City** = canonical locality; Market/Submarket stay in Dealality registry.
