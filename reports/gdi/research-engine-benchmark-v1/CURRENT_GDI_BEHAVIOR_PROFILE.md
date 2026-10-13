# Current GDI Research Behavior Profile

Evidence: `native-blind-discovery.js`, Mallorca yield forensic QUERY_BUDGET_AUDIT, funnel CSVs, international-discovery-v2 spines.

## Measured

| Metric | Castillo | Sheraton | Notes |
|---|---|---|---|
| Queries / investigation | 30 | 30 | native SERP budget |
| Domains / sources returned | 20 | 33 | organic pages fetched |
| Reformulations | template/stratified tasks | same | `buildDiscoverySearchTasks` + lexicon |
| Languages | es/en/ca | es/en/ca/de | multilingual present |
| Page depth | shallow (SERP hit → single fetch) | same | no recursive open-next-clue loop |
| Link following | weak | weak | no systematic in-domain crawl |
| PDF usage | opportunistic if SERP lands on PDF | same | not document-first strategy |
| Entity pivots | weak | weak | ACCOUNT_FIRST=0; CONTROLLER_FIRST empty spine |
| Controller pivots | manual post-hoc (2–3) | manual (3) | not research-engine driven |
| Research iterations | ~1 pass | ~1 pass | stop at query budget / extract |
| Stop condition | query/source budget + classify | same | not "missing pillar" chase |

## Research style classification
**QUERY_AND_CLASSIFY** (primary).

Secondary: light multilingual template expansion. Missing: recursive navigation, site deep-dive, document-first, next-evidence policy.

## Default model
`gpt-4o-mini` for JSON extract after fetch. Search = SerpAPI. No browser agent in production native path.
