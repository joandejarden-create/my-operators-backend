# Webhound Behavior Profile

Primary evidence: Mallorca session `c1b18a88-279f-480d-80a3-630f048b0c5b` (cost $5, ops 116).

## Measured (supported by cost rollups + source inventory)

| Metric | Value | Evidence |
|---|---|---|
| Queries / investigation | 13 searches | operation_type_rollups.search.count |
| Page visits | 42 | page_visit count |
| LLM reasoning cycles | 61 | llm_tokens count |
| Sources cited | 66 unique | webhound_get_sources |
| Avg pages per search | ~3.2 | 42/13 |
| PDF usage | PRESENT (program/brochure/product pages among sources; exact PDF MIME count UNKNOWN without full message dump) | source URLs include congress/programme/product |
| Registration / venue portals | YES | e.g. fetalmedicine, ecio.org venue, coupled2027, caib.es |
| Local-language usage | YES | seorl.net, sedisa.net, caib.es, rfegolf.es, fueib.org (ES); DE golf-extra.com |
| Cross-domain pivots | YES | congress site → venue/travel → housing/tour operator domains |
| Entity pivots observed | organizer/event → golf operator product; event → accommodation page; hotel brand page → events | source list |
| Organizer → controller | PARTIAL | housing pages + tour operators appear; not always labeled as controller in output |
| Controller → client | WEAK in output | DMC pages visited but few named end-clients |
| Event → participant | PARTIAL | exhibitor/participant not fully expanded to named traveling buyers |
| Source revisit | UNKNOWN | not in cost rollups |
| Stop reason | budget boundary ($5) | cost.summary.total_cost ≈ 5.0 |

## Research style classification
**HYBRID** with dominant **RECURSIVE_NAVIGATION** + **ENTITY_GRAPH_EXPANSION** tendencies.

Not pure QUERY_AND_CLASSIFY: page_visit (42) >> search (13).

Not DOCUMENT_FIRST exclusive, but document/programme pages are followed when discovered.

## Unsupported (do not infer)
- Exact link-following depth histogram
- Exact query reformulation text sequence (MCP session messages truncated in local extract)
- LangSmith-equivalent step planner dump
