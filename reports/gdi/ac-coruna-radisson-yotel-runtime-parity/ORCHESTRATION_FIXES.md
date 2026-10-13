# Orchestration Fixes

1. **Hotel-agnostic campaign decomposition** in `research-orchestrator.js` (was YOTEL-only).
2. **hotelContext / thesis** no longer default to YOTEL Geneva copy.
3. **Radisson MARKET_LANGUAGE_PROFILE** + locale on AC/RAD/YOTEL configs.
4. **`buildGdiDiscoveryQueries`** shared helper; wired into Ten Bases provenance path.
5. **Multilingual evidence normalizer** (buyer roles + lodging classes).
6. **Spanish/Galician LANG_TERMS** expanded; place-generic FR/DE templates (removed hard-coded Genève-only where place exists).
7. **Apify opt-in only** in Ten Bases.
8. **Radisson** added to opportunity-discovery V5 hotel list.

No hotel-specific forks. No Ready threshold changes. No Apify. No pursuit state churn.
