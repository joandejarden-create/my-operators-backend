# SerpApi Result Cache Design

Path: `data/research-engine-v2/serpapi-research-cache/`

Keys: query + normalized identity + property_token + property_identity_id + request type + dates/gl

Stores: retrieved_at, response_hash, source_state, match_confidence, eligible_fields, raw path, expiry (30d)

Separates Dealality reproducibility cache from SerpApi provider cache (which may be free). Never stores API keys.
