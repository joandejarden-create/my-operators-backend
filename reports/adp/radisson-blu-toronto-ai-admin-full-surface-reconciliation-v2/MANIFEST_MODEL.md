# MANIFEST_MODEL

1. legacy published manifest — data/ai-demand-positioning/published/{id}/manifest.json
2. durable published-current — data/adp-durable-published/current/{id}/manifest.json (+ registry)
3. staged report — ADP staged certified packages
4. report snapshot — archived CURRENT/ARCHIVED report versions
5. AVA AUDIT_META — audit lifecycle / PDF / customer auth

Admin eligibility resolution order: legacy published → seed → durable current.
Do not require legacy published_manifest for durable-only founder-staged hotels.
