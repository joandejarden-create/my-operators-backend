# Full CALA 15K — Source Inventory

Generated: 2026-08-09T18:32:37.036Z
Cvent artifacts present: **true**


| Source | Type | Count | Insert identity | Field provenance | Use |
| --- | --- | ---: | --- | --- | --- |
| `reports/research-engine-v2/hbx-cala-wave1-candidate-pack.json` | hbx_content_api | 3385 | true | partial_internal_only | primary Wave1 identity + contact for MX/DO/CO/CR/PA |
| `data/research-engine-v2/census-autopilot-v2-full-universe/candidates/*.json` | cvent_challenge_universe_plus_independent | 14035 | true | false | shell identity discovery; validate fields via HBX/official later |
| `reports/research-engine-v2/cvent-latam-harvest-inventory-summary.json` | cvent_harvest_inventory | 13369 | true | false | coverage reference; do not scrape live |
| `data/research-engine-v2/census-autopilot-v4-full-universe/27-universe-ledger-index.json` | universe_ledger_index | 15198 | false | false | coverage planning / status reference |
| `reports/research-engine-v2/autopilot/**/enrichment-candidates.json` | dataforseo_local_candidates | run-scoped | true | partial_with_policy | post-shell enrichment; optional identity if match_high |
