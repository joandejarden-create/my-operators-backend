# Normalized metric contract

Machine file: `helena-cmo-live-metrics-v1.json`

Rules:
- Connector status is **dynamic** from live probes
- No hard-coded `liveGa4 = false` as permanent SoT
- Stale snapshots allowed only with `freshness: STALE`
- Reports must not commit Airtable record IDs or PII
