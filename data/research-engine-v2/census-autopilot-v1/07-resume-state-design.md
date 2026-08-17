# Resume State Design

File: `data/research-engine-v2/census-autopilot-v1/runs/<run_id>/resume-state.json`

Stores:

- run ID, mode, scope filters
- progress (completed / failed / remaining)
- completed entity IDs
- failed entities + retry state
- source failures
- research checkpoints
- observability snapshot

Resume: `npm run census:autopilot-v1 -- --resume <run_id>`

Does **not** restart an entire country/group because one source fails — skips completed IDs.
