# PR1 — Global Intelligence Retention Foundation

## Delivered

- Canonical global demand identity (`stableGlobalDemandId` / identity key)
- Lifecycle states (`RAW_DISCOVERED` → `VERIFIED_GLOBAL_SIGNAL` … `REJECTED_UNSUPPORTED`)
- Hotel match reasons including `OUT_OF_MARKET_FOR_CURRENT_HOTEL` (not global rejection)
- Local filesystem store under `data/group-demand-intelligence/global-intelligence/`
- Privacy strip + tenant isolation helpers
- Airtable dry-run mapping + activation plan (writes blocked)
- Hook in `original-full-e2e-research-chain-v1.js` after discovery (all cell candidates)

## Modules

- `lib/group-demand-intelligence/global-intelligence/gdi-global-discovered-intelligence-contract-v1.js`
- `lib/group-demand-intelligence/global-intelligence/gdi-global-intelligence-store-v1.js`
- `lib/group-demand-intelligence/global-intelligence/gdi-global-intelligence-privacy-v1.js`
- `lib/group-demand-intelligence/global-intelligence/gdi-global-intelligence-airtable-mapping-v1.js`
- `scripts/test-gdi-global-intelligence-retention-v1.mjs`

## Acceptance

| Criterion | Status |
|---|---|
| Out-of-market discoveries not silently lost | Yes — retained as verified/screened global |
| Unsupported not promoted as verified | Yes |
| Stable identity / no duplicate globals for same demand | Yes (idempotent upsert) |
| Provenance fields present | Yes (run IDs, provider, evidence URLs) |
| Hotel confidentiality isolated | Yes (denylist + tests) |
| CURRENT packages unchanged | Yes (fingerprint assert) |
| Airtable production writes | Blocked / dry-run only |

## Not in PR1

- Screen-all before deep cap (PR2)
- Published-package pre-deep dedupe (PR2)
- Selection ranking register (PR3)
- Local social/planner/venue tracks (PR4)
- Exa mission rewrites (PR5)
