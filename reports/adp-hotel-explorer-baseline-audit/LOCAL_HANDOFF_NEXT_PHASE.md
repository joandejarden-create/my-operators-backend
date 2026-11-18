# Local handoff — after Phase 1 / Phase 1.1 (Radar HE + ADP gates)

> **Phase 1.1 package:** `phase-1.1/FOUNDER_STOP_PHASE1_1.md` and sibling reports in `phase-1.1/`.  
> **Windows local transition:** `phase-1.1/LOCAL_WINDOWS_TRANSITION_V1.md` (head `9c60680c`).  
> **Do not merge PR #364** — local validation next; no Phase 2 until authorized.

## Git identity

| Item | Value |
|---|---|
| Worktree | `/agent/repos/my-operators-backend-adp-audit` |
| Branch | `cursor/adp-hotel-explorer-audit-1ed2` |
| Do not touch | `/agent/repos/my-operators-backend` (GDI primary) |

```bash
cd /agent/repos/my-operators-backend-adp-audit
git status -sb && git rev-parse HEAD
git -C /agent/repos/my-operators-backend status -sb
```

## Architecture docs

1. `PHASE1_PROGRESSIVE_ENRICHMENT_ARCHITECTURE.md` — popup inventory, identity, enrichment, tab map, files  
2. `ADP_HOTEL_EXPLORER_BASELINE_AUDIT_V1.md` — original baseline audit  
3. `PHASE2_ADP_FULL_E2E_ROADMAP.md` — next reliability factory  

## Phase 1 what shipped

- HE CSS/JS on `public/deal-capture-radar-with-ranked-list.html` (HE primary shell; Radar tabs via HDP embed)  
- `lib/hotel-intelligence/radar-hotel-explorer-progressive-enrichment-v1.js`  
- Bethesda internal census eligibility (no share pack)  
- Reconstructed certification baseline from published manifests  
- npm + PR matrix + CI for free Phase 1 gates  
- Safe null-guards: BPP profile, territory labels; restored `applyHistoricalRecordsToAirtable` dry-run export  
- Foundation test aligned to Airtable overlay compaction contract  

## Commands

```bash
npm ci
npm run test:adp-phase1-radar-he-gates
npm run test:adp-customer-surface-contract   # may still surface deeper evidence issues
npm run test:adp-permanent-full-e2e-certification-v1  # exit 2 until durable fixtures present
npm run rebuild:adp-existing-hotel-certification-baseline-v1 -- --dry-run
```

## Restrictions (unchanged)

No merge/deploy/production Airtable/customer visibility/paid research without explicit founder approval.
