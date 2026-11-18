# Phase 2A — Remaining blockers and resource requirements

| ID | Blocker | Type | Resource required | Blocks |
|----|---------|------|-------------------|--------|
| B1 | Durable `current/` manifests missing | Access / infra | Railway volume mount or governed durable export into ADP worktree | Layer B permanence PASS |
| B2 | Measured-dates production JSON missing | Access / export | Read-only export of `PRODUCTION_DURABLE_PUBLISHED_VS_MEASURED.json` | Persistence Layer B reconciliation |
| B3 | Private certification matrix absent | Authority | Locate original freeze **or** founder retires reconstruction as inventory-only | certifiedCount > 0 claims |
| B4 | Customer-surface 33 failures | Evidence + measurement + historical | See failure register — not a single code patch | Production surface claim |
| B5 | BPP measurement / peer freeze incomplete | Paid + governance | Founder-authorized BPP cycles + peer approvals | BPP READY |
| B6 | Bethesda territory rate lock | Historical cert | Pin or re-cert authorization | Bethesda surface PASS |
| B7 | 5 canonical hotels lack local published FS | Coverage | Locate remote packages or authorize Full E2E | Portfolio completeness locally |
| B8 | Worktree 12 commits behind main + dirty 1.3A/B | Integration | Founder-chosen preserve-then-branch plan | Clean ADP PR onto main |
| B9 | No single npm Full E2E CLI alias | DX | Small engineering follow-up (Phase 2B) | Operator ergonomics |
| B10 | HE browser smoke / Airtable in ADP env | Env | Credentials in ADP worktree if needed | Live HE proof (separate track) |

## Access request (precise)

To unblock Layer B only, founder (or ops) must provide **read-only** access to:

1. Durable published volume contents for at least the gate reference property:  
   `data/adp-durable-published/current/adp_hilton_times_square/manifest.json` (+ peer `current/` set used by permanence).  
2. Optional: measured-dates production reconciliation export.

No credentials or durable package bodies should be committed to Git.
