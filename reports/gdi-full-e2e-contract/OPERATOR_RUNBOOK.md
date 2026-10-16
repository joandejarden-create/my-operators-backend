# Full GDI E2E — Operator Runbook

**Contract version:** `gdi-full-e2e-contract-v1.1` (fail-closed)  
**Coordinator:** `scripts/gdi-research-to-page-workflow-v1.mjs`  
**Evaluator:** `lib/group-demand-intelligence/workflow/full-e2e-completion-evaluator-v1.js`  
**Status CLI (read-only):** `scripts/gdi-full-e2e-status-v1.mjs` — writes only under `--out-dir`, never into the source run journal  

Research spend authorization (`--authorize-paid-discovery` / `--authorize-surfe`) is **separate** from publication (`--authorize-publish`). `--dry-run` overrides every paid/write flag.

Distinctions enforced by the contract:

| Concept | Meaning |
|--------|---------|
| Task execution | Coordinator stage completed/failed |
| Research coverage | Declared segment scope satisfied with evidence |
| Publication | Qualified opportunities persisted |
| Customer delivery | Local **and** production verification of list/drawers/reports/links |

Association-only publication while other applicable segments remain plan-only **never** yields `COMPLETE`.

---

## Supported commands (implemented and verified)

### Run Full GDI E2E for a hotel under authorization

```bash
node scripts/gdi-research-to-page-workflow-v1.mjs \
  --hotel <canonicalHotelId> \
  --mode full-e2e \
  --authorization-ref "<authorization ledger / ticket id>" \
  --budget-usd <N> \
  --authorize-paid-discovery \
  --authorize-surfe \
  --out-dir reports/gdi-full-e2e-contract
```

Publish path (only when authorized; still requires local+production evidence for `COMPLETE`):

```bash
node scripts/gdi-research-to-page-workflow-v1.mjs \
  --hotel <canonicalHotelId> \
  --mode full-e2e \
  --authorization-ref "<authorization>" \
  --authorize-paid-discovery \
  --authorize-publish \
  --target-env production \
  --adapter airtable \
  --out-dir reports/gdi-full-e2e-contract
```

npm shorthand (still requires `--hotel` and flags):

```bash
npm run gdi:full-e2e -- --hotel <canonicalHotelId> --authorization-ref "<auth>" --dry-run
```

### Dry-run / preflight (no paid calls, no production writes)

```bash
node scripts/gdi-research-to-page-workflow-v1.mjs \
  --hotel <canonicalHotelId> \
  --mode full-e2e \
  --dry-run \
  --authorization-ref "<authorization>" \
  --out-dir reports/gdi-full-e2e-contract
```

`--dry-run` blocks paid Parallel/Surfe and production writes **even if** `--authorize-paid-discovery` / `--authorize-publish` are also present.

### Resume (skips stages already `completed` in the run ledger)

```bash
node scripts/gdi-research-to-page-workflow-v1.mjs \
  --hotel <canonicalHotelId> \
  --mode full-e2e \
  --resume-run <runId> \
  --authorization-ref "<authorization>" \
  --out-dir reports/gdi-full-e2e-contract
```

### Read-only status / completion evaluation (safe for active Ren/Tampa/ADP)

Does **not** mutate journals, resubmit research, or publish:

```bash
node scripts/gdi-full-e2e-status-v1.mjs \
  --hotel <canonicalHotelId> \
  --run-dir <path-to-existing-run>

# or from evidence fixture
node scripts/gdi-full-e2e-status-v1.mjs --evidence <path/to/evidence.json>

# emit machine-readable contract document only
node scripts/gdi-full-e2e-status-v1.mjs --write-contract-doc --out-dir reports/gdi-full-e2e-contract
```

npm: `npm run gdi:full-e2e-status -- --hotel <id> --run-dir <path>`

### Weekly incremental refresh (distinct mode — not Full E2E)

```bash
node scripts/gdi-weekly-refresh-coordinator-v1.mjs --hotel=<canonicalHotelId>
```

Or coordinator stage:

```bash
node scripts/gdi-research-to-page-workflow-v1.mjs \
  --hotel <canonicalHotelId> \
  --stage weekly_refresh \
  --dry-run
```

A completed weekly refresh **must not** be relabeled as full initial market coverage `COMPLETE`.

---

## Evidence overlay for delivery proofs

Record verification outcomes in the run directory as `full-e2e-evidence.json` (or pass `--completion-evidence <path>`):

```json
{
  "segmentCoverage": [],
  "contactDepth": {},
  "publication": {},
  "localVerification": { "status": "passed", "verifiedAt": "...", "buildSha": "..." },
  "productionVerification": { "status": "passed", "verifiedAt": "...", "dataRevision": "..." },
  "deliverables": [
    { "id": "customer_list_drawers", "status": "verified" },
    { "id": "hotel_scoped_share_link", "status": "verified" }
  ],
  "budget": { "reconciled": true }
}
```

Primary local service remains **`:8080`**. Auth-blocked browser checks must be `blocked` / `not_run` — never substituted with a weaker asset check and called passed.

---

## Regression

```bash
npm run test:gdi-full-e2e-contract
```
