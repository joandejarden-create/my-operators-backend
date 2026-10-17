# Full GDI E2E — Operator Runbook

**Contract version:** `gdi-full-e2e-contract-v1.5` (fail-closed; required lessons capture; honest five-axis proof)  
**Coordinator:** `scripts/gdi-research-to-page-workflow-v1.mjs`  
**Evaluator:** `lib/group-demand-intelligence/workflow/full-e2e-completion-evaluator-v1.js`  
**Lessons capture:** `lib/group-demand-intelligence/workflow/full-e2e-lessons-capture-v1.js` (stage `P` / `capture_lessons`)  
**Verification adapters:** `lib/group-demand-intelligence/workflow/full-e2e-verification-adapters-v1.js`  
**Publication quality:** `lib/group-demand-intelligence/workflow/full-e2e-publication-quality-v1.js`  
**Status CLI (read-only):** `scripts/gdi-full-e2e-status-v1.mjs` — writes only under `--out-dir`, never into the source run journal  
**Lesson register:** `LESSON_REGISTER.md` · reusable Cursor prompt: `CURSOR_PROMPT_FULL_E2E.md`

Research spend authorization (`--authorize-paid-discovery` / `--authorize-surfe`) is **separate** from publication (`--authorize-publish`). `--dry-run` overrides every paid/write flag.

**Next hotel uses this entry point only** — do not create a hotel-specific eligibility-repair or overlay script as the primary path.

Distinctions enforced by the contract:

| Concept | Meaning |
|--------|---------|
| Task execution | Coordinator stage completed/failed |
| Research coverage | Declared segment scope satisfied with evidence |
| Publication | Qualified opportunities persisted **with** fit/drawers/eligibility/dedupe gates |
| Customer delivery | Local **and** production verification of list/drawers/reports/links |
| Lessons capture | Required stage before COMPLETE — not a hotel-only report appendix |

Association-only publication while other applicable segments remain plan-only **never** yields `COMPLETE`. Sports/educational axes require **supported** independent assessment (not merely absence of a proxy flag). Contact depth cannot be skipped because an organization route exists. Canonical eligibility uses the use-case suitability contract — `whyThisMatters` / `proposedHotelUse` alone are insufficient. Assessment/drawer readiness gates authorized publication writes. Provider metadata must never reach customer DTOs. False contact EXHAUSTED without documented attempts is forbidden. Lesson register statuses are five-axis — do not treat fixture rejects or status labels as full incorporation.

### Defect-fix rule (same task)

For every defect discovered or correction made, update in the **same** task:

1. Shared implementation (where applicable)  
2. Regression/integration test  
3. Lesson register  
4. This runbook + `CURSOR_PROMPT_FULL_E2E.md`  

Track separately: Documented · Implemented (shared path) · Tested · Executed successfully · Production-verified. A hotel-specific repair is **not** a completed process improvement until the shared path prevents recurrence or explicitly detects and routes it.

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

## Delivery verification + budget reconcile (v1.3 fail-closed)

Adapters: `runLocalDeliveryVerification` / `runProductionDeliveryVerification` / `reconcileAuthoritativeBudget`.

**Delivery is not proven by** HTTP 200 alone, empty JSON fallback, asset presence, or a caller-supplied SHA.

Required for `passed`:

1. Valid JSON object schema with `opportunities` array  
2. Served `hotelId` matches expected hotel  
3. Published IDs reconciled to the publication manifest (legacy IDs explicit)  
4. List/detail parity sample for scoped published IDs  
5. Zero prohibited provider metadata in customer DTOs (`trun_`, Surfe/Parallel cost fields, etc.)  
6. When an intended build SHA is asserted: **observed** runtime identity from `/health` (or supported headers) must match — caller SHA is not observation  

API · browser · share · PDF · narrative remain **separate** surfaces.

**Budget reconcile:** read `authorization` / `authorizationId` from the ledger **first**, then compare to `--authorization-ref`. Mismatch, missing identity, wrong hotel scope, invalid cap/amounts → `reconciled: false`. Never bootstrap an empty ledger or mint allowance from CLI args.

```bash
npm run test:gdi-full-e2e-contract
```

---

## Lessons capture (required — stage P)

Coordinator order ends with: `reconcile_run` → **`capture_lessons`** → `evaluate_completion`.

Artifact: `<runDir>/full_e2e/lessons-capture-v1.json`.

Evidence overlay may declare what this run incorporated:

```json
{
  "incorporatedLessonIds": [
    "provider_leak_customer_sanitize",
    "false_contact_exhaustion_forbidden"
  ],
  "hotelSpecificRepairsClaimedComplete": []
}
```

Closeout **must** identify:

1. Lessons incorporated during this run (with code / test / evidence / release refs), **or** `noNewLessonsThisRun=true` with coordinator execution proof  
2. Remaining implementation or verification gaps (five-axis)  
3. Next-hotel inheritance **by mode**: automatically_performs · automatically_validates · detects_gap_requires_operator · not_yet_released  

`coordinatorExecutionProof` must include `runId`, `hotelId`, `artifactPath`, `capture_lessons`, and `gitSha`/`releaseRef`. A generated checklist alone does **not** satisfy the gate.

Missing capture, empty register, missing inheritance/gaps, missing execution proof on a no-new-lessons run, or hotel-only repair claimed complete without shared-path prevention → **cannot COMPLETE**.

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
  "budget": { "reconciled": true },
  "incorporatedLessonIds": []
}
```

Primary local service remains **`:8080`**. Auth-blocked browser checks must be `blocked` / `not_run` — never substituted with a weaker asset check and called passed.

Client delivery has five separate proofs. Do not collapse them:

| Proof | What it shows |
|-------|----------------|
| Share validity | Token verifies, is not revoked or expired, and is scoped to this hotel |
| Property and report availability | The published report payload loads for that property |
| PDF availability | The monthly-review PDF or current-report PDF file for that product opens |
| Browser rendering | The external page shows the hotel report, not only HTTP 200 on the HTML shell |
| Authenticated Admin actions | Open, Copy, View, and Download were clicked in a real Memberstack admin session |

ADP property lookup uses the property profile fixture when it is present, and otherwise the shipped published manifest. A lean image that omits `fixtures/` must still serve `data/ai-demand-positioning/published/<propertyId>/`. That lookup does not read Group Demand Intelligence files.

---

## Regression

```bash
npm run test:gdi-full-e2e-contract
npm run test:gdi-customer-provider-leak-sanitize-v3
npm run test:adp-published-profile-without-fixtures
```
