# AVA Permanent Audit Runner — Implementation Report (V1)

**Status:** `AVA_PERMANENT_AUDIT_RUNNER_READY_FOR_REVIEW`  
**Date:** 2026-11-06  
**Scope:** Local, reusable on-demand AVA V4 execution + immutable archive + Admin history.  
**Not claimed:** Production deploy, live Airtable persistence, customer publication, paid research.

---

## Exact files changed / added

### Added

| Path | Role |
|---|---|
| `lib/ai-visibility-audit-action-plan/ava-audit-execution-contract-v1.js` | `AVA_AUDIT_EXECUTION_CONTRACT_V1` |
| `lib/ai-visibility-audit-action-plan/ava-hotel-identity-resolver-v1.js` | Hotel identity (rec… / adp_* / slug) |
| `lib/ai-visibility-audit-action-plan/ava-audit-run-archive-v1.js` | Immutable local run archive |
| `lib/ai-visibility-audit-action-plan/ava-audit-history-compare-v1.js` | Period-over-period compare |
| `lib/ai-visibility-audit-action-plan/ava-audit-airtable-sync-v1.js` | Local mirror + gated live sync |
| `lib/ai-visibility-audit-action-plan/ava-permanent-audit-runner-v1.js` | Canonical orchestrator |
| `scripts/run-ava-permanent-audit-v1.mjs` | Node CLI |
| `scripts/Run-AVA-Audit.ps1` | PowerShell orchestrator |
| `scripts/test-ava-permanent-audit-runner-v1.mjs` | Regression suite |
| `docs/ava-permanent-audit-runner-airtable-schema-migration-v1.md` | Schema proposal (not applied) |
| `docs/ava-permanent-audit-runner-operator-guide-v1.md` | Operator docs |
| `reports/ava-permanent-audit-runner-v1/*` | Execution reports + this doc |

### Restored (prior ADP↔AVA permanent certification work lost from disk)

| Path | Role |
|---|---|
| `lib/.../adp-ava-reference-contract-v1.js` | ADP lean pin + reconcile |
| `lib/.../adp-ava-permanent-cross-report-certification-v1.js` | Customer ADP package + cert |
| `scripts/ava-adp-*-v1.mjs` + `scripts/test-adp-ava-*-v1.mjs` | Portfolio cert scripts/tests |

### Modified

| Path | Change |
|---|---|
| `api/admin-ai-visibility-audit.js` | Permanent runs list / compare / exact PDF |
| `server.js` | Route wiring |
| `public/js/admin-ai-visibility-audit.js` | Current Audit + permanent history + compare UI |
| `package.json` | npm scripts for runner + ADP cert |

---

## CLI usage

```powershell
.\scripts\Run-AVA-Audit.ps1 -HotelId "adp_cambridge_beaches_bermuda" -DryRun
.\scripts\Run-AVA-Audit.ps1 -HotelIds "adp_a","adp_b" -CertifyOnly
.\scripts\Run-AVA-Audit.ps1 -AllHotels -DryRun
.\scripts\Run-AVA-Audit.ps1 -AuditRunId "AVA-RUN-..." -Reproduce
```

```bash
node scripts/run-ava-permanent-audit-v1.mjs --hotel adp_cambridge_beaches_bermuda --dry-run
npm run test:ava-permanent-audit-runner
```

---

## Existing components reused

- AVA Full E2E: `ava-full-e2e-pipeline-v1.js` → `runMandatoryAvaEvidenceStages`
- Canonical Action Register, CQA, V4 HTML: `build-audit-pdf-html-v4.js`
- PDF generator: `generate-audit-pdf-v1.mjs` with `buildHtml: V4`
- ADP↔AVA permanent certification + reference contract
- Durable Admin store: `audit-store-v1.js`, `ava-pdf-archive-v1.js` (draft channel only; published untouched)
- Existing Admin PDF channels (`X-AVA-PDF-Sha256`, history channel)

**One** content pipeline · **one** PDF pipeline · no parallel methodology.

---

## Schema changes proposed / applied

| Item | Status |
|---|---|
| Airtable tables A–F (runs, findings, actions, metrics, evidence, report versions) | **Proposed only** — `docs/ava-permanent-audit-runner-airtable-schema-migration-v1.md` |
| Production schema create/alter | **Not applied** (requires authorization) |
| Live Airtable sync | **Blocked** unless `AVA_AUDIT_AIRTABLE_SYNC_ENABLED=1` + `AVA_AUDIT_AIRTABLE_SCHEMA_AUTHORIZED=1` |
| Local mirror sync | **Verified** |

---

## Test results

`npm run test:ava-permanent-audit-runner` → **PASS AVA_PERMANENT_AUDIT_RUNNER_V1**

| Case | Result |
|---|---|
| Single hotel dry-run | PASS |
| Three hotels dry-run (independent run IDs) | PASS |
| Full 38-hotel dry-run (no published artifact changes) | PASS |
| Measured zero classification | PASS |
| Intentional KPI mismatch blocks cert | PASS |
| Certify-only archive + second version preserves first PDF | PASS |
| Reproduce frozen audit (semantic content parity) | PASS (`pdfMatch=false` — nondeterministic renderer bytes; documented) |
| Historical compare API | PASS |
| Airtable mirror idempotency / no false ARCHIVE_COMPLETE | PASS |
| Historical ADP labeling / joint delivery blocked when not CERTIFIED_MATCH | PASS |
| PowerShell script + CLI help | PASS |

---

## Sample hotel run

**Hotel:** Cambridge Beaches (`adp_cambridge_beaches_bermuda`)  
**Mode:** `--certify-only`

- Cross-report: `CERTIFIED_HISTORICAL_REFERENCE` (AVA pin older than published/approved current period)
- Joint publication allowed: **false**
- Customer quality: certified
- PDF SHA-256 recorded; immutable archive under `data/ai-visibility-audit-action-plan/audit-runs/...`
- Admin draft registration: ok (`publishedUntouched: true`)
- Airtable: `LOCAL_MIRROR_ONLY` (not claimed as production persistence)

---

## Reproduction results

- Content SHA parity: **match**
- PDF byte parity: **not match** (Playwright/Chromium nondeterminism)
- Claim: semantic-content parity only; byte parity never asserted without matching hashes

---

## Airtable synchronization results

- Local mirror written with idempotency keys
- Live sync **not verified** (intentionally disabled; schema unauthorized)
- Failure path: archive retained; status `ARCHIVE_PENDING_AIRTABLE` / mirror `LOCAL_MIRROR_ONLY`

---

## AI Admin verification

- API: `GET .../permanent-runs`, `.../compare`, `.../permanent-runs/:auditRunId/pdf`
- UI: Current Audit + Permanent runner history + compare selection
- Download serves archived bytes + `X-AVA-PDF-Sha256`; refuses regenerate/substitute
- Local Admin registration exercised during certify-only sample (draft only)

---

## Known limitations

1. Many portfolio payloads currently dry-run as `CERTIFICATION_BLOCKED` / `RECONCILIATION_REQUIRED` after session recovery of ADP modules — KPI/period re-pin work from the prior ADP wave may need re-application before claiming 38/38 `CERTIFIED_MATCH`. The **runner** itself is intact and correctly blocks joint delivery.
2. Live Airtable indexing requires separate schema authorization.
3. Airtable `rec…` HotelId requires populating `ava-hotel-identity-map-v1.json`.
4. PDF byte-identical reproduction not guaranteed across Chromium runs.
5. Paid research remains off (`AVA_AUDIT_PAID_RESEARCH_AUTHORIZED` unset).
6. Playwright Chromium must be installed for PDF generation.

---

## Deployment requirements (not performed)

- No production deploy
- No customer publication / share-token rotation
- No GDI changes
- Founder approval still required before customer distribution
- Provision Airtable tables per migration plan before enabling live sync

---

## Remaining authorization gates

1. Airtable schema create + `AVA_AUDIT_AIRTABLE_SCHEMA_AUTHORIZED=1`
2. Live sync enable `AVA_AUDIT_AIRTABLE_SYNC_ENABLED=1`
3. Paid research `AVA_AUDIT_PAID_RESEARCH_AUTHORIZED=1` (only if needed)
4. Founder approval + separate customer publication authorization
5. Production deploy authorization
6. Optional: re-reconcile AVA payloads to current ADP periods for `CERTIFIED_MATCH` joint delivery

---

## Final status

**`AVA_PERMANENT_AUDIT_RUNNER_READY_FOR_REVIEW`**

Locally testable components passed. Production Airtable persistence and production readiness are **not** claimed.
