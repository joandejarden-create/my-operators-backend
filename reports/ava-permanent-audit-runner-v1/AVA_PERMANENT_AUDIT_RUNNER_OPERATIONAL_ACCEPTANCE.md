# AVA Permanent Audit Runner — Operational Acceptance

**Status token:** `AVA_PERMANENT_AUDIT_RUNNER_OPERATIONAL_ACCEPTANCE_PASS`  
**Overall gate:** `OPERATIONAL_ACCEPTANCE_PASS`  
**Date:** 2026-11-06  
**Worktree:** `C:\Dev\dealality\my-operators-backend-ava-v4-quality`  
**Branch:** `fix/ava-v4-customer-quality-remediation-v1`  
**Command:** `npm run ava:operational-acceptance`  
**Manifest:** `reports/ava-permanent-audit-runner-v1/OPERATIONAL_ACCEPTANCE_MANIFEST.json`  
**Airtable integration:** `reports/ava-permanent-audit-runner-v1/AVA_EXISTING_ADP_AIRTABLE_INTEGRATION_REPORT.md`

---

## Summary table

| Gate | Result | Evidence |
|------|--------|----------|
| Full audit execution | **PASS** | Spice / Cambridge / AC Coruña certify-only runs archived with distinct run IDs |
| ADP cross-certification | **PASS** | Spice `CERTIFIED_MATCH`; Cambridge `CERTIFIED_HISTORICAL_REFERENCE` (joint delivery blocked) |
| PDF quality | **PASS** | CQA certified; V4 PDF generated; SHA-256 recorded |
| Immutable archive | **PASS** | `data/.../audit-runs/{propertyId}/{auditRunId}/` retains v1 after v2; hashes verify |
| Historical reproduction | **PASS** | `BYTE_IDENTICAL_REPRODUCTION` on stamped archives |
| Airtable persistence | **PASS** | Live write/read/idempotency on existing ADP base `appa2cE7FTRmIbB32` |
| Admin version history | **PASS** | Permanent-runs / compare / PDF; `airtableSync` status on hotel payload |
| Historical comparison | **PASS** | Natural AC compare + controlled fixture detects new finding |
| Regression testing | **PASS** | Includes EXISTING_ADP_TABLES_V1 mirror + failure-path cases |

---

## Airtable persistence (cleared blocker)

Previously CONDITIONAL because six proposed AVA tables did not exist. Resolved by reusing existing ADP tables:

| AVA entity | Existing table |
|---|---|
| Audit run / report version | `ADP Report Snapshots` (`Snapshot ID` = `AVA-RUN-…`) |
| Period / execution index | `ADP Monitoring Periods` |
| Metrics | `ADP Period Metrics` |
| Findings / actions / evidence | `Hotel Intelligence Evidence` (`Entity Type=Other`) |

Live cert: `AIRTABLE_PERSISTENCE_PASS` (`ADP_AIRTABLE_LIVE_PERSISTENCE_CERT.json`).  
No writes to `AI Demand Positioning - Published Reports`. No new base. No hotel-master duplication.

---

## Architecture verified

1. **Research / Full E2E:** `runMandatoryAvaEvidenceStages` (`ava-full-e2e-pipeline-v1.js`)
2. **PDF:** `generateAiVisibilityAuditPdfV1` + `buildAiVisibilityAuditPdfHtmlV4`
3. **ADP↔AVA cert:** `certifyAvaAgainstCustomerFacingAdp`
4. **PowerShell:** `scripts/Run-AVA-Audit.ps1` → `scripts/run-ava-permanent-audit-v1.mjs`
5. **Immutability:** `saveImmutableAuditRun` refuses overwrite
6. **Airtable index:** `ava-audit-airtable-sync-v1.js` → existing ADP tables
7. **Published isolation:** draft-only Admin registration (`publishedUntouched: true`)

---

## Final status

```
AVA_PERMANENT_AUDIT_RUNNER_OPERATIONAL_ACCEPTANCE_PASS
```
