# AVA ↔ Existing ADP Airtable Integration Report

**Date:** 2026-11-06  
**Final status:** `AVA_PERMANENT_AUDIT_RUNNER_OPERATIONAL_ACCEPTANCE_PASS`  
**Live persistence cert:** `AIRTABLE_PERSISTENCE_PASS`  
**Artifact:** `ADP_AIRTABLE_LIVE_PERSISTENCE_CERT.json`

---

## 1. Existing Airtable base identified

| Item | Value |
|---|---|
| Canonical ADP / intelligence base | `appa2cE7FTRmIbB32` |
| Base name | AI Demand Positioning (governed) |
| Resolution | `resolveAdpCanonicalBaseId` / `AIRTABLE_INTELLIGENCE_BASE_ID` |
| Credentials | Existing `AIRTABLE_API_KEY` / `AIRTABLE_PAT` (not exposed) |
| Read access | Verified against `ADP Report Snapshots` |

**Not used for AVA history:** product Deal Capture base `appvtnDurnMSjINP6` (legacy MVP — forbidden for ADP writes).

---

## 2. Existing ADP tables inspected

Governed base tables relevant to AVA indexing (35 total; subset):

- `AI Demand Positioning - Published Reports` — **Live ADP overlay only — FORBIDDEN for AVA writes**
- `ADP Monitoring Periods`
- `ADP Report Snapshots`
- `ADP Period Metrics`
- `ADP Territory Metrics` / `ADP Provider Metrics` / `ADP Competitive Rankings`
- `ADP Evidence Index` / `ADP Report Corrections` / `ADP Observation Prompt Ledger`
- `Hotel ADP Attributes` / `Hotel Commercial Profiles` / `Hotel Intelligence Evidence`

Field discovery: `ADP_AIRTABLE_TABLE_FIELD_DISCOVERY.json`

---

## 3. Reused tables and fields

| AVA logical entity | Existing table | Isolation strategy |
|---|---|---|
| Audit run + report version | `ADP Report Snapshots` | Primary `Snapshot ID` = `AVA-RUN-…`; `Schema Version` = `AVA_AUDIT_EXECUTION_CONTRACT_V1`; `Report Edition` = `AVA_AUDIT_V1` |
| Execution / period index | `ADP Monitoring Periods` | `Period Key` = `ava\|{propertyId}\|{auditRunId}` |
| ADP-derived KPI refs | `ADP Period Metrics` | `Metrics Key` = `ava\|{auditRunId}\|{METRIC}`; family select left blank (CORE/BPP options only) |
| Findings / actions / evidence | `Hotel Intelligence Evidence` | `Evidence ID` = `ava\|{run}\|finding\|…` / `action\|…`; `Entity Type` = `Other` |

**Hotel identity:** `Property ID` = existing `adp_*` property id (same ADP identity). No duplicate hotel master.

**Durable artifacts:** remain under `data/ai-visibility-audit-action-plan/audit-runs/…`; Airtable stores `Snapshot Store Ref` + content/PDF hashes.

---

## 4. Required new fields/tables

**None created.**  

Optional future enhancements (not required for PASS):

- Add select option `AVA_AUDIT` to `Measurement Family` on Snapshots/Metrics for cleaner filtering (currently isolated by key prefix + Schema Version).
- Dedicated AVA Findings / Actions tables if query volume outgrows Evidence index.

These are **not** blockers; existing fields accommodate persistence.

---

## 5. Credential / access verification

| Check | Result |
|---|---|
| API key present | Yes |
| Canonical base reachable | Yes |
| Snapshot table read | Yes |
| Live write (additive AVA keys) | Yes |
| Forbidden Published Reports write | Enforced in sync (refused) |

---

## 6. Data model mapping

Implemented in `ava-audit-airtable-sync-v1.js` (`mappingMode: EXISTING_ADP_TABLES_V1`).

Persisted per run:

- Audit identity (run id, property, ADP period/package/fingerprint, cert status, hashes, store ref)
- Metrics (Consideration / Scenario Presence values when present + Disclosures JSON)
- Findings & actions (via Hotel Intelligence Evidence rows)
- Report version index (same snapshot row; previous version via archive `previousVersionRef`)

---

## 7. Live synchronization results

Controlled write for latest local archives (cert `2026-11-06T10:15:31Z`):

| Hotel | Audit Run | Indexed | Snapshot record |
|---|---|---|---|
| Spice Island | `AVA-RUN-…ed1b30dc` | Yes | `reckWlKh4YseDQ174` |
| Cambridge Beaches | (acceptance archive) | Yes | created/patched |
| AC Coruña | (acceptance archive) | Yes | created/patched |

No ADP Live Published Reports rows modified. No CORE/BPP primary keys overwritten.

---

## 8. Idempotency test results

Spice Island: sync twice → **single** Snapshot record; second pass **PATCH**.  
Live cert `idempotency.ok` → **true**.

---

## 9. Historical report verification

- Local immutable archives retained for all versions
- Admin permanent PDF path continues to serve archived bytes + SHA-256
- Airtable indexes point at archive store refs (not attachment URLs)
- New audit creates new `AVA-RUN-…` Snapshot ID — does not replace prior run keys

---

## 10. AI Admin verification

- `GET .../:propertyId` now includes `permanentAuditRuns[].airtableSync` (`status`, `airtableIndexed`)
- History/compare/PDF download remain archive-first (works if Airtable unavailable)
- Sync status clearly indicates indexing state vs durable archive

Live browser against stale :8080 process still requires server restart from this worktree for route pickup; in-process Admin API verification previously passed.

---

## 11. Remaining blockers

None for operational Airtable persistence PASS.

Non-blocking follow-ups:

1. Restart Admin server from this worktree for browser E2E
2. Optional `Measurement Family=AVA_AUDIT` select option (schema enhancement)
3. Populate `ava-hotel-identity-map-v1.json` census/Airtable hotel rec links when needed for `rec…` CLI IDs
4. Enable `AVA_AUDIT_AIRTABLE_SYNC_ENABLED=1` in operator env for routine runs

---

## 12. Final operational status

```
AVA_PERMANENT_AUDIT_RUNNER_OPERATIONAL_ACCEPTANCE_PASS
```

Gate command: `npm run ava:operational-acceptance`  
Live cert: `npm run ava:adp-airtable-live-persistence-cert`
