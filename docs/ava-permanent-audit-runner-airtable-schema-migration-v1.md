# AVA Permanent Audit Runner — Airtable schema migration plan (V1)

**Status:** PROPOSAL ONLY — not applied to production.  
**Read-only reconciliation (2026-11-06):** none of the six proposed AVA tables exist in product, sandbox, or alt bases (`reports/ava-permanent-audit-runner-v1/AIRTABLE_SCHEMA_RECONCILIATION_READONLY.json`). Existing `AI Visibility - *` tables are Brand/Operator monitoring, not AVA hotel audit history.  
**Authorization required** before creating tables, fields, or live sync (`AVA_AUDIT_AIRTABLE_SCHEMA_AUTHORIZED=1` + `AVA_AUDIT_AIRTABLE_SYNC_ENABLED=1`).

## Principles

- Reuse existing Hotel Master / property identity — **no duplicate hotel tables**.
- Airtable = searchable structured research index + historical analysis layer.
- Immutable JSON snapshots + PDFs live in durable local/repo storage (`data/ai-visibility-audit-action-plan/audit-runs/`); Airtable stores hashes + storage references.
- Do not overwrite historical rows on rerun.
- Do not store large PDF binaries in text fields.
- Attachment fields optional and non-authoritative (temp URLs).

## Proposed tables (existing base)

| Logical entity | Proposed table name | Link to Hotel |
|---|---|---|
| A. Audit Runs | `AVA Audit Runs` | Link → Hotel Master (or existing property link field) |
| B. Findings | `AVA Findings` | Link → Hotel + Audit Run |
| C. Recommended Actions | `AVA Recommended Actions` | Link → Hotel + Audit Run (+ Finding) |
| D. Historical Metrics | `AVA Historical Metrics` | Link → Hotel + Audit Run |
| E. Research Evidence | `AVA Research Evidence` | Link → Hotel + Audit Run |
| F. Report Versions | `AVA Report Versions` | Link → Hotel + Audit Run |

Populate `data/ai-visibility-audit-action-plan/ava-hotel-identity-map-v1.json` with `airtableRecordId` before using `-HotelId rec…`.

### A. AVA Audit Runs

| Field | Type | Notes |
|---|---|---|
| Audit Run ID | Single line text (primary) | `AVA-RUN-…` immutable |
| Hotel | Link | Existing hotel record |
| Audit period | Single line / select | ADP period id |
| Execution timestamp | Date (include time) | |
| Methodology version | Text | `AVA_FULL_E2E_PIPELINE_V1` |
| ADP package ID | Text | |
| ADP period ID | Text | |
| ADP fingerprint | Text | methodology fingerprint sha256 |
| Certification status | Single select | DRAFT / AUDIT_CERTIFIED / FAILED_CERTIFICATION / HISTORICAL_REFERENCE / SUPERSEDED |
| Execution status | Single select | PENDING / RUNNING / AUDIT_CERTIFIED / ARCHIVE_PENDING_AIRTABLE / ARCHIVE_COMPLETE / … |
| PDF version | Text | version id |
| PDF hash | Text | sha256 |
| Content hash | Text | sha256 |
| Previous audit | Link → AVA Audit Runs | |
| Storage reference | Long text | relative path under audit-runs |
| Current/latest indicator | Checkbox | cleared on newer run |
| Cross-report status | Single select | CERTIFIED_MATCH / CERTIFIED_HISTORICAL_REFERENCE / RECONCILIATION_REQUIRED / CERTIFICATION_BLOCKED |

### B–F

See `lib/ai-visibility-audit-action-plan/ava-audit-airtable-sync-v1.js` (`buildAirtableSyncBundle`) for the exact field map used by the sync layer.

**Measurement status** (metrics): `MEASURED_VALUE` | `MEASURED_ZERO` | `NOT_MEASURED` | `INSUFFICIENT_EVIDENCE` | `NOT_APPLICABLE` — never coerce missing → 0.

## Sync behavior (already implemented locally)

1. Always write local mirror under `data/…/airtable-history-mirror/{propertyId}/{auditRunId}/`.
2. Live Airtable upsert blocked unless both env flags are set **and** tables exist.
3. Idempotency keys: `run:`, `finding:`, `action:`, `metric:`, `report:`.
4. Airtable failure leaves local certified archive intact; status = `ARCHIVE_PENDING_AIRTABLE`.
5. Retry via `retryAirtableSync(auditRunId)`.

## Migration steps (when authorized)

1. Founder approves this plan.
2. Create tables + fields in the existing product base (not a parallel hotel DB).
3. Link Hotel field to the canonical hotel table.
4. Dry-run sync one hotel (`AVA_AUDIT_AIRTABLE_SYNC_ENABLED=0` mirror only → review bundle).
5. Set `AVA_AUDIT_AIRTABLE_SCHEMA_AUTHORIZED=1` and enable live sync for a pilot hotel.
6. Read-after-write verification; then enable portfolio sync.

## Not in scope without separate authorization

- Production schema create/alter
- Attachment backfill of historical PDFs
- Customer publication or share-token changes
