# YOTEL full backup and recovery plan

- Hotel: `recrPQcZg7SFARRb2`
- Backed-up GDO rows: **108**
- Artifact: `YOTEL_GDO_BACKUP_REDACTED.json` (PII redacted in git)
- Fingerprints: per-record sha256 over redacted field set
- Local baseline snapshot: `YOTEL_LOCAL_BASELINE_SNAPSHOT.json` (42 ids)

## Restoration feasibility
1. Re-create each GDO from backup `fields` via Airtable create (or API upsert by opportunityId).
2. Re-attach linked record IDs only if those shared records still exist (DG/Program/Signal are never deleted by this migration).
3. Re-apply `decisionId` / `actionStatus` from backup when present.
4. Verify read-back count and opportunityId set equal backup.
5. Filesystem `opportunities.json` is independent and remains available even if Airtable restore is needed.

## Deletion prerequisite
Do **not** delete until founder authorizes the exact `airtableRecordIdsExact` list and an unredacted local recovery copy exists on founder-controlled media if contact fields must be preserved bit-for-bit.
