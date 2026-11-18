# Migration execution sequence

Airtable has **no** atomic multi-record transactions. Use staged recoverable phases:

### Phase 1 — FREEZE_AND_BACKUP
- Writes: `false`
  - Re-query YOTEL GDOs by exact hotelId
  - Write full recoverable backup + fingerprints
  - Snapshot local opportunities.json
  - Audit Decisions/feedback/publication pointers
### Phase 2 — FOUNDER_AUTHORIZATION
- Writes: `false`
  - Review exact delete ID list
  - Review qualified GDO replacement set
  - Authorize with exactWriteSetAuthorization token bound to manifests
### Phase 3 — CREATE_QUALIFIED_REPLACEMENTS_FIRST
- Writes: `AUTHORIZED_CREATES_ONLY`
  - Create only 0 qualified GDO rows from local baseline
  - Read-back verify each create
  - Do not create research-only rows into GDO
### Phase 4 — DELETE_OBSOLETE_108
- Writes: `AUTHORIZED_DELETES_ONLY`
  - Delete only airtableRecordIdsExact from delete manifest
  - Re-verify hotelId on each record immediately before delete
  - Never delete shared DG/Program/Signal/Venue
  - Skip any ID not in allowlist
### Phase 5 — POST_MIGRATION_RECONCILE
- Writes: `false`
  - Re-list YOTEL GDOs
  - Expect GDO count == qualified creates (not necessarily 42)
  - Confirm local 42 still present on filesystem
  - Confirm publication packages unchanged
  - Idempotent replan: no further deletes/creates

Proposed deletes: **108**
Qualified creates: **0**
Research-only remain on filesystem: **42**
