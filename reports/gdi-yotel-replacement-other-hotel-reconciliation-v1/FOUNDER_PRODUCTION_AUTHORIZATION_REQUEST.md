# Founder production authorization request — YOTEL replacement

This preparation phase did **not** delete or write Airtable records.

## Requested authorization (when ready)
1. Delete exactly the Airtable record IDs in `YOTEL_108_RECORD_DELETE_MANIFEST.json` → `airtableRecordIdsExact`.
2. Create exactly the qualified GDO set (0 IDs) from `YOTEL_42_RECORD_TARGET_STATE.json`.
3. Leave research-only local rows on filesystem only.
4. Do not touch other hotels, shared intelligence, or publication packages.
5. Rock Creek correction remains a separate allowlist item.

## Required flags (future apply runner)
- `authorizeProductionWrites=true`
- `exactWriteSetAuthorization=<hash-bound manifest token>`
- `apply=true`
- hotel allowlist: `recrPQcZg7SFARRb2` only

**STOP — await founder signature on this request before any production mutation.**
