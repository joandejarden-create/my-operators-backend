# Airtable UPDATE Transaction Design (not executed)

1. Read current Airtable row
2. Compare retrieved_at / version / content hash
3. Detect concurrent changes → REVIEW if conflict
4. Calculate field diff
5. Validate source rights per field
6. Validate property identity Exact/High
7. Write only changed eligible fields (Class A/B)
8. Preserve prior evidence/history (temporal facts)
9. Log transaction id + actor + sources
10. Support rollback from transaction log

Never overwrite a stronger source with a weaker source merely because it is newer.
