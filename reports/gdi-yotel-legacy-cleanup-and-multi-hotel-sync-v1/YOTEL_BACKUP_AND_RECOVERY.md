# YOTEL backup and recovery

- Off-git unredacted backup: `C:\Dev\dealality\_secure-backups\gdi-yotel-legacy-gdo-deletion-v1\yotel-gdo-unredacted-backup-2026-11-18T09-14-35-959Z.json`
- Pointer: `C:\\Dev\\dealality\\_secure-backups\\gdi-yotel-legacy-gdo-deletion-v1\\LATEST_BACKUP_POINTER.json`
- Record count: 108
- ID fingerprint: `52eef5edd2a2f07f0ce5aaf8c5c33eee5c696c1631434d369d0d476173905a28`
- Git retains redacted audit backup under replacement package; unredacted copy is outside Git.

## Recovery
1. Load unredacted backup JSON.
2. Recreate each GDO with original `opportunityId` + fields.
3. Do not delete/recreate shared DG/Program/Signal/Venue records.
4. Read-back verify opportunityId set.
