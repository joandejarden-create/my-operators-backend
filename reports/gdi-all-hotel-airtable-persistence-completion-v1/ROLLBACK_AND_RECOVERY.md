# Rollback and recovery

- Research inventory creates (future): delete by `opportunityId` allowlist or restore from FS `opportunities.json` SoT
- Qualified GDO creates: delete only exact new record IDs from apply ledger; never broad hotel wipe
- YOTEL: off-git backup of deleted 108 remains under `C:\Dev\dealality\_secure-backups\` — do not recreate
- GDO updates: reverse from pre-write field snapshots in apply ledger
- Publication packages: never mutated by this persistence path
- Schema: if table created then abandoned, leave empty or archive; do not migrate research into GDO as cleanup
