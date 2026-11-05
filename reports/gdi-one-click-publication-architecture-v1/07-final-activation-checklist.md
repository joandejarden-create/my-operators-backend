# Final activation checklist (STOP before merge / Railway / production publish)

## Code revision

- Branch: `feat/gdi-spice-published-inventory-registry-v1`
- Commit: _(filled after push)_
- PR: https://github.com/joandejarden-create/my-operators-backend/pull/136

## Production data source for Spice seven

Deploy-bundled certified pack (ships with app revision — not local staging):

`reports/gdi-production-publication-full-content-parity-v1/enriched-snapshots/{id}.json`

Resolver: `gdi-approved-publication-opportunity-source-v1`  
Falls back: supplied local preview → enriched pack → published registry snaps.

## Pre-merge gates

- [x] Scoped commit (no ADP / unrelated package inflation)
- [x] Three approved event titles preserved
- [x] Admin-only publish route (`requireAdminAccess`)
- [x] Share paths cannot publish
- [x] Verify-before-pointer (no rollback crash window)
- [x] Local backup/restore drill PASS
- [ ] CI green on **this** commit (not prior cc62f243)
- [ ] Founder GO for merge
- [ ] Founder GO for Railway deploy
- [ ] Founder GO for production volume backup write
- [ ] Founder GO for Spice one-click apply

## Rollback

Restore backup of `registry/current-registry-v1.json` + Spice `snapshots/` + `history/` per `06-production-backup-restore-procedure.md`.
