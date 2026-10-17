# Release status

## PR #133 (merged)

- PR: https://github.com/joandejarden-create/my-operators-backend/pull/133
- Merge commit: `b34dea7910c7cba1b1b4a403f5c85bf5300ba016` on `main`
- Review acceptance: sealed production client URLs without local signing secret; null ≠ zero client counts; sealed-token identity/scope/expiry/revocation via `assessSealedShareToken`; Airtable client inventory separate from Ready/Action Set/Watch summary metrics
- First lean deploy after merge recorded under `reports/deployments/2026-10-17T08-00-08-311Z-production.json`

## Follow-up: presentation + sealed local ingest

- Branch: `fix/admin-gdi-client-inventory-presentation`
- Compact zero label (`No published opportunities`); link-unavailable reasons on Actions only
- Shared Admin tab strip `position: static` (stops sticky float over table in app shell)
- Disabled Open/Copy remain visible when sealed URL absent
- `scripts/ingest-production-share-sealed-local.mjs` hydrates gitignored sealed catalog from authorized delivery URLs (no secret copy, no bearer commit)
- Westin + Wyndham GDI sealed bearers ingested locally from `reports/admin-six-combo-delivery/02{a,b}-issue-*-railway.log` (UTF-16); not rotated

## Coordination

- Nobu remains on worktree `exec/nobu-london-portman-e2e` — not switched for this Admin release
- Authenticated six-row UI clicks still require Memberstack in the Cursor browser after production confirms the new assets
