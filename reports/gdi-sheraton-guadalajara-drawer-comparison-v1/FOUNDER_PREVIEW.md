# Sheraton Guadalajara Expo — GDI drawer side-by-side preview

## Integration gate

| Item | Status |
|------|--------|
| PR #462 | **OPEN** — https://github.com/joandejarden-create/my-operators-backend/pull/462 |
| Competing implementation PR | **NOT OPENED** |
| Formal merge / `ws/gdi` home sync | **AWAITING_INTEGRATION_CONTROLLER** |

Local comparison files are ready on the #462 tip for founder visual review only. They are **not** pushed as a competing PR.

## Exact comparison URL

```
http://127.0.0.1:8095/group-demand-intelligence-drawer-comparison.html?hotelId=recCAGqkERSXgiku4
```

**Not** the ordinary staged GDI page:
`/group-demand-intelligence.html?hotelId=recCAGqkERSXgiku4&preview=staged`

## Server / port

| Item | Value |
|------|--------|
| Workspace | `C:\Dev\dealality\ws-gdi` (`feat/gdi-commercial-fact-selection-v1` tip) |
| Port | **8095** |
| Why not 8080 | Canonical `:8080` is reserved for `my-operators-backend`; that checkout does **not** include #462 Fact Selection |
| Env | Primary `.env` loaded for Airtable; `DEALALITY_ALLOW_WORKTREE_SERVER=1` |

Restart (PowerShell):

```powershell
cd C:\Dev\dealality\ws-gdi
$env:PORT='8095'
$env:DEALALITY_ALLOW_WORKTREE_SERVER='1'
node --input-type=module -e "import dotenv from 'dotenv'; dotenv.config({ path: 'C:/Dev/dealality/my-operators-backend/.env', override: false }); await import('./server.js');"
```

## Inventory coverage (reconciled)

| Source | Count |
|--------|------:|
| Union deduplicated | **79** |
| Current STAGED package (`…_final_certification_v1`) | **38** |
| Local research store | **43** |
| Historical full E2E package (62-era) | **62** |

Status classification is in `INVENTORY_RECONCILIATION.json`. STAGED is **not** treated as the sole inventory without reconciliation.

## Browser smoke (authenticated-app style founder page)

| Check | Result |
|-------|--------|
| Comparison HTML HTTP | **200** |
| Visible LEFT — BEFORE | **PASS** |
| Visible RIGHT — AFTER | **PASS** |
| Both panels simultaneous | **PASS** |
| Real Sheraton opportunities | **PASS** (79 selector) |
| Highlight Changes | **PASS** (amber revised sections) |
| Sync scroll control | present |
| Screenshot | `screenshots/01-both-panels.png` |

## Constraints

- No production Airtable writes
- No paid research
- No publication overwrite / deploy
- Customer share route unchanged

## STOP

Founder visual review required before any PR ship of this comparison surface.
