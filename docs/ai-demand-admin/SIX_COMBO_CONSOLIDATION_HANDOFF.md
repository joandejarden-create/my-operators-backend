# AI Admin six-combo consolidation handoff

**Date:** 2026-10-17  
**Owner chat:** this conversation (primary execution owner)  
**Primary workspace:** `C:\Dev\dealality\my-operators-backend`

## Versions

| Surface | Value |
|---|---|
| Local `main` SHA | `82b30e4b` (`fix(admin): six-combo ADP/GDI Admin delivery (#127)`) |
| Local server | `:8080` from `my-operators-backend` (`DEALALITY_GIT_HEAD: 82b30e4b4098`) |
| Production deploy | `deploy:production` lean-stage → Railway; record `reports/deployments/2026-10-16T22-30-39-858Z-production.json` |
| PR | https://github.com/joandejarden-create/my-operators-backend/pull/127 (squash-merged) |

## Six-combination delivery status

Canonical IDs:

| Hotel | ADP id | HPC / GDI id |
|---|---|---|
| Renaissance New York Times Square | `adp_renaissance_times_square` | `recG66DQJKP2c0UNh` |
| Westin Tampa Waterside (**not** Tampa Airport Marriott) | `adp_westin_tampa_waterside` | `recdmo0CnhIZAwHGC` |
| Wyndham San Diego Bayside | `adp_wyndham_san_diego_bayside` | `recnPB3Q02NO8Ma1o` |

Evidence (redacted share URLs in Git): `reports/admin-six-combo-delivery/`  
Post-merge API verify copy (aside): `C:\Dev\dealality\_aside-six-combo-consolidate-20261017-002537\07-authenticated-admin-clicks.json`

| Hotel | Product | Admin status (local catalog) | Client link | PDF | Notes |
|---|---|---|---|---|---|
| Renaissance NYTS | ADP | READY (current-report PDF) | Verified | Verified | Monthly-review row also exists; distinct from current-report PDF |
| Renaissance NYTS | GDI | READY | Verified (44 facing) | Verified | |
| Westin Tampa Waterside | ADP | READY (current-report PDF) | Verified | Verified | **Not** in monthly-reviews → `review_artifact_missing` for monthly path is accurate; do not force READY |
| Westin Tampa Waterside | GDI | BLOCKED / unavailable | Share opens; **0 facing** | PDF present | Accurate zero-facing; no publication reclassification |
| Wyndham SD Bayside | ADP | READY (current-report PDF) | Verified | Verified | Same monthly-review note as Westin ADP |
| Wyndham SD Bayside | GDI | READY (local filesystem mirror) | External share Verified (**51 facing**) | Verified | Airtable mirror: 82 opportunities, 51 facing, customer-ready 5, future-watch 11. Facing prospects are not reclassified as customer-ready. |

### Verification scope honesty

- **Verified:** authenticated Admin **API** paths used by Open/Copy (`/api/admin/ai-demand/hotels/:id/external-client-links`, proxies to production) and View/Download PDF (`current-report-pdf` / `report-pdf`); external client pages/APIs without Admin session.
- **Not verified as UI clicks:** Playwright Admin shell remained **Access restricted** without a real Memberstack JWT — do not treat Action Plan dropdown clicks as verified.
- Westin/Wyndham are **not** in ADP Action Plan property catalog in the UI smoke; delivery uses current-report PDF + external links APIs.

## Authoritative ledgers

| Ledger | Path | Figures |
|---|---|---|
| Renaissance GDI | `data/group-demand-intelligence/renaissance-gdi-budget/authorization-ledger-v1.json` | **$4.31 incurred / $7.69 available** (budget $12, 13 entries) |
| Mayflower/SD shared | `data/group-demand-intelligence/paid-multisegment-budget/authorization-ledger-v1.json` | **$5.25 incurred / $1.25 hold / $4.50 available** (`reconciliation.safelyAvailableUsd`) |
| Renaissance ADP | separate from GDI ledgers | unchanged |

Do **not** restore older Mayflower/SD summaries ($3.85 / $7.15) from other worktrees.

## Backups / aside (not deleted)

- `C:\Dev\dealality\_aside-six-combo-consolidate-20261017-002537` — pre-integration dirty snapshot  
- `C:\Dev\dealality\_aside-six-combo-untracked-opps-20261017` — held Westin/Wyndham snapshots. Wyndham publication fields matched live Airtable (0 mismatches) and the mirror was rewritten from that read. Westin snapshot stays unmerged (0 facing).

## Held worktrees (leave inactive)

| Path | Why held |
|---|---|
| `gdi-release-main/.worktrees/main-deploy` | Former six-combo branch worktree; supersedes by #127 |
| `gdi-ren-e2e-closeout`, `gdi-renaissance-exec` | Ren closeout archives; dirty research noise |
| `wyndham-sd-adp-e2e` | Wyndham ADP e2e; minor dirty |
| `tampa-airport-adp-gdi` | **Deferred** Tampa Airport activation — preserve instructions; do not run |
| `gdi-main-consolidate`, `xps-*`, `october-recovery-test`, `adp-share-unknown-fix`, etc. | Unrelated |

## Completed after this handoff

- **Local GDI Admin catalog parity** for Wyndham — filesystem mirror of the published Airtable set (`ready=5`, 51 facing, status READY). No Airtable write. Westin remains unmirrored. Gate: `npm run test:admin-wyndham-gdi-catalog-parity`.

## Deferred tasks

1. **Tampa Airport Marriott** ADP/GDI activation — deferred; do not execute from this chat.  
2. **Memberstack-authenticated Admin UI click** pass for Open/Copy/View/Download on all six (shell Access restricted under local bypass alone).  
3. Optional: add Westin/Wyndham to ADP Action Plan catalog if product requires that tab (current-report PDF path already works).

## Operating rule going forward

- One primary workspace: `my-operators-backend`  
- Short-lived branches → PR → merge → return to clean `main`  
- Deploy only via `npm run deploy:production` from clean `main`  
- Do not create another execution worktree unless there is a concrete, explained need  

## Post-deploy production mapping (verified)

After Railway finished deploying `82b30e4b`, internal resolver returned **both** ADP and GDI links for all six hotel keys (ADP ids and HPC ids). Bidirectional Admin Open/Copy mapping is live on production.

Note: `fixtures/ai-demand-positioning/census-links-v1.json` remains railwayignored; production mapping currently relies on `config/group-demand-intelligence/hotels/*.json` aliases + shipped resolver. Prefer un-ignoring the single census-links fixture in a follow-up if census becomes the sole SoT.
