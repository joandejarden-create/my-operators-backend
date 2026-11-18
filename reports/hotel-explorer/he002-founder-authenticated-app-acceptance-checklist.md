# HE-002 — Founder authenticated `/app` acceptance checklist

**Contract:** `DEALALITY_RADAR_AUTHENTICATED_APP_ACCEPTANCE_GATE_V1`  
**Canonical URL:** `http://localhost:8080/app#/opportunity-radar`  
**PR:** #398  
**Workspace:** `C:\Dev\dealality\ws-hotel-explorer`  
**Branch:** `fix/radar-app-hpc-search-focus-v1`

Automated tests and standalone embeds **cannot** award **PASS**. Founder browser sign-in on HE `:8080` is required.

## Pre-check (already completed by HE chat)

- [x] PR reconciled with latest `origin/main` (includes authenticated acceptance gate from #399).
- [x] Workspace-principal policy `radar_app_workspace_principal_v1` revalidated (`ALLOW_RADAR_APP_WORKSPACE_PRINCIPAL` for Owner/Operator/Brand).
- [x] Missing-coordinate Open hotels are searchable / Hotel Explorer openable; map pins still require coords.
- [x] Radisson proposed coords staged only: `reports/hotel-explorer/he002-radisson-toronto-coords-evidence-staged.json` (**no Airtable write**).
- [x] Local Radar/HPC/auth/acceptance-contract gates green.

## Founder browser checklist (required for PASS)

Sign in on HE canonical checkout (`:8080`), open `/app#/opportunity-radar`, then verify:

1. [ ] Authenticated application access succeeds (no Sign-in wall; no `http_403` on HPC preview).
2. [ ] `GET /api/brand-presence/internal-preview-hotels` succeeds for the session (no auth errors).
3. [ ] Open/Pipeline HPC hotels enter the app dataset used by this route (7 map-eligible + Radisson searchable).
4. [ ] Map shows the 7 coordinate-ready Open hotels / clusters; Radisson has **no** map pin yet.
5. [ ] Search for `Radisson Blu Toronto` (or `recH9h3McgMNFQEla`) finds the hotel.
6. [ ] Selecting Radisson opens Hotel Explorer for `recH9h3McgMNFQEla` (address / identity correct).
7. [ ] Selecting a mapped Open hotel (e.g. Bethesda Marriott) focuses the map and opens the correct Explorer record.
8. [ ] Share / customer isolation: `share=1` does **not** show HPC merge notice or internal-only inventory.

## Evidence to record (no secrets)

| Evidence | Value |
| --- | --- |
| Git SHA | _(paste HEAD after sign-in verification)_ |
| Browser URL | `http://localhost:8080/app#/opportunity-radar` |
| Session type | founder / Owner / Operator (no tokens) |
| Expected vs observed Open | 8 status / 7 map / 1 missing-coords searchable |
| Screenshots | search hit + Explorer open + map without Radisson pin |
| Completion state | **PASS** / **FAIL** / **BLOCKED** |

## Merge gate

Only after this checklist is **PASS** and latest CI is green may the founder authorize Integration Controller merge of #398 + `dealality:post-merge-sync --stream he --pr 398`.
