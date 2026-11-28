# Memmo Príncipe Real — Phase C Consolidated Founder Authorization

**Status:** `BLOCKED — AWAITING_FOUNDER_AUTHORIZATION_CHECKPOINT`  
**Date:** 2026-11-28  
**Hotel:** Memmo Príncipe Real · `rec1t35e3sPerzxYu`  
**Gates PR:** [#705](https://github.com/joandejarden-create/my-operators-backend/pull/705) **MERGED** · merge `2429e4ff71f3b89d55f56092b84156b1ace8d580`  
**Follow-up PR (ES auto-gen + material restage):** pending this tranche  
**Paid Live Full E2E:** **not started** — awaiting A–H below  

---

## One package — approve / edit / deny

| ID | Ask | Default proposal |
|---|---|---|
| **A** | Approve Memmo **competitive set** and authorize write to `config/group-demand-intelligence/hotels/rec1t35e3sPerzxYu.json` → `competitiveContext` | See `02-PROPOSED_COMPETITIVE_SET.md` (5 peers; canonical IDs TBD where not in census) |
| **B** | Confirm **remaining Exa-family USD** for Live discovery continuation + Live CAD + Live LDI + Deep HOLD continuation | Re-sum ledgers on GO; prior reports ~$2.1–$4.5 of ~$25 — **do not assume $22.90** until locked |
| **C** | Authorize **Findymail credits** under cumulative hard-cap | Prior: **16 / 15** (cap exceeded; defect fixed on main). Propose **hard-cap = 31** (= 16 spent + **+15 new**) or state exact new ceiling |
| **D** | Authorize **Live LDI** (`GDI_LDI_LIVE=1`) — not mock | Required for gold-standard LDI gate |
| **E** | Airtable **inventory membership schema** | Still PROPOSED (`15-AIRTABLE_SCHEMA_AUTHORIZATION_REQUEST.md`) **or** confirm payload-embed interim |
| **F** | Authorize **Airtable production writes** within ExactWriteSet | Separate from paid research |
| **G** | Authorize **material-improvement STAGED restage** (`-AuthorizeMaterialImprovementRestage`) even if net-new FQO = 0 | Required so recovered emails appear on STAGED |
| **H** | Authorize **new STAGED activation**; preserve `gdi_dh_who_how_rec1t35e3sPerzxYu_20261124` as rollback; **CURRENT frozen** | |

**Explicitly not requested:** Surfe auto · CURRENT publish · UI redesign · unlimited spend · artificial FQO quotas.

---

## Exact Live command (after A–H)

```powershell
cd C:\Dev\dealality\ws-gdi
$env:GDI_EXA_AGENT_LIVE = '1'
$env:GDI_LDI_LIVE = '1'
powershell -NoProfile -ExecutionPolicy Bypass -File scripts/gdi/Invoke-GdiFullE2E.ps1 `
  -HotelId rec1t35e3sPerzxYu -Mode Live `
  -AuthorizePaidResearch -AuthorizePaidDiscovery -AuthorizeFindymail `
  -EnableLeisure -EnableCad `
  -AuthorizeMaterialImprovementRestage `
  -AuthorizationId '<FOUNDER_GO_MEMMO_GOLD_STANDARD_…>' `
  -HardCeilingUsd <reconciled_remaining> -FindymailCreditHardCap <new_ceiling> `
  -Manifest 'reports/gdi-memmo-gold-standard-full-e2e-v1/auth/01-TRANCHE_MANIFEST.json' `
  -OutDir 'reports/gdi-memmo-gold-standard-full-e2e-v1/live' `
  -ActivateStagedPointer
```

Generic future hotel: same runners; change `-HotelId`, auth IDs, ceilings, `-OutDir` only.

---

## Already permanent on main (no further auth)

- Cumulative Findymail hard-cap across auth IDs / resume / recovery  
- CAD fail-closed without competitive set; classify-only ≠ Live CAD  
- Gold-standard certification gate module + regression test  
- Shared phone formatting V1 + dynamic contact enrichment V3  

---

## Return classification (this checkpoint)

```
PARTIAL — PHASE_C_AUTH_CHECKPOINT — PR705_MERGED_HOME_SYNCED
— COMPETITIVE_SET_AND_PAID_LIVE_AWAITING_FOUNDER_GO — NO_PAID_SPEND
```
