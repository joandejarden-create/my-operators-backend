# CHECKPOINT A CLOSEOUT — Wyndham Final Commercial Continuation

**Status:** `CHECKPOINT_A_COMPLETE — AWAITING_FOUNDER_REVIEW`  
**At:** 2026-11-27  
**Authorization:** `FOUNDER_GO_WYNDHAM_FINAL_COMMERCIAL_CONTINUATION_18USD_20261127`  
**Hotel:** `recnPB3Q02NO8Ma1o` Wyndham San Diego Bayside  

---

## Exact executable command (recorded)

```powershell
cd C:\Dev\dealality\ws-gdi
$env:GDI_EXA_AGENT_LIVE = '1'
.\scripts\gdi\Invoke-GdiWhoHow.ps1 `
  -HotelId recnPB3Q02NO8Ma1o `
  -Mode Live `
  -AuthorizePaidResearch `
  -AuthorizationId 'FOUNDER_GO_WYNDHAM_FINAL_COMMERCIAL_CONTINUATION_18USD_20261127' `
  -HardCeilingUsd 5 `
  -Manifest 'reports/gdi-wyndham-who-how-v3-tranche-20261127/01-TRANCHE_MANIFEST.json' `
  -ContinuationOnly `
  -Resume
```

Path: `Invoke-GdiWhoHow.ps1` → `run-gdi-who-how-checkpoint-a-v1.mjs` → `gdi-exa-who-how-live-adapter-v1.js`

---

## Spend

| Item | USD |
|---|---:|
| Wave 1 (structure-defect outputs) | 2.50 |
| Wave 2 (structured remediation, contract v1.1) | 2.50 |
| **Checkpoint A actual** | **5.00** |
| Checkpoint A ceiling | 5.00 |
| Aggregate ceiling | 18.00 |
| **Remaining aggregate** | **13.00** |
| Carryover prior unused | **0** (NO_CARRYOVER) |

Budget exhaustion of Checkpoint A → PARTIAL on remaining Checkpoint A capacity; aggregate still open for Checkpoint B **only after founder acceptance**.

---

## Aggregate scorecard (Wave 2 authoritative)

| Metric | Value |
|---|---:|
| Opportunities attempted | 25 |
| Successfully researched | 25 |
| Direct buyers recovered | 8–9 |
| Buyer influencers recovered | 17 |
| Two verified alternative entry points | 15–17 |
| One verified alternative entry point | 1 |
| No credible named contact | 0 |
| Meaningful WHO/HOW recovery | **25 / 25** |
| Minimum target (≥10/25) | **MET** |
| Person-specific reachability (FQO channel) | 0 (Stage 4 still required) |
| Incremental FQO vs locked 26 | **0** (gates not relaxed) |
| Potential FQO pending Stage 4 / dossier reattach | 25 WHO/HOW-ready candidates |
| Cost / recovered opportunity | $0.20 |
| Cost / incremental FQO | n/a (0 incremental FQO) |
| Settled-job reuse (Wave 2) | New v1.1 keys; Wave 1 broken structures not reused |

Statuses (Wave 2): `VERIFIED_DIRECT_BUYER` 8 · `VERIFIED_BUYER_INFLUENCER` 2 · `VERIFIED_ENTRY_POINTS_2` 15

Full row-level before/after: `16-CHECKPOINT_A_SCORECARD.json`

---

## Operational defect (Wave 1) and fix

Wave 1 returned LinkedIn/named people in `evidence` while leaving structured `entryPoints` as `NOT_FOUND` (0/25 structured recovery).

Genuine defect fixed in-adapter: richer output schema + mandatory structure rule; prompt contract bumped to `…-v1.1` so remediation jobs did not reuse broken settled structures. Remained inside Checkpoint A $5 ceiling.

---

## Browser validation

- Preferred `:8080` process is **stale** (does not serve current `ws-gdi` writes).
- Non-publishing preview served at `http://127.0.0.1:8767/` from `public/internal/gdi-checkpoint-a-who-how-preview.html`.
- Verified on-page: Primary Lodging Decision-Maker, Influencer, Alternative Entry Points, why/approach, identity vs authority, Surfe explicit-click only, NO_STAGED/NO_CURRENT.

---

## Governance held

NO_AIRTABLE_APPLY · NO_STAGED · NO_CURRENT · NO_FINDYMAIL · NO_SURFE_AUTO · NO_CARRYOVER · NO_REDISCOVERY_161 · Bethesda ON · CLEAN11 ON · Locked 26 **unchanged / not applied**

---

## Checkpoint B recommendation

**Recommend founder acceptance of Checkpoint A**, then authorize Checkpoint B within remaining **USD 13.00**:

1. PLP recovery (evidence-gated rewritten queries)  
2. Competitive/overflow CAD on 15 association/event HOLDs  
3. Six applicable live leisure modules  
4. Deduped deep on genuinely new leisure only  
5. Public-source Stage 4 contact DryRun on WHO/HOW recoveries  
6. Strict FQO reassessment vs locked 26  

Do **not** auto-start Checkpoint B.

---

## PR note

Authorized-path + structure remediation commits live on branch `feat/gdi-who-how-checkpoint-a-authorized-path` / PR #615 (IC merge for home sync). Execution used that canonical path under founder GO.
