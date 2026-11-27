# Complete Competitive Demand Intelligence & Market Scope UI

**Status:** `GDI_COMPETITIVE_DEMAND_INTELLIGENCE_PARTIAL — AWAITING_FOUNDER_REVIEW`  
**Date:** 2026-11-27  
**Port:** `http://127.0.0.1:8080` from `ws-gdi`  
**PR:** #626 (cumulative) · Checkpoint B PR #620 remains **OPEN**

## 1. Exact reason Competitive previously displayed zero

**Measured STAGED zero — not a Market Scope filter false negative.**

Prior tip `…_c456c74e` published `competitiveMarketScopeEligible=false` for all 26 rows (empty `inventoryPopulations`). The UI correctly showed Competitive (0).

Classification of that zero:

| State | Applies? |
|---|---|
| Research not executed | **No** — partial historical CAD existed |
| Research executed with no discoveries | **No** — 15 CAD `FQO_CANDIDATE` rows with named competitors |
| Competitive candidates discovered but not qualified | **Yes** for 13 of 15 CAD candidates (outside locked 26) |
| Qualified competitive opportunities missing from STAGED | **Yes** — 2 CAD FQO_CANDIDATE ids were **in** locked 26 but classification was omitted |
| Incorrect Market Scope mapping | **Yes (mapping omission)** for those 2 + ACG CapCon QA host evidence |

## 2. Existing competitive intelligence recovered ($0)

Recovered into Wyndham STAGED via rematerialize (no paid research):

| Opportunity ID | Title | Competitor | Source |
|---|---|---|---|
| `gdi_opp_prospect_mv8p5ddh_7edb8e7c` | 2027 AANS Annual Scientific Meeting | Marriott Marquis San Diego Marina | CAD competitive log FQO_CANDIDATE |
| `gdi_opp_prospect_mv8p5ddj_cf54ecae` | WEST 2027 Participant Housing | Marriott Marquis San Diego Marina | CAD competitive log FQO_CANDIDATE |
| `gdi_wyndham_live_associatio_a790d46ad631` | ACG San Diego – Regional Meetings & Events | Marriott Marquis / Hilton Bayfront | QA competitor-host evidence (CapCon) |

**Resulting STAGED:** `gdi_hotel_wide_fqo_recnPB3Q02NO8Ma1o_20261127_84288a6e`  
**Counts:** All 26 · Hotel 26 · Competitive **3** (overlapping populations)  
**CURRENT:** unchanged (null)

## 3. Not recovered (honest exclusions)

- **13** CAD `FQO_CANDIDATE` rows with named competitors remain **outside** locked 26 — Layer-1 CAD ≠ FQO PASS ≠ auto-STAGED. Require Checkpoint B CAD completion + six-gate qualification before inventory expansion.
- Overflow findings (`05-COMPETITIVE_OVERFLOW_FINDINGS.json`): COMPETITIVE_DISPLACEMENT = 0 / OVERFLOW = 0 in queue deep dossiers — agent outputs rarely named hosts; CAD refresh not run on HOLD association set.
- Historical competitor business alone does not establish a new qualified opportunity beyond the three recovered rows.

## 4. Checkpoint B / authorization reconciliation

| Item | Status |
|---|---|
| PR #620 | **OPEN** at `f050f731f…` — paid PLP/CAD/leisure **not executed** in that PR |
| Authorization | `FOUNDER_GO_WYNDHAM_FINAL_COMMERCIAL_CONTINUATION_18USD_20261127` |
| Aggregate ceiling | USD **18.00** |
| Checkpoint A spend | USD **5.00** (scorecard) |
| Remaining | USD **13.00** |
| This instruction | **No new budget · no paid Checkpoint B spend** |
| Blocker | IC must merge #620 + post-merge sync + $0 preflight PASS before paid CAD/PLP/leisure sequence |

GDI chat does **not** merge. Paid competitive refresh on the previously identified 15 association/event HOLDs awaits Integration Controller.

## 5. Market Scope UI

Visible labels: **All (n) · Hotel (n) · Competitive (n)**  
Competitive column wider (unequal grid). Fixed control widths — Opportunity Type does not shift. Full aria/title retain “Competitive Market Intelligence”.

## 6. Permanent Competitive Research Completion Gate

Module: `lib/group-demand-intelligence/workflow/gdi-competitive-research-completion-gate-v1.js`  
Wired into GDI Research Intelligence (admin). Distinguishes NOT_EXECUTED vs EXECUTED_ZERO_QUALIFIED vs CANDIDATES_DISCOVERED_NOT_QUALIFIED vs QUALIFIED_MISSING_FROM_STAGED. Zero qualified is acceptable only when research executed.

## 7. Validation commands

```powershell
cd C:\Dev\dealality\ws-gdi
npm run test:gdi-competitive-market-scope-completion-v1
npm run test:gdi-executive-summary-v2
npm run test:gdi-es-admin-shared-tiles-v1
node scripts/dealality-gdi-local-8080-stable-launch-v1.mjs
# Then: http://127.0.0.1:8080/group-demand-intelligence.html?hotelId=recnPB3Q02NO8Ma1o&preview=staged
```

## 8. Governance

FULL E2E → STAGED → FOUNDER APPROVAL → CURRENT  
No CURRENT · no Surfe auto · no production · no new budget · DEALALITY_ADDITIVE_NON_REGRESSION_V1
