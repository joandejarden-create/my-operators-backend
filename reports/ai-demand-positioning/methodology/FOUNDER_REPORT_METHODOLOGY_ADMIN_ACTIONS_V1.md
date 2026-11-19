# FOUNDER REPORT — ADP Methodology Consistency + AI Admin Actions

**Date:** 2026-11-19  
**Workspace:** `C:\Dev\dealality\ws-adp`  
**Branch:** `feat/adp/methodology-consistency-admin-actions-v1`  
**Authorization:** Read-only audit + provenance + staged-only normalize + Admin action layout.  
**STOP before merge / customer publication.**

---

## 1. Latest authoritative ADP methodology

Canonical gate: `ADP_METHODOLOGY_CANONICAL_V1`

| Layer | Authoritative ID |
|---|---|
| Product | `ai_demand_positioning_v1` |
| Measurement contract | `ADP_MEASUREMENT_CONTRACT_V1` |
| AI visibility / consideration | `ADP_AI_CONSIDERATION_INDEPENDENT_UNPROMPTED_V1` |
| Governed Presence Index | `adp_governed_ai_presence_index_v2` |
| Reality Gap (current) | `RG_WEIGHTED_PARTIAL_V2` |
| Reality Gap (historical immutable) | `RG_BINARY_GAP_V1` |
| Executive Read | `ADP_EXECUTIVE_READ_COMPOSITION_V3` |
| BPP metrics pack | `ADP_BRAND_PORTFOLIO_METRICS_V1_1` / `bpp-customer-v1.1-20260902-p2` |
| Performance Review builder | `ADP_MONTHLY_REVIEW_BUILDER_V1` |
| Review template | `AI_DEMAND_PERFORMANCE_REVIEW_CURRENT_TEMPLATE_V1` |
| PDF generator | `AdpMonthlyReviewReportPdfV1` |
| Renderer asset | `adp-v91-bpp-rank-only-parity-20260910` |

Source of truth: durable CURRENT snapshot envelopes under `data/adp-durable-published/` (not report version labels V1/V8/V14).

## 2. Methodology version / provenance available

- Module: `lib/ai-demand-positioning/methodology/adp-methodology-canonical-v1.js`
- Extractor: `adp-methodology-provenance-v1.js` (unwraps durable `.payload` + envelope stamps)
- Admin API coverage rows now include `methodologyCompatibility` + `methodologyProvenance`
- New Performance Review archives persist `methodologyProvenance` on metadata (no customer UI clutter)

## 3–5. Portfolio counts (live inventory — synthetics excluded)

| Class | Count |
|---|---:|
| CURRENT_COMPARABLE | **38** |
| RECOMPUTABLE | **0** |
| PARTIALLY_COMPARABLE | 0 |
| HISTORICAL_ONLY | 0 |
| EVIDENCE_INSUFFICIENT | 0 |

Live AI Demand Reviews: **38 hotels / 38 ready / 0 rebuild / 0 blocked**.  
`adp_smoke_test_synthetic` removed from live discovery (fixture under `fixtures/ai-demand-positioning/test-only/`).  
`BLOCKED_SYNTHETIC` removed from live methodology compatibility classes.  
Comparable: **38**. Recomputation required: **0**. Evidence gaps: **0**.

## 6. Important metric / template differences

- Flat `published-snapshot` payloads often omit methodology stamps; **durable CURRENT** envelopes carry full stamps (appearance, RG V2 dual scores, presence v2, Exec V3, template/PDF asset versions).
- Audit classifies against durable CURRENT (the live admin/customer analytics source), not against report version numbers or September archive labels alone.
- RG binary V1 remains historically immutable beside weighted V2 dual scores on current payloads.
- No new scoring formulas invented.

## 7. Hotel-by-hotel matrix

- Markdown: `reports/ai-demand-positioning/methodology/adp-methodology-compatibility-matrix-v1.md`
- JSON: `reports/ai-demand-positioning/methodology/adp-methodology-compatibility-matrix-v1.json`
- CLI: `npm run dealality:adp-methodology-compatibility-audit`

All 38 legitimate hotels: `CURRENT_COMPARABLE`.

## 8. Staged report/PDF updates

- Staged normalize planner: `adp-methodology-staged-normalize-v1.js`
- Result: **no staged metric rewrites** (`recomputableCount=0`)
- No silent overwrite of published customer reports
- No paid research; no new shares; no auto-publication

## 9. Before / after Admin row actions

**Before (prior primary):** View PDF + Open review (admin preview); Download PDF + Copy ADP URL buried in More / ADP Client column.

**After (this PR — final row-action order):**
- Primary icons: **View PDF** → **Download PDF** → **Copy URL** → **More**
- More: Open review in client view (authorized share only), Open action plan, Regenerate review, View report history (+ admin preview / meeting). View/Download PDF are not duplicated in More.

Screenshots:
- `reports/ai-demand-positioning/methodology/admin-actions-after-primary.png`
- `reports/ai-demand-positioning/methodology/admin-actions-after-more-menu.png`
- Fixture: `/app/admin/_fixtures/adp-admin-actions-fixture-v1.html`

*(Memberstack blocks Cursor browser on the live Admin HTML gate; fixture uses the same `DealalityAiAdminTable.actionsHtml` component.)*

## 10. Browser / API validation

| Check | Result |
|---|---|
| PDF download (admin API, AC Hotel A Coruña) | 200 `%PDF` 235036 bytes |
| Copy external URL host | `my-operators-backend-production.up.railway.app` HTTPS; not localhost; not raw `sht_` |
| Unauthorized share disabled path | 8 READY hotels without share (e.g. Charlotte) — Copy URL disabled with founder-approval reason |
| Open client view | uses existing `open-adp-client` + publication gate (no new share mint) |
| Methodology on admin list | `methodologyGate=ADP_METHODOLOGY_CANONICAL_V1`; 38× CURRENT_COMPARABLE |
| Local demo `/api/me` admin | `isAdmin=true` |
| GDI Reports | unaffected (`ok`, 22 hotels) |

## 11. Regression / gates

```
npm run test:adp-methodology-consistency-v1   # PASS
npm run test:adp-admin-reviews-primary-actions-v1  # PASS
```

## 12. PR / workspace status

- Registered workspace: `C:\Dev\dealality\ws-adp`
- Home: `ws/adp` (feature branch for this PR)
- Integration Controller owns merge + post-merge sync
- **AWAITING_INTEGRATION_CONTROLLER** after PR open — STOP for founder review before merge or customer publication

### Preserved untracked (not in PR)

- `.env.local.pr406-parked`
- prior recovery handoff markdown under `reports/adp-gdi-full-e2e-recovery-v1/`
- `tmp/adp-8080-restore-runtime-20261118.txt`
