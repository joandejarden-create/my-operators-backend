# GDI Executive Summary Phase 1 — Navigation & Inventory Regression Remediation

**Status:** `GDI_EXECUTIVE_SUMMARY_PHASE_1_REGRESSION_REMEDIATED — AWAITING_FOUNDER_ACCEPTANCE`  
**PR:** [#618](https://github.com/joandejarden-create/my-operators-backend/pull/618)

## 1. Navigation root cause + fix

PR #618 replaced constrained tab widths with `flex: 1 1 0` / `max-width: none` / `justify-content: space-between`, stretching four tabs across the viewport.

**Restored** pre-Executive Summary compact left-aligned CSS:

- `flex: 0 1 auto`
- `min-width` / `max-width` constraints (112–176px)
- `justify-content: flex-start`
- Executive Summary uses the same constrained width family
- “How to Read This Report” remains `margin-left: auto` on the far right

Evidence screenshot: `screenshots/gdi-four-tabs-left-aligned-compact.png`  
Measured: tabs left=30…394 (spread ~476px) vs nav width ~1580px; Opportunities default.

Build stamp: `gdi-exec-summary-p1e`

## 2. Wyndham “11 of 11” root cause

**Not an Executive Summary inventory regression.**

| Layer | State |
|---|---|
| STAGED pointer | `gdi_hotel_wide_fqo_recnPB3Q02NO8Ma1o_20261125_3283ee92` |
| STAGED count | **11** (CLEAN11) |
| CURRENT | **none** |
| Locked proposed qualified | **26** (CLEAN11 11 + Turner 1 + ACG 1 + QA write-ready 13) |
| HOLD | Arevon + Level 3 (not qualified) |
| Write-set applied? | **No** (governance: NO_STAGED_ACTIVATE / NO_CURRENT_PUBLISH) |

UI “Showing 11 of 11” = authorized STAGED CLEAN11 package only.

### Exact 15 missing from STAGED (accepted proposed additions)

| ID | Org | Bucket |
|---|---|---|
| `disc_75ee8629955e` | Turner Construction Company | PRIOR_VALIDATED_ADDITION |
| `gdi_wyndham_live_associatio_a790d46ad631` | ACG San Diego | PRIOR_VALIDATED_ADDITION |
| `disc_0fa36c29506f` | Bora Biologics | NEW_WRITE_READY_ADDITION |
| `disc_aa66ce064dbd` | Cirsium Biosciences | NEW_WRITE_READY_ADDITION |
| `disc_c5596f4fa5e2` | OHLA USA, Inc. | NEW_WRITE_READY_ADDITION |
| `disc_0185c1ea7cbb` | Seasats | NEW_WRITE_READY_ADDITION |
| `disc_5b183538390f` | EDAN Diagnostics | NEW_WRITE_READY_ADDITION |
| `disc_61593e390477` | Bora Biologics | NEW_WRITE_READY_ADDITION |
| `disc_ecba7545037a` | Saronic Technologies | NEW_WRITE_READY_ADDITION |
| `disc_1b7b55bca2b2` | Hensel Phelps Construction Co. | NEW_WRITE_READY_ADDITION |
| `disc_f9f06522dd52` | Stacy Witbeck, Inc. | NEW_WRITE_READY_ADDITION |
| `disc_2e67ee5dc429` | Athletic Brewing | NEW_WRITE_READY_ADDITION |
| `disc_67a7c2b44502` | Grifols Diagnostic | NEW_WRITE_READY_ADDITION |
| `disc_7a658d5504ba` | Firestorm Labs | NEW_WRITE_READY_ADDITION |
| `disc_84bb84a372dd` | Flatiron Construction | NEW_WRITE_READY_ADDITION |

HOLD (not presented as qualified): `disc_d12b8604951f` Arevon Energy · `disc_fab21d1173f4` Level 3 Construction.

Full machine reconciliation: `WYNDHAM_PROPOSED_26_FOUNDER_REVIEW.json` + HTML.

## 3. Founder review access (non-publishing)

1. **Admin API (auth):**  
   `GET /api/admin/group-demand-intelligence/hotels/recnPB3Q02NO8Ma1o/proposed-write-set-review`
2. **Static review:**  
   `reports/gdi-executive-summary-v1/WYNDHAM_PROPOSED_26_FOUNDER_REVIEW.html`
3. Distinguishes STAGED CLEAN11 vs proposed additions vs HOLD vs CURRENT (none).
4. Does **not** treat 161 discovery or 25 WHO/HOW continuations as qualified.
5. Does **not** activate STAGED or publish CURRENT.

Remaining authorization for publication (unchanged): founder Airtable write + STAGED activate + CURRENT publish — separate GO required.

## 4. Product-wide opportunity-ID audit

Artifact: `PRODUCT_WIDE_OPPORTUNITY_ID_AUDIT.json`

| Hotel | STAGED | CURRENT | ES KPI parity |
|---|---:|---:|---|
| Bethesda Marriott | 68 | 67 | yes (CURRENT) |
| Wyndham SD Bayside | 11 | 0 | yes (STAGED preview) |
| YOTEL Geneva Lake | 42 | 42 | yes |
| Hilton NY Times Square | 0 | 0 | n/a (no pointer) |
| Sheraton Guadalajara Expo | 38 | 0 | yes (STAGED) |
| Radisson Blu Toronto | 66 | 66 | yes |
| Wyndham Alltra Punta Cana | 27 | 0 | yes |
| Memmo Príncipe Real | 11 | 0 | yes |

Renaissance New York Times Square: not present in bundled publication packages (Hilton TS is).

Conclusion: Executive Summary did not rewrite Opportunities inventory IDs. Wyndham 11 is pre-existing CLEAN11 STAGED gate.

## 5. Regression

`npm run test:gdi-executive-summary-v1` → **21 PASS**  
Includes compact nav CSS, Opportunities default, Wyndham 26↔CLEAN11, ES ID preservation, preview auth negatives.

## 6. PowerShell

```powershell
cd C:\Dev\dealality\ws-gdi
npm run test:gdi-executive-summary-v1
.\scripts\gdi\Invoke-GdiExecutiveSummary.ps1 -Mode inventory-parity
.\scripts\gdi\Invoke-GdiExecutiveSummary.ps1 -Mode wyndham-proposed-review
.\scripts\gdi\Invoke-GdiExecutiveSummary.ps1 -HotelId recLuxvwwxID7U2B8 -Mode validate
.\scripts\gdi\Invoke-GdiExecutiveSummary.ps1 -HotelId recnPB3Q02NO8Ma1o -Mode validate -Preview staged
```

## Non-regression assessment (PR #618 — binding)

Per permanent principle `DEALALITY_ADDITIVE_NON_REGRESSION_V1`:

| Surface | Baseline | After | Classification |
|---|---|---|---|
| GDI tab layout | Compact left-aligned 3 tabs | Compact left-aligned **4** tabs (ES added) | **Intentional additive** after restoring pre-ES constrained CSS (unintended full-width flex distribution remedié) |
| Opportunities default | Opportunities landing | Opportunities landing preserved | **Preserved** |
| Bethesda CURRENT inventory | 67 IDs | 67 IDs; ES KPI = 67 | **Preserved** |
| Wyndham STAGED inventory | CLEAN11 (11 IDs) | CLEAN11 (11 IDs) | **Preserved** — locked 26 not applied; founder review surface is additive/non-publishing |
| YOTEL CURRENT | 42 | 42 | **Preserved** |
| Customer visibility rules | CURRENT-only for customers | unchanged | **Preserved** |
| Publication controls | no silent STAGED/CURRENT | unchanged | **Preserved** |

Unintended regressions found and resolved in this PR cycle:

1. Full-width tab distribution (`flex: 1 1 0`) → restored compact left alignment.
2. Misread of Wyndham 11 as ES inventory loss → proven CLEAN11 STAGED gate; additive founder review for locked 26 without publish.

Automated coverage added: compact nav CSS, Opportunities default, Wyndham 26↔CLEAN11 ID reconciliation, ES ID preservation, preview auth negatives (`npm run test:gdi-executive-summary-v1`).

## Governance

NO_PAID_RESEARCH · NO_PAID_ENRICHMENT · NO_AIRTABLE_WRITES · NO_STAGED_ACTIVATE · NO_CURRENT_PUBLISH · NO_PRODUCTION_DEPLOYMENT · locked 26 immutable · no Phase 2 · no independent merge
