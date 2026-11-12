# Bethesda 67 Sales Baseline + CMI — Founder Publication Review

**Hotel:** Bethesda Marriott (`recLuxvwwxID7U2B8`)  
**Zero paid:** YES · **Auto-publish:** NO  
**Customer approval:** NOT APPROVED (hotel-facing feed stays at **19**)  
**Main baseline before this PR:** `7c513888878ebffe9a48df094f1a819ca455d38e`

## Operating model correction

The frozen **67** IDs are the sales-team Opportunities baseline (19 published + 48 staged).  
They are **not** permanent founder-only research inventory.  
They are **not** all Sales Ready.

**67 Sales Opportunities + 7 Competitive Intelligence Records**

## 1. Exact 67-ID baseline reconciliation

| Check | Result |
|-------|--------|
| Expected baseline IDs | **67** |
| Bodies recovered (store ∪ historical union) | **67** |
| Missing bodies | **0** |
| Published 19 intact | **YES** |
| Current customer-facing | **19** |
| Founder-review selectable | **67** |

Manifest: `01-67-id-manifest.json`  
Source: `reports/gdi-bethesda-67-to-19-forensic-reconciliation-v1/`

## 2. Commercial maturity breakdown (67)

| Maturity | Count |
|----------|------:|
| Sales Ready | 14 |
| Account Development | 21 |
| Qualification Required | 1 |
| Research Continuation / Monitor | 31 |

## 3. Customer-readiness of the additional 48

| Readiness | Count |
|-----------|------:|
| CUSTOMER_READY_CONTENT | 12 |
| INCOMPLETE_OR_UNCERTAIN | 4 |
| REQUIRES_ADDITIONAL_RESEARCH | 32 |

Full per-record package: `02-staged-48-publication-review.json`

## 4. Publication gate (no auto-publish)

Until founder approval via:

- `data/group-demand-intelligence/hotels/recLuxvwwxID7U2B8/bethesda-67-customer-publication-approval.json` with `approved: true`, **or**
- `GDI_BETHESDA_67_CUSTOMER_PUBLICATION_APPROVED=1`

…the hotel customer/share Opportunities feed remains the **approved 19**.

After approval, `customer-visibility` allowlists the full 67 baseline with honest maturity labels (does not invent Sales Ready).

## 5–7. Existing Exa / cached CMI (zero paid)

| Class | Count |
|-------|------:|
| Verified `COMPETITIVE_MARKET_INTELLIGENCE` | **7** |
| Needs further verification | **32** |

Verified (existing Exa recovery seeds + named overflow hosts):

- NADO/DDAA WashCon 2027 overflow → Crystal Gateway Marriott  
- NADO/DDAA WashCon 2028 → Crystal Gateway Marriott  
- ACVNU Renal Week → The Bethesdan Hotel (Tapestry)  
- World Biomaterials 2028 overflow  
- AAD 2028 overflow  
- ECS 251 2027 overflow  
- AFCEA HITS 2027 (title documents Bethesda North booking / overflow)

No new Exa calls. Speculative competitor strings alone are **not** verified CMI.

## 8. Populations / UI

Opportunity Type dropdown (default **Sales Opportunities**):

1. Sales Opportunities  
2. Competitive Market Intelligence  
3. All Commercial Intelligence  
4. Existing demand-category filters (unchanged)

Sales Opportunities chip row: **unchanged**  
Competitor Capture chip: **unchanged**  
Reset View: **unchanged** (defaults to Sales Opportunities)  
Tabs: Opportunities + Hotel Need Times & Priorities preserved

## 9–11. CSV / evidence

- Founder-review 67 CSV: `Bethesda Marriott - Founder Review 67 Sales Baseline.csv`  
- Customer published 19 CSV: `Bethesda Marriott - Customer Published 19.csv`  
- Full-detail contract preserved (64 columns)

## 12–13. Verification

| Gate | Result |
|------|--------|
| `npm run test:gdi-bethesda-67-sales-baseline-cmi-v1` | PASS |
| `npm run test:gdi-bethesda-67-customer-feed-reconciliation-v1` | PASS (19 facing) |
| `npm run test:gdi-customer-visibility` | PASS |
| `npm run test:gdi-customer-csv-export` | PASS |
| Cross-hotel baseline replication | Blocked |
| Paid research | None |

## 14. PR / CI / merge

Filled after merge.
