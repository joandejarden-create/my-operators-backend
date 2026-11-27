# GDI Competitive Intelligence Audit & Filter Layout

Date: 2026-11-27  
Server: http://localhost:8080  
Paid research: **none** (read-only)

## 1. Exact reason Wyndham Competitive = 0

**Measured STAGED zero — not a Market Scope filter false negative.**

STAGED `gdi_hotel_wide_fqo_recnPB3Q02NO8Ma1o_20261127_6f4aef5e` publishes `inventoryCounts: { all: 26, hotel: 26, competitive: 0 }` and every opportunity lacks competitive eligibility flags. The UI filter correctly shows Competitive (0).

### Cause chain

| Layer | Finding |
|---|---|
| Competitor / CAD research | Prior live competitive log lists **15** CAD `FQO_CANDIDATE` rows with named competitors (Marriott Marquis San Diego Marina, InterContinental San Diego) |
| Checkpoint B (PR #620) | **OPEN** — paid PLP/CAD/leisure **not executed** in that PR |
| CAD read-only pilot | Wyndham CLEAN 11: 0 competitive classes; HOLD: 1 historical + 3 emerging + **0 FQO_CANDIDATE** |
| Qualification / locked 26 | Locked STAGED 26 is hotel-facing FQO inventory; prior CAD candidates were **not** promoted into Competitive population |
| Persistence / STAGED | Competitive classification absent from current STAGED DTO |
| API / filter | Faithfully mirrors STAGED competitive=0 |

**Classification:** research existed / partially executed historically, but competitive opportunities are **not in current STAGED**; Checkpoint B paid CAD has **not** run.

## 2. Market Scope semantics (unchanged methodology)

Hotel and Competitive are **overlapping**, not mutually exclusive.

- An opportunity may belong to Hotel, Competitive, or both.
- **All** = unique opportunity union (deduped by id).
- Filter + optional List column now share `opportunityInventoryPopulations` / eligibility taxonomy (Hotel / Competitive / Hotel · Competitive).

## 3. Data-mapping corrections applied

1. Optional Market Scope column hydrates from the same taxonomy as the filter (no empty cells when column enabled).
2. Market Scope remains offered for Hotel-only inventories so column can display **Hotel** (parity with filter classification).
3. Filter layout: fixed widths for Property / Weekly Change / Opportunity Type / Market Scope to stop Opportunity Type resizing when Market Scope changes.

## 4. Product-wide scorecard

| Hotel | STAGED | All | Hotel | Competitive | Competitive research | Mapping defect |
|---|---:|---:|---:|---:|---|---|
| Wyndham San Diego Bayside | 26 | 26 | 26 | 0 | PARTIAL_HISTORICAL — CAD candidates logged in prior live E2E; Checkpoint B paid CAD NOT executed | NOT_A_FILTER_BUG — STAGED package competitive=0; prior CAD candidates not in locked 26 |
| Bethesda Marriott | 68 | 69 | 63 | 10 | YES — competitive inventory present in STAGED (filter Competitive > 0) | OK |
| Sheraton Guadalajara | 38 | 38 | 34 | 4 | YES — competitive rows in STAGED DTO | OK |
| Hilton packages sample | 6 | 6 | 5 | 1 | YES — competitive rows in STAGED DTO | OK |

## 5. Remaining research recommendations (no spend under this instruction)

1. After PR #620 IC merge + sync: run authorized Checkpoint B CAD/PLP on association HOLD inventory with hard ceiling.
2. Preserve CAD Layer-1 classifications through FQO → STAGED without inventing competitive flags.
3. Reconcile prior competitive-log FQO_CANDIDATE ids against locked 26 — promote only when six-gate FQO + founder process authorize.
4. Do **not** auto-classify every event held elsewhere as Competitive.

## 6. Governance

FULL E2E → STAGED → FOUNDER APPROVAL → CURRENT  
`DEALALITY_ADDITIVE_NON_REGRESSION_V1`  
No CURRENT publication · no paid research · no unauthorized qualification changes
