# GDI Competitive Intelligence Integration V1 — Founder Report

**Date:** 2026-11-23  
**Branch:** `feat/gdi-competitive-intelligence-integration-v1`  
**Worktree:** isolated clean (from `origin/main`)  
**Authorization:** Controlled GO — no Airtable writes, no customer publication, no paid research, no deploy  
**Status:** Implementation complete for mapping + tests + dry-run persistence plan. **STOP** before live persistence.

---

## 1. Exact research → canonical mapping

| Research input | Canonical field |
|---|---|
| `dispositionRecommendation = STOP_PRIMARY_COMPETITOR_COMMITTED` | Gate — only this disposition maps |
| Named hotel in `hotelSourcingDecisionEvidence` (not `venue` alone) | `competitorHotel`, `cmiCompetitorHotel` |
| Room/hotel/negotiated block or official housing language | `competitorVenueStatus` = `CONFIRMED_CONTRACTED_BLOCK` (or `CONFIRMED_EVENT_VENUE` when host-venue language dominates) |
| Same evidence blob | `competitorBookingClaim`, `lodgingEvidence` |
| Booking evidence present | `competitorVenueConfirmed=true`, `competitorBookingEvidenceClass=VERIFIED_BOOKING` |
| `sourcesInspected` / contact `sourceUrl` | `evidence[]`, `sources[]`, `sourceUrl`, `primarySourceUrl` |
| Event title + start/end | `title`, `eventStartDate`, `eventEndDate` |
| No independently verified remaining demand | `targetHotelSourcingStatus=SOURCING_CLOSED` |
| Overflow / pilot-hotel demand verified | `targetHotelSourcingStatus=OVERFLOW_CONFIRMED` (+ dual Market Scope) |
| Always | `commercialActionability=COMPETITIVE_MARKET_INTELLIGENCE`, `customerVisible=false`, `salesReady=false`, `salesBaselineMembership=OUTSIDE_67_BASELINE`, `doNotAddToBethesda67SalesBaseline=true` |

**Module:** `lib/group-demand-intelligence/commercial-consistency/gdi-research-competitor-commitment-mapping-v1.js`  
**Hard rule:** event venue mentions alone do **not** create competitor commitment (AAEI Reagan Building ≠ Hotel Washington).

Classifier synonym retained: `hotel block` + `negotiated block` accepted alongside `room block`.

---

## 2. AAEI / AHA before → after

| ID | Org | Before (raw discovery shell) | After (mapped + annotate) |
|---|---|---|---|
| `disc_0a11a0bb1158` | AAEI | Hotel=false, Competitive=false, pops=`[]` | Competitive=true (VERIFIED), Hotel=false, competitor=`Hotel Washington`, pops=`[COMPETITIVE_MARKET_INTELLIGENCE]`, customerVisible=false, **not** on Bethesda 67 |
| `disc_9334f4f1e3dd` | AHA | Hotel=false, Competitive=false, pops=`[]` | Competitive=true (VERIFIED), Hotel=false, competitor=`Marriott Marquis Washington…`, pops=`[COMPETITIVE_MARKET_INTELLIGENCE]`, customerVisible=false, **not** on Bethesda 67 |

Evidence JSON: `reports/gdi-competitive-intelligence-integration-v1/aaei-aha-before-after.json`  
Fixture: `fixtures/gdi-competitive-intelligence-integration-v1/aaei-aha-verified-research-v1.json`

Neither is promoted to open Bethesda sales opportunity. Neither is discarded — both map into research Competitive inventory.

---

## 3. Non-destructive persistence plan

Dry-run only (`planResearchCompetitorCommitmentPersistence`):

| Layer | Behavior |
|---|---|
| Research discovery | Read dispositions (fixture / review JSON) |
| Verified evidence | Require lodging decision evidence + booking language + source URL |
| Canonical field mapping | Existing competitor/sourcing fields only — no parallel model |
| Research inventory persistence | **Planned** UPSERT to Hotel Research Inventory / FS mirror — **not executed** |
| Customer opportunity eligibility | `customerVisible=false`; outside 67; not Sales Ready |
| Market Scope presentation | Existing `annotateInventoryPopulations` after fields present |

`persistence-plan-dry-run.json`: `airtableWrites=0`, `customerPublications=0`, `bethesda67Additions=0`, `stopForFounderReview=true`.

**Separate approvals required before apply:**

1. `AIRTABLE_RESEARCH_INVENTORY_APPLY` (+ existing `GDI_RESEARCH_INVENTORY_PRODUCTION_WRITES=1` bridge gates)
2. `CUSTOMER_PUBLICATION` (explicit; Decision A does not auto-publish these discovery IDs)
3. `BETHESDA_67_BASELINE_MEMBERSHIP` (explicit; **not** requested here)

---

## 4. Bethesda 67 gate verification

- Mapped AAEI/AHA IDs are **outside** the frozen 67 (`isBethesda67BaselineId` = false).
- Hotel Market Scope remains false for these competitor-committed rows (outside baseline + no independent target demand).
- Sales-team inventory selection unchanged (still 67 baseline IDs only).
- Do **not** add discovery IDs to 67 merely to demonstrate classification.

---

## 5. Regression results

| Suite | Result |
|---|---|
| `test:gdi-competitive-intelligence-integration-v1` | **11/11 PASS** |
| `test:gdi-bethesda-67-sales-baseline-cmi-v1` | **8/8 PASS** (includes hotel-block synonym + corrected 19 allowlist vs surface-active) |
| `test:gdi-market-scope-toggle-v1` | **7/7 PASS** |
| `test:gdi-bethesda-67-sales-team-inventory-display-v1` | **10/10 PASS** (Decision A aware) |
| `test:gdi-bethesda-67-inventory-access-control-v1` | Pass after forceApproved isolation |
| Club Med Punta Cana fixtures | Competitive-only retained |

Rules re-verified: confirmed competitor → Competitive; open demand → Hotel; unknown sourcing → Hotel when eligible; overflow dual; venue-only / non-STOP disposition → no false Competitive; competitor-committed research stays internal (`customerVisible=false`).

---

## 6. Explanation of 18-versus-19

**Not a stale fixture count bug. Two real effects:**

### A) Temporal surface eligibility (the original 18)

With `forceApproved: false`, the published **allowlist remains 19 IDs**.  
`gdi_opp_bethesda_premier_cup_2026` (`eventEndDate: 2026-11-22`) is now **PAST_CLOSED** relative to 2026-11-23 → `isCustomerSurfaceActiveEligible=false` → customer facing among published = **18**.

- Manifest / publication allowlist: still **19**
- Currently surface-active published: **18**
- Correct behavior: do **not** weaken surface gates; do **not** change allowlist to 18 merely to pass a test

Test contract corrected to: allowlist size = 19; facing = surface-active published subset; Premier Cup must not leak when past-closed.

### B) Decision A approval receipt expands default to 67

`data/.../bethesda-67-customer-publication-approval.json` (`authorizedAt: 2026-11-13`, phrase “Authorize Bethesda 67 customer publication”) makes `isBethesda67CustomerPublicationApproved()` **true** by default.  
Then `selectBethesdaSalesOpportunityInventory` / customer-facing without `forceApproved:false` returns **67**, not 19.

Tests that asserted unconditional `facing.length === 19` without isolating approval were measuring the wrong gate. Updated to:

- `forceApproved: false` → ≤19 / exactly published allowlist selection
- Default with Decision A present → 67

---

## 7. PR / CI

See PR URL after open. Integration Controller owns merge. Local prechecks to run before PR update.

---

## 8. Approvals still required

| Action | Status |
|---|---|
| Mapping + classifier synonym + tests | Done in this PR |
| Live research-inventory Airtable apply | **STOP — needs separate founder GO** |
| Customer publication of AAEI/AHA | **STOP — not authorized** |
| Add AAEI/AHA to Bethesda 67 | **STOP — not requested / not done** |
| Paid research | Not used |
| Production deploy | Not requested |

---

## Files touched (summary)

- `lib/.../gdi-research-competitor-commitment-mapping-v1.js` (new)
- `lib/.../bethesda-67-sales-baseline-cmi-v1.js` (hotel/negotiated block synonym)
- `fixtures/gdi-competitive-intelligence-integration-v1/aaei-aha-verified-research-v1.json`
- `scripts/test-gdi-competitive-intelligence-integration-v1.mjs`
- Bethesda inventory tests (18/19 + Decision A isolation)
- `package.json` script entry
- This report + dry-run JSON artifacts
