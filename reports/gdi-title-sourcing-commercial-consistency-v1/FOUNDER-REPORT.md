# GDI — Permanent Title, Sourcing Status & Competitive Intelligence Consistency

**Branch:** `fix/gdi-regression-recovery-v1` (local only — no deploy / merge without founder authorization)  
**Generated:** 2026-11-13  
**Contract:** extend existing title + commercial-consistency path (no second sanitizer)  
**Baseline preserved:** Market Scope 67 / 61 / 10 · 14 demand segments · Surfe · guided tour · Hotel Validation / Action / Outcome · CSV · original source titles

---

## 1. Root causes (exact functions / files)

| Symptom | Root cause | Fix |
|---|---|---|
| Titles still showed `— Crystal Gateway Marriott Host`, `Competitor Room Block…`, `WEWCC + Marriott Marquis` | Title contract v1.0 missed Host / venue / competitor-room-block suffixes | Extended `customerFacingOpportunityTitle` / `normalizeCustomerFacingOpportunityTitle` in `lib/group-demand-intelligence/customer-facing-opportunity-title-contract-v1.js` (v1.1 patterns + competitorHotel opts) |
| Drawer / overview said **Hotel sourcing = Venue Confirmed** while competitor overflow was unconfirmed | `normalizeHotelSourcingStatus` + `attachEventsOpportunityOverview` collapsed competitor venue into one of four legacy labels and **overwrote** `hotelSourcingStatusLabel` | `buildHotelSourcing` now prefers `composeCustomerFacingHotelSourcingLabel` (`events-opportunity-overview-v1.js`); DTO + presentation sanitizer re-stamp overview |
| Location row showed `Host hotel contracted — Crystal Gateway Marriott` | `buildLocation` treated venue-commitment prose as geography (`eventLocationSummary`) | Reject venue-status strings in `buildLocation`; NADO ledger remediation sets Arlington / DC metro geography |
| Card / narrative: `… is dated in Host hotel contracted — …` / `Qualify Overflow — Qualify Overflow — …` | Hotel-relevance template glued venue status into “dated in {location}”; sanitizer stripped only “dated in” leaving “is Host hotel contracted”; CMI UI concatenated action label + body | `hotel-relevance-research-v1.js` venue-status branch; `sanitizeDuplicatedActionNarrative`; UI no longer concatenates `Qualify Overflow —` onto action body |
| Card meta still showed Venue Confirmed after API label was correct | Stale `ov.tileClassificationLine` / overview `hotelSourcing` preferred over composed label | `dealality-gdi-ui.js` prefers `hotelSourcingStatusLabel`; overview HTML uses composed sourcing |

**Central modules (single path — no second sanitizer):**

- `lib/group-demand-intelligence/customer-facing-opportunity-title-contract-v1.js`
- `lib/group-demand-intelligence/commercial-consistency/commercial-status-dimensions-v1.js` → `composeCustomerFacingHotelSourcingLabel`
- `lib/group-demand-intelligence/commercial-consistency/commercial-presentation-sanitizer-v1.js`
- `lib/group-demand-intelligence/commercial-consistency/cmi-evidence-ledger-v1.js` (NADO 2027/2028 evidence remediations)
- `lib/group-demand-intelligence/events-opportunity-overview-v1.js`
- `lib/group-demand-intelligence/opportunity-list-dto.js`
- `public/js/group-demand-intelligence/dealality-gdi-ui.js`

---

## 2. Three independent dimensions (enforced)

| Dimension | Representation | Must not |
|---|---|---|
| **A. Demand type** | Canonical 14 segments / opportunity type (e.g. Association / Conference / Events) | Be overwritten by Competitive Market Intelligence |
| **B. Market relationship** | Hotel opportunity · Competitive Market Intelligence · Both (`inventoryPopulations` / Market Scope) | Drive Opportunity Type filters |
| **C. Commercial sourcing** | Competitor venue status **and** target-hotel sourcing (composed label) | Collapse into a single “Venue Confirmed” that reads as open hotel RFP |

Composer example (NADO 2027):

`Competitor venue confirmed (Crystal Gateway Marriott); target-hotel overflow sourcing unconfirmed`

---

## 3. NADO 2027 — expected vs live local

| Field | Expected | Live (local browser + API) |
|---|---|---|
| Title | NADO & DDAA Washington Conference 2027 | ✅ |
| Competitor venue | Crystal Gateway Marriott | ✅ |
| Hotel sourcing | Competitor venue confirmed; Bethesda overflow unconfirmed | ✅ composed label on card meta + overview |
| Location | Geography (not “Host hotel contracted…”) | ✅ Arlington, VA / Washington, DC metro |
| What is happening | Competitor host; overflow unconfirmed | ✅ ledger `summaryWhat` |
| Recommended action | Contact housing re: overflow; do not RFP as host | ✅ (no duplicated Qualify Overflow prefix) |
| Feedback | Deferred preserved | ✅ drawer shows Deferred · Sep 17, 2026 |

**Not portrayed as an open host-hotel RFP.**

---

## 4. Portfolio audit (17 hotels, 699 opportunities)

Source: `reports/gdi-title-sourcing-commercial-consistency-v1/00-portfolio-audit.json`

| Metric | Count |
|---|---|
| Hotels scanned | 17 |
| Opportunities scanned | 699 |
| Customer-facing titles still carrying competitor/host/venue suffixes after presentation | **0** |
| Card/overview/API sourcing-label inconsistencies (composed vs overview) | **0** |
| Duplicated / malformed narratives remaining (`Qualify Overflow — Qualify Overflow`, `dated in Host hotel contracted`) | **0** |
| Founder dirty-title examples normalized by contract | **5 / 5** (NADO 2027/2028, ACVNU, ECS, WBC) |

Before → after (Bethesda):

| ID | Before title | After title | After sourcing |
|---|---|---|---|
| `gdi_opp_nado_ddaa_washcon_2027_overflow` | NADO Arlington Washington Conference 2027 (overflow) / Host suffix variants | NADO & DDAA Washington Conference 2027 | Competitor venue confirmed (Crystal Gateway Marriott); target-hotel overflow sourcing unconfirmed |
| `gdi_opp_nado_ddaa_washcon_2028` | NADO Arlington Washington Conference 2028 | NADO & DDAA Washington Conference 2028 | Same pattern |
| `gdi_opp_acvnu_renal_week_20270412` | … Competitor Room Block… | ACVNU Renal Week 2027 | Competitor venue confirmed (The Bethesdan…); overflow unconfirmed |
| `gdi_opp_ecs_251_2027_overflow` | … WEWCC + Marriott Marquis | 251st ECS Meeting 2027 | Competitor venue confirmed (Marriott Marquis Washington, DC); overflow unconfirmed |

---

## 5. Regression tests

```text
npm run test:gdi-regression-recovery-v1
```

All passed, including:

- `test-gdi-regression-recovery-v1` (Market Scope / polish / title module)
- `test-gdi-opportunity-type-classification-v1` (recovered=182, remaining_research=70; Bethesda Market Scope intact)
- `test-gdi-title-sourcing-commercial-consistency-v1`
  - competitor venue excluded from canonical title
  - NADO 2027 ledger + overview sourcing + location
  - malformed Host-hotel-contracted narrative rewrite
  - sourcing composer independence
  - no repeated action prefix
  - Bethesda 67 clean titles
  - portfolio sample scan

Local browser (Bethesda Competitive): clean titles, composed sourcing on cards, overview Hotel sourcing + Location correct, Deferred feedback visible.

---

## 6. Preserved recovery surfaces

- Market Scope counts / filtering  
- Canonical 14 demand segments  
- Card badge presentation  
- Surfe contact lookup  
- Guided tour  
- Hotel Validation / Action / Outcome (Deferred preserved on NADO 2027)  
- Source evidence URLs on CMI claims  
- CSV export path (same DTO title + sourcing fields)  
- Original / research titles on `originalSourceTitle` / `researchTitlePreserved`  

**No publication eligibility or scoring rule changes.** Competitor bookings remain Competitive Market Intelligence / overflow-qualify — not auto-promoted to sales-ready open RFPs.

---

## 7. Remaining unresolved (research — not presentation bugs)

- Portfolio classification research queue: **70** opportunities still research-flagged from prior Opportunity Type remediation (unchanged by this task).  
- Some competitive rows still carry stretch fit prose that mentions competitor geography; commercially correct but can be tightened in a later editorial pass.  
- ECS location string still mentions WEWCC + Marquis as place context (not a title suffix); optional geography cleanup if founder wants pure city-only Location rows everywhere.

---

## 8. Authorization

**Do not deploy or merge without founder authorization.**
