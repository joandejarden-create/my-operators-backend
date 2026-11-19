# GDI 3.0 — Portfolio Demand Intelligence Assessment V1

**Status:** Founder review gate (read-only assessment)  
**Date:** 2026-11-19  
**Stream:** `gdi` · Workspace `C:\Dev\dealality\ws-gdi`  
**Does not disrupt:** Slice 2A PR #444 · prior Slice 1 merges  
**Does not authorize:** UI implementation · schema changes · paid research · production Airtable writes · customer publication · deployment

**Success target:** One market research process serving multiple hotels, presented through a management-company portfolio view, with evidence-backed qualification and individual hotel pursuit recommendations.

---

## 1. Existing architectural components to reuse

### GDI customer UI (tab host)

| Component | Path | Reuse |
| --- | --- | --- |
| Page shell + section nav | `public/js/group-demand-intelligence/dealality-gdi-ui.js` | Add a third customer tab **left of** current Priorities tab |
| Customer tabs today | `GDI_MAIN_TAB` (`opportunities`) + `GDI_PRIORITIES_TAB` (`priorities`) | **Do not replace** Priorities |
| App routing / tab mount | `public/js/group-demand-intelligence/app.js` | Mount new panel beside opportunities + priorities |
| Commercial Priorities tab | `hotel-need-times-priorities-tab.js` | Leave intact |
| Opportunity drawer | `gdi-universal-presentation-controller.js` → `openOpportunityDetail` | Reuse with `hotelId` + `opportunityId` + hotel-scoped detail URL |

**Naming note:** The live tab label is **“Commercial Priorities”** (not “Commercial Properties”). Portfolio Demand Intelligence should sit immediately to the **left** of Commercial Priorities:

`Group & Demand Opportunities` · **`Portfolio Demand Intelligence`** · `Commercial Priorities`

Confirm label with founder if “Commercial Properties” was intentional rename vs typo.

### Market intelligence layer (already built, under-wired)

| Module | Path | Role |
| --- | --- | --- |
| Market Opportunity Graph index | `lib/group-demand-intelligence/market-opportunity-graph/` | Shared market packet + cross-hotel fit |
| Extract market packet | `market-opportunity-packet-v1.js` → `extractMarketOpportunityPacket` | Hotel-neutral org/event/evidence/contacts |
| Cross-hotel fit | `cross-hotel-fit-v1.js` → `evaluateCrossHotelFit` | Score + reasons + missingData + finalState |
| Build hotel opp from market | `build-hotel-opportunity-from-market-v1.js` | Deterministic `marketOpportunityId` → hotel-scoped opp id |
| Geography / applicability | `market-geography-v1.js`, `geographic-applicability-v1.js`, `hotel-geography-profile-v1.js` | Catchment / metro / territory |
| Weekly Change durability | `workflow/gdi-weekly-change-durability-v1.js` + `weekly-delta.js` | NEW / UPDATED / UNCHANGED (evidence-based) |
| Slim persistence / one-run contract | Slice 2A modules (once #444 merges) | Durable market+hotel intelligence survival |

### Hotel qualification / HE attributes

| Source | Usable fields | Caveat |
| --- | --- | --- |
| GDI hotel config | `capabilityProfile.totalGuestrooms`, `totalMeetingSpaceSqFt`, peak bands, `demandTerritory`, city/country | Per-hotel JSON under `config/group-demand-intelligence/hotels/` |
| Hotel census / HPC | `rooms`, `Market`, `Submarket`, `Chain Scale`, `Property Type`, **`Management Company`**, Affiliation/Parent | Meeting space often flag-only; do not invent sqft |
| `buildHotelGeographyProfile` | rooms, meetingSqFt, territory, event-space domain status | Treat `UNKNOWN_UNRESEARCHED` as UNKNOWN — never as zero capability |
| Commercial qualification V41 / hotel fit | Existing GDI qualify path | Hotel-specific; must stay hotel-scoped |

### Auth / isolation (must keep)

- Hotel-scoped publication packages + share tokens (`gdi-signed-share-capability-v1`)
- Airtable opportunity `hotel_boundary_violation` if hotelId mismatches
- Feedback / sales notes FS-only per hotel (`feedback.json` + sales-owned payload keys)
- Cross-operator confidential fields must never appear on shared market packets (already stripped in `extractMarketOpportunityPacket`’s hotel-specific exclusions — extend for portfolio APIs)

---

## 2. Operator / market / hotel relationships (current reality)

### What exists

| Relationship | Today | Gap |
| --- | --- | --- |
| Hotel ↔ GDI inventory | HPC `rec…` hotelId; `listGdiSelectableHotels()` | No `operatorId` on selectable DTO |
| Hotel ↔ Aimbridge (example) | Free-text notes in hotel config (“Aimbridge LATAM managed”) for Guadalajara / voco Cancún / Alltra | **Not a structured operator portfolio** |
| Hotel ↔ Management Company | Census/HPC field exists | Not wired into GDI hotel list or auth |
| Hotel ↔ Market | Config city/country + Dealality Market/Submarket on census | No first-class “operator × market → hotels[]” index |
| Market opportunity ↔ many hotels | MOG `marketOpportunityId` + `evaluateCrossHotelFit` | Shadow/offline; not customer UI; not default Full E2E |
| Operator master | Operator Explorer / Operator Master Airtable | Not linked to GDI hotel selector or share auth |

### Aimbridge CALA · Guadalajara example

- **Hotel:** Sheraton Guadalajara Expo `recCAGqkERSXgiku4` (Aimbridge-managed note).  
- **Market:** Guadalajara / Mexico (Dealality geography — confirm Market label from census/config).  
- **Portfolio peers in other markets:** voco Cancún, Wyndham Alltra — same operator notes, **different markets**; must not auto-qualify Cancún hotels for Guadalajara demand.

**Minimum relationship model (prefer config/API over new Airtable table initially):**

```text
OperatorPortfolioView
  operatorId | operatorDisplayName
  markets[] → { marketId, marketLabel, hotelIds[] }
  authorization: memberstack / share capability scoped to operator OR explicit hotel allowlist

MarketOpportunity (shared)
  marketOpportunityId  (existing MOG identity)
  organization / program / evidence / public contacts / partners / competitor venues
  weeklyChange vs prior market baseline

HotelOpportunityMatch (derived, hotel-scoped)
  marketOpportunityId + hotelId
  fitClass | confidence | evidence | limitations | recommendedPursuit
  links to existing hotel opportunity id when published/persisted
```

**Do not** duplicate the market research object per hotel. Persist hotel qualification as a thin match layer (existing pattern: `buildHotelOpportunityFromMarketPacket`).

---

## 3. Proposed new GDI tab structure

### Navigation

Insert tab id `portfolio` (label **Portfolio Demand Intelligence**) in `GDI_CUSTOMER_TABS`:

```text
[ Group & Demand Opportunities ] [ Portfolio Demand Intelligence ] [ Commercial Priorities ]
```

### Panel contents (R1 read-only)

1. **Operator selector** — Aimbridge CALA fixture first; later Operator Master–backed  
2. **Market selector** — markets where that operator has ≥1 authorized hotel  
3. **Summary chips** — participating hotels · shared market opportunities · qualified matches  
4. **Filters** — demand segment · opportunity maturity · Weekly Change (NEW / UPDATED / UNCHANGED)  
5. **Fit matrix** — rows = market opportunities; columns = participating hotels; cells = fit class + confidence  
6. **Detail** — market opportunity summary + per-hotel fit explanation + “Open hotel brief” → existing drawer for that hotel’s opportunity id  
7. Empty / loading / error / insufficient-evidence states (Dealality UI rules)

### Non-goals for R1 UI

- No redesign of Commercial Priorities  
- No new drawer implementation  
- No live Full E2E trigger from this tab  
- No cross-operator comparison

---

## 4. Hotel opportunity-fit contract (proposed)

Reuse `evaluateCrossHotelFit` outputs; map to founder-facing fit classes:

| Fit class | Meaning | Maps from (initial) |
| --- | --- | --- |
| `POTENTIAL_PRIMARY_HOST` | Credible host venue for the demand mechanism | High physical+geo fit; meeting capability known sufficient when required |
| `OVERFLOW_ACCOMMODATION` | Room-block / overflow role | Housing/overflow demand mechanism; rooms band OK; meeting not required |
| `PREFERRED_LODGING_OPPORTUNITY` | Preferred lodging / partner hotel path | Preferred lodging segment / venue partnership signals |
| `MEETING_CATERING_OPPORTUNITY` | Meetings/F&B without full host claim | Meeting demand; space known; overnight optional |
| `OTHER_PARTNERSHIP` | Relevant non-host commercial path | Planner/DMC/partner route with hotel relevance |
| `UNSUITABLE` | Evidence shows mismatch | `NOT_FIT` / geo NONE / known capability conflict |
| `INSUFFICIENT_EVIDENCE` | Cannot decide | `HOTEL_MATCHED_NEEDS_MORE_DATA` / UNKNOWN attributes |

**Rules**

- Missing hotel attributes → **UNKNOWN** (never invent rooms, meeting sqft, or demand).  
- Explicit confidence: `HIGH` / `MEDIUM` / `LOW` / `UNKNOWN` from evidence completeness.  
- Evidence array required for non-UNKNOWN classes (URLs or structured claim refs).  
- Limitations[] required when confidence &lt; HIGH or fit is overflow/partnership.  
- Recommended pursuit is hotel-scoped copy; must not leak other hotels’ sales feedback.  
- One market opportunity may produce multiple hotel matches with different fit classes.

Machine draft (implementation slice): `gdi-portfolio-hotel-opportunity-fit-contract-v1.js` — **not created in this assessment PR**.

---

## 5. Data and permission gaps

| Gap | Severity | Recommendation |
| --- | --- | --- |
| No structured operator→hotels→markets index for GDI | **P0** | R1: fixture registry for Aimbridge CALA markets; R2+: derive from census `Management Company` + Dealality Market with steward review — **prefer config/API before new Airtable table** |
| GDI hotel DTO lacks operatorId | **P0** | Extend list/detail DTO with optional `operatorId` / `managementCompany` (read-only) |
| Market opportunities stored only as hotel-scoped rows | **P0** | Use MOG `marketOpportunityId` extraction over existing hotel inventories; later persist market layer without duplicating blobs |
| Airtable DG reuse off by default; partners thin on Full E2E path | **P1** | Depends on Slice 2A honesty + 2B relationship wiring (same-run lifecycle) |
| Weekly Change not default on every Full E2E | **P1** | R3 portfolio Weekly Change after canonical Weekly Change wiring |
| Cross-operator isolation for portfolio API | **P0** | New authz: caller may only see operators/hotels in their allowlist; strip sales-owned keys from shared market payloads |
| Drawer deep-link lacks opportunityId query | **P2** | Add optional `opportunityId` query when opening from matrix |
| HE meeting capability often UNKNOWN | **P1** | Surface INSUFFICIENT_EVIDENCE; never invent |
| “Commercial Properties” vs “Priorities” label | **P3** | Founder confirm |

**Airtable:** No new tables required for R1. Demonstrate need before any Partner/Portfolio table. Reuse GDO + Research Opportunities + DG graph + MOG identities.

---

## 6. Recommended implementation PR sequence

Keep Slice 2A (#444) independent. Start Portfolio work only after founder approval of this assessment (and preferably after #444 merges for persistence honesty).

| PR | Scope | Auth / writes |
| --- | --- | --- |
| **PDI-0** (this) | Assessment doc only | None |
| **PDI-1** | Fit contract module + offline tests (Aimbridge Guadalajara fixtures); operator-market-hotel fixture registry | $0 · no UI · no Airtable writes |
| **PDI-2** | Read-only API: operator → markets → hotels → market opportunities → fit matrix (fixtures + existing hotel inventories) | Authz isolation tests · no production writes |
| **PDI-3** | GDI tab UI (left of Commercial Priorities); matrix + open existing drawer | Authenticated GDI UI acceptance · no publish |
| **PDI-4** | Integrate market-level Full E2E + independent multi-hotel qualification in **one run** (founder one-run correction) | Still gated apply · no paid without auth |
| **PDI-5** | Portfolio Weekly Change + commercial outcome feedback (hotel-isolated) | Feedback remains hotel-scoped |

### Release mapping (founder)

1. **First release** = PDI-1..3 — read-only portfolio view + fit matrix  
2. **Second release** = PDI-4 — market Full E2E + multi-hotel qualification  
3. **Third release** = PDI-5 — portfolio Weekly Change + outcomes  

---

## 7. Acceptance criteria

### PDI-0 (this assessment)

- [x] Commercial Priorities tab located; insertion point defined  
- [x] Reuse inventory documented (MOG, cross-hotel fit, drawer, Weekly Change, census)  
- [x] Operator/market/hotel relationship gaps explicit  
- [x] Fit class contract proposed  
- [x] PR sequence + restrictions  
- [ ] Founder approval to start PDI-1  

### First release (PDI-1..3)

- Operator + market selectors show Aimbridge CALA · Guadalajara fixture  
- Matrix shows shared market opportunities × participating Guadalajara hotels (not Cancún/Punta Cana)  
- Fit cells use contract classes; UNKNOWN/INSUFFICIENT_EVIDENCE when attributes missing  
- Opening a cell uses **existing** hotel drawer/brief for that hotelId  
- No cross-operator leakage in API tests  
- Weekly Change filter works when baseline annotations exist; otherwise honest empty  
- Authenticated GDI UI acceptance on local app (not API-only)  
- No paid research / production Airtable writes / publication  

### Later releases

- One market research run qualifies multiple hotels independently  
- Durable persistence + Weekly Change per one-run lifecycle contract  
- Confidential hotel feedback never appears in shared market layer  

---

## 8. STOP

Do **not** begin PDI-1 UI/API implementation until founder approves this assessment.

Do **not** merge Portfolio work into Slice 2A (#444).

Do **not** create Airtable tables or a second intelligence database without a demonstrated gap beyond config/API + existing MOG/GDO.

**AWAITING_FOUNDER_REVIEW**
