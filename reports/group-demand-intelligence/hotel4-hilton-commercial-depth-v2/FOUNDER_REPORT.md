# Hilton Times Square — GDI Commercial Depth V2 Founder Report

**Hotel:** Hilton New York Times Square (`rec35fExUxCClpOP6`)  
**Date:** 2026-09-27  
**Version:** `gdi_commercial_depth_v2`

---

## A. BEFORE

| Metric | Value |
|---|---:|
| VISIBLE | 15 (16 in bag; 1 geo-DQ LA Olympics hidden) |
| ACTIONABLE | 0 |
| WATCH | 15 |
| OVERFLOW | 1 (type tag) |
| NAMED_DIRECT | 0 |
| NAMED_PARTIAL | 7 |
| FUNCTIONAL | 0 |
| ORG_PATH | 8–9 |
| NO_CONTACT | 0 |

## B. DRAWER COVERAGE BEFORE

| Field | Coverage |
|---|---:|
| Dates | 93.3% |
| Venue status | ~0–20% meaningful |
| Room demand | 46.7% |
| Attendance | 33.3% |
| Peak rooms | 33.3% |
| Historical housing | 0% |
| Thesis | 100% (often thin) |
| Why Hotel | 100% (often thin) |
| Why Now | 100% (often year-generic before rewrite) |
| Contact | 100% path present |
| Source | 100% |
| Action | 100% (often boilerplate before rewrite) |

## C. CONTACT RESEARCH

| Metric | Value |
|---|---:|
| RESEARCHED | **16** |
| NAMED_DIRECT AFTER | **0** |
| NAMED_PARTIAL AFTER | **7** |
| FUNCTIONAL AFTER | **0** |
| ORG_PATH AFTER | **8** |
| NO_CONTACT AFTER | **0** |
| NEW NAMED PEOPLE | **0** net new (grades unchanged) |

Public-data ceiling: many “NAMED_PARTIAL” values are org/brand strings (e.g. “Colonial Dames”, “Survey Software”), not validated decision-makers — treat as **weak WHO**, not sales-ready NAMED_DIRECT.

## D. NAMED CONTACTS (as stored — quality caveats)

| Opportunity | Person | Role | Grade | Notes |
|---|---|---|---|---|
| CDA Annual Meeting 2026 | Colonial Dames | — | B | Org name as person — reject for sales |
| NY Forum Economic Sanctions | Learning Futures New York | truncated | B | Org / program bleed |
| Holiday Celebration Offer | Survey Software | Event Venues | B | Likely page noise |
| FIFA World Cup 2026 | Enquire Beyond | — | B | Unvalidated |
| Scale Healthcare Leadership | David Kolb | Vice President | B | Best candidate; role not housing-specific |
| Corporate Meetings Midtown | Climate Monitoring | — | B | Noise |
| IJCAI-ECAI 2026 | Diego Calvanese Free | — | B | Academic name; housing role unclear |

## E. CONTACT REJECTIONS

| Candidate | Opportunity | Reason |
|---|---|---|
| Colonial Dames | CDA | Organization, not person |
| Survey Software | Holiday Celebration | Parser/page noise |
| Climate Monitoring | Corporate Meetings | Not a person |
| Enquire Beyond | FIFA | Unvalidated brand entity |

## F. LODGING EVIDENCE

| Metric | Count |
|---|---:|
| HOUSING PAGE FOUND | **9** |
| ROOM BLOCK EVIDENCE | **9** |
| OVERFLOW EVIDENCE | **6** |
| ATTENDANCE KNOWN (confirmed this pass) | **0** |
| PEAK ROOMS KNOWN (confirmed this pass) | **0** |
| UNKNOWN_AFTER_RESEARCH (attendance/peak) | **16** |

## G. QUALIFICATION MOVEMENT

| Transition | Count |
|---|---:|
| WATCH → ACTIONABLE | **0** |
| WATCH → HOUSING / lodging-primary states | motions remapped (see below) |
| WATCH → FUTURE_WATCH | 5 |
| WATCH → DQ | 0 new (LA Olympics already DQ) |
| UNCHANGED WATCH | majority remain non-ACTIONABLE |

**Lodging-primary motions after restamp:** ASSOCIATION_HOUSING 4 · CORPORATE_BLOCK 4 · SPORTS_HOUSING 4 · SOCIAL_HOUSING 2 · OVERFLOW 1

## H. ACTIONABLE_NOW

**TOTAL: 0**

Why none cleared the strict gate (honest):
- No open housing channel + usable validated person + Midtown partner timing together
- Several rows are promo / venue-list / mega-event watches without hotel-specific housing portals
- Contact WHO quality insufficient for sales ACTIONABLE even where room-block language exists
- Did **not** lower CQ to force ACTIONABLE

## I. DRAWER COVERAGE AFTER

| Field | Coverage | Unknown After Research |
|---|---:|---|
| Venue status | **100%** | 0 blank |
| Room demand | **~53%+** classified | remainder UNKNOWN_AFTER_RESEARCH / HOUSING_PENDING |
| Thesis (hotel-specific) | **100%** | — |
| Why Hotel (478 / Times Square) | **100%** | — |
| Why Now (timing language) | **100%** | monitor vs housing-language triggers |
| Action (specific) | **100%** | — |
| Attendance / peak confirmed | **0% this pass** | UNKNOWN_AFTER_RESEARCH |

## J. DRAWER QA

| Check | Result |
|---|---|
| 16/16 researched | **YES** |
| THIN after restamp | **0** expected for thesis/why/action |
| GENERIC THESIS | **0** (rewritten lodging-primary) |
| GENERIC WHY NOW | **0** year-only forms replaced |
| INTERNAL ID LEAK | **0** |
| UNEXPLAINED BLANKS | venue previously blank → filled |

## K. JEV

| Metric | Value |
|---|---:|
| CALLS | **91** |
| SAFE APPLY | **4** |
| HELPFUL_DIFFERENT | **0** |
| SAME | **74** |
| WRONG | **0** |
| HIGH_CONF_WRONG | **0** |
| UNKNOWN | **17** |

Playbook decisions mostly agreed with `LODGING_HOUSING` default (correct for this hotel) → few differential applies.

## L. JEV IMPACT

| Metric | Value |
|---|---|
| CONTACT UPGRADES | **0** |
| ACTIONABILITY UPGRADES | **0** |
| FETCHES SAVED | not evidenced |
| DEFER/STOP SAVINGS | limited |
| MATERIAL VALUE | **LIMITED** |

Safe apply remains enabled (no harm). Person selection stays advisory (**NO** person apply).

## M. ADP SCENARIO PARITY

| Field | Value |
|---|---|
| CURRENT LIVE BASELINE | **50** |
| AFTER GENERIC FIX (not re-run) | **65** (50 standard + 15 capability) |
| CORE / ARCHETYPE / MARKET (parity pack) | 16 / 16 / 16 |
| CAPABILITY | **15** (was missing) |
| EXPECTED | **~63–65** |
| ROOT CAUSE | `nyc_times_square` market pack returned 50; capability layer skipped because `combined.length > 0` |
| CLASSIFICATION | **MARKET_LAYER_PRESENT_BUT_CAPABILITY_LAYER_THIN** |
| DEFECT | **YES** (generic; fixed in `scenario-registry.js`) |
| RERUN REQUIRED | **YES** (fresh official baseline) |
| ESTIMATED COST | **~$8.45** (65 × 4 providers) |

## N. DURABILITY

| Check | Result |
|---|---|
| SURVIVES RESTART | **PASS** (Airtable) |
| SURVIVES CLEAN PROCESS | **PASS** (cache invalidate + reload) |
| LOCAL-ONLY DEPENDENCY | **NO** |

## O. CROSS-HOTEL

Isolation suite assertions passed (token / hotelId / bleed). Share revoke FS flake on Windows is environmental, not a data leak.

| Hotel | Result |
|---|---|
| BETHESDA | PASS (no bleed) |
| RENAISSANCE | PASS |
| W ROME | PASS |
| HILTON | PASS |
| LEAKS | **0** |

## P. DECISION

1. All 16 deeply re-researched? **YES**  
2. Named relevant people? **0 sales-validated NAMED_DIRECT; 7 NAMED_PARTIAL with weak WHO quality**  
3. ORG_PATH only? **8**  
4. ORG_PATH = true public ceiling? **Mostly YES for this corpus**  
5. Lodging evidence improved? **YES** (9 housing / 9 room-block / 6 overflow)  
6. Room-demand completeness improved? **YES** (classification + evidence flags)  
7. Attendee/peak improved? **NO confirmed extractions this pass**  
8. Why This Hotel hotel-specific? **YES** (478 / Times Square / lodging motion)  
9. Why Now timing-specific? **YES** (monitor vs housing-language triggers)  
10. Actions contact-specific where possible? **PARTIAL** (templates; WHO often weak)  
11. Any ACTIONABLE_NOW? **NO**  
12. Why? Strict gate unmet — no open Midtown housing partner channel + validated person + timing together; CQ not lowered  
13. Lodging-primary optimized? **YES** (motions remapped; no forced FULL_MEETING_RFP)  
14. Jev improve routing? **LIMITED** (mostly SAME; 4 safe applies; 0 HELPFUL_DIFFERENT)  
15. Jev harmful? **NO**  
16. Keep safe apply enabled? **YES**  
17. Person selection advisory? **YES**  
18. Why 50 scenarios? Market pack only; capability layer omitted  
19. 50 methodologically valid vs mature bar? **NO** — should be ~65  
20. ADP fresh rerun? **YES — recommend** (~$8.45)  
21. Drawers = Bethesda commercial depth? **CLOSER — not equal** (lodging/thesis improved; contacts/ACTIONABLE still behind)  
22. Largest remaining gap? **Validated housing-owner WHO + open housing portals that clear ACTIONABLE_NOW without inventing demand**

## Q. FINAL VERDICT

**HILTON GDI LODGING-PRIMARY LOGIC IMPROVED — ONE MORE DISCOVERY CYCLE NEEDED**

Also: **ADP scenario defect found and fixed in code — official baseline rerun required** (do not treat live 50-scenario period as parity-complete).

---

## PERSISTENCE / GENERALIZATION

| Field | Value |
|---|---|
| CODE | `lib/group-demand-intelligence/commercial-depth-v2.js`, `scripts/gdi-hilton-ts-commercial-depth-v2.mjs`, `scripts/test-gdi-commercial-depth-v2.mjs`, `lib/ai-demand-positioning/prompt-universe/scenario-registry.js` |
| TESTS | `test-gdi-commercial-depth-v2.mjs` |
| HILTON PROD HARDCODES | **0** |
| SURFE AUTO | **0** |
| SURFE PERSISTED PII | **0** |
| WEBHOUND REQUIRED | **0** |
| JEV PERSON APPLY | **NO** |
