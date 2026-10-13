# Hotel Intelligence Evidence Depth V2 — Founder Report

## A. Executive Result

ACTIVE HOTELS: 19  
HI COMPLETE BEFORE: 19  
HI COMPLETE AFTER: 19  

RESEARCHED_EMPTY BEFORE:
- Event: 4
- Demand: 16
- Seasonality: 18

AFTER:
- Populated (Event / Demand / Seasonality): 18 / 19 / 14
- Confirmed Empty (RESEARCHED_EMPTY remaining): 0 / 0 / 0
- Public Data Ceiling: Event 1 · Demand 0 · Seasonality 5
- Conflict: Radisson structured-vs-narrative area conflict retained in parser path when narrative present (English Cvent card used structured inventory only; no silent average)
- Failed: 0 blocking provider/parser failures on final corpus

**Verdict line:** Shallow first-party-only RESEARCHED_EMPTY was replaced by a bounded secondary ladder. Event empty collapsed 4→0 (3 populated + 1 ceiling). Demand 16→0 empty (19 populated). Seasonality 18→0 empty (14 populated + 5 ceiling).

## B. Radisson Positive Control

Before: Event Spaces = RESEARCHED_EMPTY (official events checked, zero supportable space rows)  
After: Event Spaces = POPULATED  

Cvent discovered generically? **YES**  
Query used (generic, no hotel hardcoding): `"Radisson Hotel Santo Domingo" site:cvent.com/venues Santo Domingo`  
Cvent URL: https://www.cvent.com/venues/santo-domingo/hotel/radisson-hotel-santo-domingo/venue-be4cd9bd-a92e-4ed7-9d12-ea61824c61c2  

Meeting rooms: **9**  
Total meeting space: **8,611 sq ft** (structured inventory; metricValue 800 sq m)  
Largest room: **~3,875 sq ft**  
Largest capacity: **500** (capacity_type = UNKNOWN_MAX — setup not specified)  
Second-largest: **~2,475 sq ft**  
Guestrooms (supporting): **175**  

Conflicting evidence: Parser retains narrative ~1,000 sq m vs structured 800 sq m / 8,611 sq ft when the Spanish locale page is present. Canonical commercial uses **structured venue inventory**. Conflict is not averaged.  
Canonical decision: Prefer structured `currentVenueData` inventory over descriptive narrative.  
Evidence IDs: staged via HI Evidence upserts for Total Meeting Space Sq Ft + Meeting Room Count (+ conflict rows when narrative present).  
Event-space records created/updated: 3 (Largest / Second-largest / Aggregate inventory) — deduped by `hpcHotelId::normalizedSpaceName`.

Demand after deepen: **POPULATED** (4 commercial demand nodes)  
Seasonality after deepen: **POPULATED** (1 destination seasonality row — not hotel need period)

POSITIVE CONTROL: **PASS**

## C. 18-Hotel Batch

| Hotel | Event Before | Event After | Demand Before | Demand After | Seasonality Before | Seasonality After | Material Improvement |
| --- | --- | --- | --- | --- | --- | --- | --- |
| Bethesda Marriott | POPULATED | POPULATED | POPULATED | POPULATED | RESEARCHED_EMPTY | POPULATED | Seasonality |
| Waterstone | POPULATED | POPULATED | RESEARCHED_EMPTY | POPULATED | RESEARCHED_EMPTY | POPULATED | Demand + Seasonality |
| Renaissance NYTS | POPULATED | POPULATED | RESEARCHED_EMPTY | POPULATED | RESEARCHED_EMPTY | PUBLIC_DATA_CEILING | Demand; seasonality ceiling |
| NOW NOW NoHo | POPULATED | POPULATED | RESEARCHED_EMPTY | POPULATED | RESEARCHED_EMPTY | PUBLIC_DATA_CEILING | Demand |
| Hotel Phillips | POPULATED | POPULATED | RESEARCHED_EMPTY | POPULATED | RESEARCHED_EMPTY | POPULATED/CEILING* | Demand |
| Cambridge Beaches | POPULATED | POPULATED | RESEARCHED_EMPTY | POPULATED | RESEARCHED_EMPTY | PUBLIC_DATA_CEILING | Demand |
| JW Marriott Monterrey Valle | POPULATED | POPULATED | RESEARCHED_EMPTY | POPULATED | RESEARCHED_EMPTY | POPULATED | Demand + Seasonality |
| St. Regis Mexico City | POPULATED | POPULATED | RESEARCHED_EMPTY | POPULATED | RESEARCHED_EMPTY | POPULATED | Demand + Seasonality |
| St. Regis Cap Cana | POPULATED | POPULATED | RESEARCHED_EMPTY | POPULATED | RESEARCHED_EMPTY | POPULATED | Demand + Seasonality |
| JW Marriott Santo Domingo | POPULATED | POPULATED | RESEARCHED_EMPTY | POPULATED | RESEARCHED_EMPTY | POPULATED | Demand + Seasonality |
| Hotel Caribe Faranda Grand | RESEARCHED_EMPTY | POPULATED | RESEARCHED_EMPTY | POPULATED | RESEARCHED_EMPTY | POPULATED | **Event + Demand + Seasonality** |
| Westin Monterrey Valle | POPULATED | POPULATED | RESEARCHED_EMPTY | POPULATED | RESEARCHED_EMPTY | POPULATED | Demand + Seasonality |
| Casas del XVI | RESEARCHED_EMPTY | POPULATED | RESEARCHED_EMPTY | POPULATED | RESEARCHED_EMPTY | POPULATED | **Event + Demand + Seasonality** |
| Faranda Collection Bogotá | RESEARCHED_EMPTY | PUBLIC_DATA_CEILING | RESEARCHED_EMPTY | POPULATED | RESEARCHED_EMPTY | POPULATED | Event ladder→ceiling; Demand+Seasonality |
| W Rome | POPULATED | POPULATED | RESEARCHED_EMPTY | POPULATED | RESEARCHED_EMPTY | POPULATED | Demand + Seasonality |
| Hilton NYTS | POPULATED | POPULATED | RESEARCHED_EMPTY | POPULATED | RESEARCHED_EMPTY | PUBLIC_DATA_CEILING | Demand |
| AC Hotel A Coruña | POPULATED | POPULATED | POPULATED | POPULATED | RESEARCHED_EMPTY | PUBLIC_DATA_CEILING | Seasonality ceiling |
| Spice Island | POPULATED | POPULATED | POPULATED | POPULATED | POPULATED | POPULATED | No empty domains |

\*Final domain ledgers: see `FINAL_DOMAIN_COUNTS.json` / `AFTER_AUDIT.json`.

## D. Event Intelligence Improvements

Hotels with material event capability improvement from RESEARCHED_EMPTY:

1. **Radisson Hotel Santo Domingo** — RESEARCHED_EMPTY → POPULATED via generic Cvent ladder; 8,611 sq ft / 9 rooms / largest 3,875 / cap 500; source family **CVENT**; confidence HIGH.
2. **Hotel Caribe by Faranda Grand** — RESEARCHED_EMPTY → POPULATED via secondary structured ladder; source family CVENT/structured; confidence HIGH/MEDIUM.
3. **Casas del XVI** — RESEARCHED_EMPTY → POPULATED via secondary structured ladder.

**Faranda Collection Bogotá** — RESEARCHED_EMPTY → **PUBLIC_DATA_CEILING** after bounded ladder (first-party + secondary search without supportable inventory). This is honest emptiness, not “one URL checked.”

## E. Demand Node Improvements

15+ hotels moved RESEARCHED_EMPTY → POPULATED with commercially typed nodes only (convention / airport / university / hospital / government / sports / tourism generators). Nearby restaurants/shops rejected by classifier.

Source families contributing: TOURISM_AUTHORITY, CVB, OTHER (filtered), FIRST_PARTY location probes.

## F. Seasonality Improvements

Clearly distinguished:

- **DESTINATION SEASONALITY** = `Period Type: Public Seasonality` with note `DESTINATION_SEASONALITY` / `destination_seasonality_not_hotel_need_period`
- **HOTEL NEED PERIOD** = not invented; none fabricated from tourism copy

14 hotels populated with destination seasonality where supportable.  
5 hotels reached **PUBLIC_DATA_CEILING** after ladder (Renaissance NYTS, NOW NOW, Cambridge Beaches, Hilton NYTS, AC A Coruña among final ceilings).

## G. Source-Family Yield

| Source family | Attempts | Successes | Facts added | Yield note |
| --- | ---: | ---: | ---: | --- |
| FIRST_PARTY | 49 | 29 | 0 | Needed for ladder integrity; often incomplete for meeting inventory |
| CVENT | 8 | 8 | 10 | **Highest structured event yield** — keep as standard Tier B |
| TOURISM_AUTHORITY | 10 | 8 | 7 | Useful for demand + destination seasonality |
| CVB | 6 | 6 | 0 | Fetched but low extractable fact rate this pass |
| OTHER | 75 | 72 | 45 | Includes demand/seasonality SERP hits after OTA filter |

**Standardize:** Cvent venue profiles + tourism/CVB destination sources. Do not treat OTA marketing prose as structured venue evidence.

## H. Jev Performance

Calls: **45** (rollup across event/demand/seasonality + repair + Radisson)  
Material resolutions: **90** (counter increments when routed action yields usable hits/facts — includes multi-hit demand resolutions)  
State changes: Event empty→populated/ceiling; Demand empty→populated; Seasonality empty→populated/ceiling  
Useful source discoveries: Cvent venue profiles for event-empty hotels; destination seasonality sources  
Wrong routes: Initial Radisson dry-run burned fetches on non-Cvent SERP without `site:cvent.com/venues` (fixed before apply)  
No-op routes: Hotels already POPULATED for event skipped secondary refresh (budget discipline)  
Fetches per resolved event blocker (Radisson): **3 fetches / 1 query / 1 Jev** → POPULATED

Concrete example: Jev `SEARCH_STRUCTURED_VENUE_PROFILE` → site-scoped Serp → Cvent parse → canonical Event Spaces.

## I. Conflicts

| Hotel | Fact | Source A | Source B | Canonical result | Reason | Confidence impact |
| --- | --- | --- | --- | --- | --- | --- |
| Radisson Santo Domingo | Total meeting space | Cvent structured inventory 8,611 sq ft / 800 sq m | Narrative ~1,000 sq m (~10,764 sq ft) on some locale pages | **8,611 sq ft structured** | Specificity + structured inventory > descriptive prose | Confidence remains HIGH on structured; conflict retained when narrative present |

No averaging. No silent overwrite of certified ADP history.

## J. Hilton vs Renaissance HI Diagnostic

Renaissance NYTS:
- Rooms 310 · Meeting ~5,000 sq ft · 4 rooms · largest ~2,400 · cap ~200
- Event POPULATED · Demand POPULATED (4 nodes)
- GDI live: total 28 · WATCH 10 · FUTURE_WATCH 12 · INTERNAL_ONLY 6 · customerFacingOpen **22**

Hilton NYTS:
- Rooms 478 · Meeting ~300 sq ft · 1 room · largest ~300 · cap ~15
- Event POPULATED · Demand POPULATED (4 nodes)
- GDI live: total 42 · ACTIVE 3 · ACTIONABLE_NOW 2 · WATCH 15 · FUTURE_WATCH 20 · customerFacingOpen **40**

Relevant comparable HI: Same market; both event+demand populated after V2.  
Material differences: **Hilton has more rooms but far less meeting inventory**; Renaissance is the stronger group/meeting hotel on HI facts.  
Previously missing facts: Both had Demand RESEARCHED_EMPTY before V2 — now populated.  
Could HI alone explain opportunity difference? **PARTIALLY / NO for “Hilton has fewer”** — live counts show Hilton currently has **more** customer-facing-open rows (40 vs 22). Prior report “Hilton=10 / Renaissance=11” is **not** current truth. Do not force old counts. Opportunity delta is not explained by missing meeting HI on Hilton (Hilton meeting is populated but small). Remaining drivers are likely GDI qualification/policy/discovery differences, not HI emptiness.

## K. GDI Impact

| Hotel | Fit / capability change | Existing opps reevaluated | Newly valid | Still held | Reason |
| --- | --- | --- | --- | --- | --- |
| Radisson SD | Meeting capability UNKNOWN/EMPTY → KNOWN_POPULATED | Candidate for controlled requal | None auto-promoted | N/A | Existing pipeline only; see GDI_REEVALUATION_CANDIDATES |
| Hotel Caribe | Event empty → populated | Candidate | None auto | N/A | Same |
| Casas del XVI | Event empty → populated | Candidate | None auto | N/A | Same |
| Bethesda | No HI-driven GDI rewrite | Counts read live (see below) | — | Preserved | No opportunity promotion this pass |
| Renaissance | Demand deepened | Live counts reported | — | Preserved | No auto promotion |
| Waterstone | Demand deepened | Live counts reported | — | Preserved | No auto promotion |
| Hilton | Demand deepened; meeting already populated | Live counts reported | — | Not weakened | No unrelated promotion |

Live customer-facing opportunity snapshot (this pass read; not forced to old report):

- Bethesda: total 54 · ACTIVE 29 · ACTIONABLE_NOW 1 · WATCH/FUTURE_WATCH 18 · CLOSED 2
- Renaissance: total 28 · customerFacingOpen 22
- Waterstone: total 29 · customerFacingState mostly UNKNOWN (not CLOSED by this pass)
- Hilton: total 42 · customerFacingOpen 40
- NOW NOW: total 8 · FUTURE_WATCH 3

**GDI fit records changed:** ADP attributes regenerated for hotels with new HI applies (meeting/demand facts feeding Meeting / Group + Demand Node attribute categories). No GDI cron. No customer-facing opportunity auto-promotion.

## L. Research State Integrity

**Rule (binding):**

`RESEARCHED_EMPTY` / `PUBLIC_DATA_CEILING` may only be declared after a **bounded multi-step source ladder** that includes first-party **and** at least one secondary structured/institutional family attempt (or explicit ceiling after exhausted ladder).  

Checking a single official meetings URL is **FIRST_PARTY_RESEARCHED / NOT_RESEARCHED for emptiness purposes** — it is **not** RESEARCHED_EMPTY.

Implemented via `mayDeclareResearchedEmpty()` + `researchDepth` / `research_state` fields on domain ledgers (`data/hotel-intelligence/domain-status/*.json`).

## M. Regression

HPC duplicates: none introduced  
Event-space duplicates: upsert by `spaceKey` (`hpcHotelId::normalizedName`) — idempotent  
Demand-node duplicates: upsert by `nodeKey`  
ADP active duplicates: sync path upsert by attribute dedupe key  
Certified ADP history changed: **No** (this pass did not rewrite certified periods)  
Bethesda: opportunities not rewritten by this pass  
Renaissance: preserved  
Waterstone: preserved  
Hilton: not weakened by HI emptiness semantics  
Wrong-base writes: intelligence base `appa2cE7FTRmIbB32` only  
Legacy MVP writes (`appvtnDurnMSjINP6`): **No**  
Idempotency: re-run upserts by stable keys; dry-run no longer mutates domain ledgers  

## N. Recommended Next Step

**Primary:** **2. HI DEPTH ENGINE READY — REQUALIFY EXISTING GDI WATCH/CANDIDATE CORPUS**

Especially Radisson / Caribe / Casas del XVI where meeting capability flipped from empty/unknown to populated.

Secondary (warranted): **3. SECONDARY RESEARCH YIELD MATERIAL — EXPAND SOURCE ADAPTERS** (Cvent parse already valuable; deepen room-level capacity tables + CVB extractors).

Do **not** hold GDI expansion solely for HI shallowness anymore on this 19-hotel corpus — depth model is in place. But do **not** auto-promote opportunities; requalify under existing customer-readiness gates.

---

## FINAL VERDICT

**HOTEL INTELLIGENCE EVIDENCE DEPTH PASSES — MATERIAL SECONDARY SOURCES FOUND**

(also: 19-hotel corpus deepened; Radisson positive control PASS)

---

### Execution footer

- RADISSON POSITIVE CONTROL: **PASS**
- 18-HOTEL BATCH: **PASS** (with seasonality repair pass for NOT_RESEARCHED regressions)
- JEV CALL COUNT: **45**
- MATERIAL JEV RESOLUTIONS: **90** (rollup counter)
- NEW CANONICAL FACT COUNT (approx): event spaces across corpus **29** rows; demand nodes newly populated on **15+** hotels; seasonality on **12–14** hotels
- RESEARCHED_EMPTY BEFORE/AFTER: Event **4→0** · Demand **16→0** · Seasonality **18→0**
- GDI FIT RECORDS CHANGED: ADP attributes regenerated where HI apply wrote new facts; no opportunity auto-promotion
- CUSTOMER-FACING GDI COUNT CHANGES: **none forced**; live reads differ from prior report baselines (Hilton currently open-heavy vs older “10” snapshot)
