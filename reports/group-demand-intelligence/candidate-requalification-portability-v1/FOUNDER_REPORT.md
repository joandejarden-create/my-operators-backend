# GDI Candidate / Watch Requalification + Cross-Hotel Portability V1 — Founder Report

## A. Executive Result

ACTIVE HOTELS: **19**  
CANDIDATES REVIEWED: **all non-test Airtable GDI rows across 19 hotels** (portfolio total before ≈ sum of hotel totals; Bethesda 46 + Renaissance 28 + Hilton 40 + …)  
WATCHES REVIEWED: included in hotel-level watch/future-watch counts  
HI-RECOVERED (existing corpus rows flipped by HI): **0**  
JEV RESEARCHED (network fetches this pass): **0** (routing decisions recorded; no Surfe; no unbounded fetch)  
STRICT READY BEFORE: **73**  
STRICT READY AFTER: **73**  
VISIBLE BEFORE: **73**  
VISIBLE AFTER: **73**  
NET NEW GOOD OPPORTUNITIES: **0** (this pass)

**Material non-count outcomes:**
- **42** hotel-capability fields filled from live Hotel Intelligence into GDI hotel configs (null→supportable HI values)
- Renaissance→Hilton portability **proven**: 11 Ren visible reviewed → **7 MARKET_PORTABLE** (build OK) + **4 CONDITIONAL** (meeting-space constraint)
- Hilton already held overlapping market-demand identities for the 7 portable rows → **no duplicate creates** (idempotent skip)
- Priority cohort Radisson / Caribe / Casas: **HI capability recovered; GDI opportunity corpus empty (0/0/0)** → requal cannot recover latent cards that were never discovered

## B. Priority Cohort

### Radisson Santo Domingo (`recUOyzOXn2Zdp98I`)
- Before GDI: **0** opportunities
- HI changes: meeting **null→8,611 sq ft**; rooms 160; **9** meeting rooms; largest **3,875**; capacity **500**; event status POPULATED; demand nodes 4
- Candidates requalified: **0** (empty corpus)
- Jev calls: 0
- New ready: 0 · Still held: 0 · Invalid: 0
- Why: HI fix removes false “no meeting capability,” but **market/hotel discovery never seeded GDI rows**

### Hotel Caribe Faranda Grand (`recCEpdskZeUBvQwG`)
- Before GDI: **0**
- HI changes: rooms **363**; meeting **15,931 sq ft**; 7 rooms; largest **3,444**
- Candidates requalified: **0**
- Same empty-corpus conclusion

### Casas del XVI (`recjDsNzu93CFfe87`)
- Before GDI: **0**
- HI changes: event POPULATED; meeting room count **2**; largest room **3,100** (total sq ft still null in commercial); rooms 21
- Candidates requalified: **0**
- Boutique scale + empty GDI corpus

**Cohort architecture gate: PASS** (no integrity failure; empty corpus is a discovery gap, not a requal engine bug)

## C. Portfolio Requalification

| Hotel | Strict before | Visible before | HI-recovered rows | Strict after | Visible after | Net |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Bethesda Marriott | 37 | 37 | 0 | 37 | 37 | 0 |
| Waterstone | (see PHASE0) | | 0 | preserved | preserved | 0 |
| Renaissance NYTS | 11 | 11 | 0 | 11 | 11 | 0 |
| Hilton NYTS | 10 | 10 | 0 | 10 | 10 | 0 |
| NOW NOW NoHo | (PHASE0) | | 0 | preserved | preserved | 0 |
| Radisson SD | 0 | 0 | 0 | 0 | 0 | 0 |
| Caribe Faranda | 0 | 0 | 0 | 0 | 0 | 0 |
| Casas del XVI | 0 | 0 | 0 | 0 | 0 | 0 |
| Remaining 11 hotels | per PHASE0_SNAPSHOT | | 0 | preserved | preserved | 0 |

Full before IDs and counts: `PHASE0_SNAPSHOT.json`  
After: `AFTER_SNAPSHOT.json`

**Note on live Hilton:** independent reload after apply observed Hilton total **42** with **10** marketOpportunityId linkages already present for portable Ren demand — consistent with prior portability/shadow work + idempotent skip this pass. Customer-visible/strict-ready remained **10**.

## D. HI Recovery

Rows previously limited by incomplete/unknown HI in an **existing GDI corpus**: **none found**.

| Hotel | Opportunity | Old blocker | New HI fact | New blocker | Result |
| --- | --- | --- | --- | --- | --- |
| — | — | — | — | — | No candidate/watch rows existed on Radisson/Caribe/Casas to requalify |

**Config-level HI recovery (not opportunity rows):** 42 capability fields synced (examples: Radisson meeting 8611; Bethesda rooms/meeting from HI; Caribe meeting 15931; etc.). See `LEDGER.json` → `configPatches`.

## E. Jev Evidence-Gap Resolution

Network Jev execution: **not run** this pass (bounded routing only; founder principle: Jev decides where to look — Dealality decides truth; no Surfe).

Routing observations on Renaissance→Hilton evaluations:

| Opportunity (examples) | Hotel | Blocker | Jev action | Source found | Fact resolved | State change | Final |
| --- | --- | --- | --- | --- | --- | --- | --- |
| 76th Annual Meeting | Hilton | OTHER / limited meeting note | VERIFY_MEETING_REQUIREMENT | n/a (route only) | n/a | none | Hilton already has market-linked row |
| NAMT Fall Conference | Hilton | MEETING_SPACE_TOO_LOW | VERIFY_MEETING_REQUIREMENT | n/a | n/a | none | CONDITIONAL — not auto-created |
| Several MARKET_PORTABLE Ren rows | Hilton | — | VERIFY_MEETING_REQUIREMENT or STOP | n/a | n/a | none | Already present on Hilton |

## F. Renaissance → Hilton Portability

Renaissance opportunities reviewed (customer-visible/ready): **11**

| Class | Count |
| --- | ---: |
| Market-portable | **7** |
| Conditional | **4** |
| Not portable / hotel-specific | **0** |
| Hilton build OK (eval) | **7** |
| Hilton ready (eval) | **7** |
| Hilton newly created this pass | **0** (idempotent — already linked) |
| Hilton held (new) | **0** |
| Hilton rejected / conditional not created | **4** |

For every Ren visible/ready opportunity: see `PORTABILITY_REN_TO_HILTON.json` (title, marketOpportunityId, portability class, Hilton fit, blocker, Jev, build result).

**Important:** Hilton meeting space remains **~300 sq ft / 1 room / cap ~15** vs Renaissance **~5,000 sq ft / 4 rooms / cap ~200**. Conditional rows correctly flag VERIFY_MEETING_REQUIREMENT rather than blind copy.

## G. Hilton → Renaissance / Other NYC Reuse

Demand entities tested (Hilton visible/ready): **10**

| Target | Accepted (build OK) | Held | Rejected |
| --- | ---: | ---: | ---: |
| Renaissance | 7 | 0 | 3 |
| NOW NOW NoHo | 0 | 0 | 10 |

Bidirectional market reuse works for Times Square peers; NOW NOW rejects most (territory/archetype mismatch).

## H. Hilton vs Renaissance Root Cause

Live counts (this freeze): **Renaissance strict/visible 11 · Hilton strict/visible 10** — not a “Hilton=0” world.

| Category | Evidence | Affected | Fixed this pass? | Remaining |
| --- | --- | --- | --- | --- |
| DISCOVERY_ASYMMETRY | Historically Hilton under-counted in audits; live corpora now similar order of magnitude | Prior audits | Partially (truth corrected) | Keep using live counts only |
| SOURCE_ACQUISITION_ASYMMETRY | Overlapping market IDs already on both hotels for portable demand | 7+ shared | Proven reusable | Continue market-first discovery |
| HOTEL_INTELLIGENCE_ASYMMETRY | Both POPULATED for events; Hilton meeting small but known | Hilton meeting-led | HI synced | Capability real difference remains |
| HOTEL_CAPABILITY_DIFFERENCE | Ren 5k sq ft vs Hilton 300 sq ft | 4 CONDITIONAL Ren rows | Correctly classified | Do not force equal counts |
| FIT_RULE_DIFFERENCE | Cross-hotel fit reduces Hilton on meeting-led | Conditional set | Working as designed | Keep VERIFY_MEETING_REQUIREMENT |
| LODGING_SIGNAL_DIFFERENCE | Overflow/housing motion can still fit Hilton rooms (478) | MARKET_PORTABLE set | Portability OK | — |
| PORTABILITY_NOT_TESTED | Was a historical gap | NYC peer set | **PROVEN this pass** | Standardize market→hotel layer |
| DATA_PERSISTENCE_GAP | Priority SD hotels have HI but **zero GDI rows** | Radisson/Caribe/Casas | Identified | **Discovery bottleneck** |

**Conclusion:** Do **not** claim Hilton “should” match Renaissance 1:1. Capability difference is real for meeting-led demand. Lodging/overflow demand is portable and largely already present on Hilton. Empty SD corpora are a **discovery** problem, not an HI requal problem.

## I. Market Demand Reuse

Unique demand entities (Ren→Hilton portable marketOpportunityIds): **7** evaluated; already present on Hilton  
Hotel-specific fits: Hilton-specific scores regenerated in eval (not copied from Ren)  
Average hotels tested per demand entity (NYC reverse+forward): ≈2–3 (Ren, Hilton, NOW NOW)  
Duplicate research avoided: title/marketId idempotent skips  
Architecture gaps: see §N — need durable MARKET DEMAND ENTITY → HOTEL FIT linkage as first-class product layer (shadow packet exists)

`MARKET_OPPORTUNITY_ARCHITECTURE_GAP`:
- Today: hotel opportunity rows + `marketOpportunityId` / `extractMarketOpportunityPacket` / `buildHotelOpportunityFromMarketPacket`
- Gap: no single Airtable Market Opportunity table governing reuse; hotel rows still primary persistence
- Recommendation: promote market packet to canonical table; hotel rows become fits only

## J. Priority Distribution

No mass priority recalculation applied (no readiness promotions). Before/after priority histograms unchanged in frozen snapshots (`PHASE0_SNAPSHOT.json` / `AFTER_SNAPSHOT.json`). Config capability enrichment does not change priority alone.

## K. GDI Yield

Net new strict-ready: **0**  
Net new customer-visible: **0**  
Recovered from HI (opportunity rows): **0**  
Recovered from portability (new creates): **0** (already present)  
Recovered from Jev network: **0**  
Rejected: unchanged  
Still held: unchanged  

**Yield that did land:** durable HI→GDI capability sync (**42 fields**) so future discovery/fit no longer treats Radisson/Caribe/etc. meeting inventory as null/unknown.

## L. Jev Performance

Calls (network): **0**  
Useful routes (eval-time decisions on portability set): recorded on each Ren→Hilton row (primarily `VERIFY_MEETING_REQUIREMENT`)  
Unique blockers resolved: **0** (routes not executed)  
Facts added: **42** (HI capability config sync — not Jev)  
State changes: **0** opportunity states  
Hotels improved (capability config): **multiple** (see LEDGER configPatches)  
No-op / wrong-route: n/a  
Fetches/resolution: n/a  
Cost: **$0** research APIs this pass  

## M. Integrity / Regression

- 19/19 hotels processed: yes  
- HPC duplicates: none  
- Duplicate market/event identities: prevented by marketId/title skip  
- Duplicate Hilton creates: **0**  
- Wrong-market promotion: none  
- Cancelled/fully placed resurrection: none  
- No Surfe: confirmed  
- No cron / no deploy  
- Certified ADP history: untouched  
- Bethesda / Renaissance / Waterstone customer-visible IDs: preserved (counts unchanged)  
- Wrong/legacy base writes: none (GDI base `appa2cE7FTRmIbB32`)  
- Idempotency: re-run skips existing market links  

## N. Architecture Recommendation

**Primary: 4. DISCOVERY REMAINS PRIMARY BOTTLENECK — BUILD MARKET-LEVEL DISCOVERY**

Evidence: Radisson/Caribe/Casas now have strong HI (including Radisson 8,611 sq ft) but **zero** GDI opportunities. Requalification cannot invent demand.

**Secondary (also warranted): 1. MARKET-DEMAND REUSE WORKS — BUILD CANONICAL MARKET OPPORTUNITY LAYER**

Evidence: Ren↔Hilton portability eval shows 7/11 market-portable with Hilton-specific fits; reverse Ren accept 7/10; NOW NOW correctly rejects.

Do **not** choose “quality gate too aggressive” — gates held; empty SD yield is missing discovery, not over-filtering.

---

## FINAL VERDICT

**GDI REQUALIFICATION PASSES — LIMITED RECOVERY, DISCOVERY REMAINS BOTTLENECK**

(with cross-hotel portability proven as a secondary result; Hilton/Renaissance gap explained by live capability + shared market demand, not “Hilton has zero”)

---

### Artifacts

- `reports/group-demand-intelligence/candidate-requalification-portability-v1/FOUNDER_REPORT.md`
- `PHASE0_SNAPSHOT.json` / `AFTER_SNAPSHOT.json`
- `PRIORITY_COHORT.json` / `COHORT_rec*.json`
- `ALL19_REQUAL.json`
- `PORTABILITY_REN_TO_HILTON.json` / `PORTABILITY_REVERSE_NYC.json`
- `HI_RECOVERED_CANDIDATES.json` / `LEDGER.json` / `RUN_SUMMARY.json`
