# GDI Full-Universe Research Coverage + Next-Cycle Prioritization V1

**Generated:** 2026-10-03T14:21:38.837Z  
**Universe:** 19 Live published ADP hotels (dynamic)  
**Verdict:** **GDI INITIALIZATION WAS MISLABELED AS RESEARCH — STATE MODEL REPAIRED**

---

## Portfolio summary

| Metric | Count |
|---|---:|
| TOTAL ACTIVE HOTELS | 19 |
| CUSTOMER_READY | 4 |
| RESEARCHED_NO_READY | 5 |
| PARTIALLY_RESEARCHED | 0 |
| INITIALIZED_ONLY | 10 |
| UNKNOWN | 0 |

---

## Founder matrix

| Hotel | Fits | Targets | Targets Researched | Coverage % | Fetches | Opps | Maturity | Root Cause | Next Cycle |
|---|---:|---:|---:|---:|---:|---:|---|---|---|
| AC Hotel A Coruña | 7 | 35 | 35 | 100 | 53 | 0 | RESEARCHED_NO_READY | VALID_RESEARCH_NO_READY_OPPORTUNITIES | NEXT_CYCLE_A |
| Bethesda Marriott | 20 | 53 | 50 | 94.3 | 18 | 35 | CUSTOMER_READY | — | NO_ADDITIONAL_RESEARCH_NEEDED_NOW |
| Cambridge Beaches Resort & Spa | 11 | 22 | 22 | 100 | 30 | 0 | RESEARCHED_NO_READY | VALID_RESEARCH_NO_READY_OPPORTUNITIES | DEFER |
| Casas del XVI | 11 | 22 | 0 | 0 | — | 0 | INITIALIZED_ONLY | INITIALIZED_NOT_RESEARCHED | NEXT_CYCLE_B |
| Faranda Collection Bogotá | 11 | 22 | 0 | 0 | — | 0 | INITIALIZED_ONLY | INITIALIZED_NOT_RESEARCHED | NEXT_CYCLE_B |
| Hilton New York Times Square | 11 | 22 | 22 | 100 | 34 | 12 | CUSTOMER_READY | — | DEFER |
| Hotel Caribe by Faranda Grand, a member of Radisson Individuals | 11 | 22 | 0 | 0 | — | 0 | INITIALIZED_ONLY | INITIALIZED_NOT_RESEARCHED | NEXT_CYCLE_B |
| Hotel Phillips Kansas City, Curio Collection by Hilton | 11 | 22 | 0 | 0 | — | 0 | INITIALIZED_ONLY | INITIALIZED_NOT_RESEARCHED | NEXT_CYCLE_A |
| JW Marriott Hotel Monterrey Valle | 11 | 22 | 0 | 0 | — | 0 | INITIALIZED_ONLY | INITIALIZED_NOT_RESEARCHED | NEXT_CYCLE_B |
| JW Marriott Hotel Santo Domingo | 11 | 22 | 0 | 0 | — | 0 | INITIALIZED_ONLY | INITIALIZED_NOT_RESEARCHED | NEXT_CYCLE_B |
| NOW NOW NOHO | 11 | 22 | 22 | 100 | 30 | 0 | RESEARCHED_NO_READY | VALID_RESEARCH_NO_READY_OPPORTUNITIES | DEFER |
| Radisson Hotel Santo Domingo | 11 | 22 | 0 | 0 | — | 0 | INITIALIZED_ONLY | INITIALIZED_NOT_RESEARCHED | NEXT_CYCLE_B |
| Renaissance New York Times Square Hotel | 8 | 16 | 12 | 75 | 12 | 11 | CUSTOMER_READY | — | DEFER |
| Spice Island Beach Resort | 7 | 35 | 35 | 100 | 33 | 0 | RESEARCHED_NO_READY | VALID_RESEARCH_NO_READY_OPPORTUNITIES | NEXT_CYCLE_A |
| The St. Regis Cap Cana Resort | 11 | 22 | 0 | 0 | — | 0 | INITIALIZED_ONLY | INITIALIZED_NOT_RESEARCHED | NEXT_CYCLE_B |
| The St. Regis Mexico City | 11 | 22 | 0 | 0 | — | 0 | INITIALIZED_ONLY | INITIALIZED_NOT_RESEARCHED | NEXT_CYCLE_B |
| The Westin Monterrey Valle | 11 | 22 | 0 | 0 | — | 0 | INITIALIZED_ONLY | INITIALIZED_NOT_RESEARCHED | NEXT_CYCLE_A |
| W Rome | 7 | 19 | 19 | 100 | 39 | 0 | RESEARCHED_NO_READY | VALID_RESEARCH_NO_READY_OPPORTUNITIES | NEXT_CYCLE_A |
| Waterstone Resort & Marina | 11 | 22 | 0 | 0 | — | 15 | CUSTOMER_READY | — | DEFER |

---

## Direct answers

1. **Substantive GDI research:** 9 of 19 hotels.
2. **Initialized-only infrastructure:** 10 hotels.
3. **Are all zero-ready hotels "quality held"?** **No.** Only 5/15 zero-ready hotels have evidence-backed research. The rest are **INITIALIZED_ONLY** — not quality-held.
4. **Zero-ready hotels that exhausted a meaningful research cycle:** AC Hotel A Coruña; Cambridge Beaches Resort & Spa; NOW NOW NOHO; Spice Island Beach Resort; W Rome.
5. **Hotels with never target-level research (ledger):** 11 — Casas del XVI; Faranda Collection Bogotá; Hotel Caribe by Faranda Grand, a member of Radisson Individuals; Hotel Phillips Kansas City, Curio Collection by Hilton; JW Marriott Hotel Monterrey Valle; JW Marriott Hotel Santo Domingo; Radisson Hotel Santo Domingo; The St. Regis Cap Cana Resort; The St. Regis Mexico City; The Westin Monterrey Valle; Waterstone Resort & Marina.
6. **Next 3–5 cycle hotels:** AC Hotel A Coruña; Spice Island Beach Resort; Hotel Phillips Kansas City, Curio Collection by Hilton; The Westin Monterrey Valle; W Rome.
7. **Strategies:** see Next-cycle plan below.
8. **State model conflated init with researched?** **Yes.** Prior universe reconciler set `GDI_RESEARCHED_NO_READY` whenever fits+targets+runs existed (including INIT_FOOTPRINT). Fixed via `research-maturity-v1` + reconciler classifier.
9. **Persistence repair?** Waterstone Resort & Marina — hotel-level cycles without Target Run rows. Do **not** fabricate per-target history; fix write-path going forward; optional Research Run provenance notes only.
10. **Bethesda protected?** **Yes** — classified CUSTOMER_READY / NO_ADDITIONAL_RESEARCH_NEEDED_NOW; no experimental broad discovery.

---

## Bethesda

Protected founding pilot. Preserve existing opportunity set. Next cycle = **NO_ADDITIONAL_RESEARCH_NEEDED_NOW**.

## AC Hotel A Coruña

**Not INITIALIZED_ONLY.** Two real cycles (44 queries / 53 fetches combined). Target-run ledger empty (orchestrator gap) → **PARTIALLY_RESEARCHED** at target grain; hotel-level research proved. Next focus: corporate/industrial, port/maritime, healthcare, deeper university/science, project teams/training — evidence from cycles 1–2 shows academic/cultural/sports overflow dominated and produced 0 ready.

## Spice Island Beach Resort

**Not INITIALIZED_ONLY.** One real cycle (30q / 33f). Same target-ledger gap → **PARTIALLY_RESEARCHED**. Next focus: incentive/executive retreat depth, luxury advisor, wedding planner (planner-side), marine/yachting, Caribbean regional orgs.

---

## Next-cycle plan (bounded — NOT launched)

### 1. AC Hotel A Coruña

- **Why now:** real research exhausted general pass; focused lane gap remains
- **Research gap:** Cycles 1–2 surfaced academic/scientific calendars, cultural festivals, sports overflow housing, and light corporate conferences — 0 customer-ready. Port/maritime logistics employers, industrial programs, healthcare cohorts, and training/project teams were not exhausted as lodging-supported lanes.
- **Lanes:** corporate / industrial; port / maritime; healthcare; university / science (deeper than webinar/calendar noise); project teams / training
- **Expected sources:** port authority / terminal operator pages; regional industrial association calendars; hospital / biomedical program pages; university faculty/event housing pages; training academy schedules
- **Budget:** queries ≤ 24, fetches ≤ 36
- **Success:** >=1 lodging-supported candidate clearing readiness OR documented public-data ceiling per lane with provenance

### 2. Spice Island Beach Resort

- **Why now:** real research exhausted general pass; focused lane gap remains
- **Research gap:** First cycle found MICE tourism conferences, yacht charter mentions, destination wedding pages, corporate retreat/incentive blurbs — mostly INVALID/WATCH, 0 ready. Deeper planner/advisor/yacht operator and regional org paths remain.
- **Lanes:** incentive / executive retreat (deeper); luxury travel advisor networks; destination wedding planner (planner-side, not brochure); marine / yachting charter groups; Caribbean regional organizations
- **Expected sources:** incentive house / DMC pages; luxury advisor consortia; wedding planner portfolios with lodging asks; yacht charter operator group itineraries; Caribbean tourism/association event pages with housing
- **Budget:** queries ≤ 20, fetches ≤ 30
- **Success:** prove or refute lodging-supported incentive/yacht/wedding-planner demand with named path — no brochure filler

### 3. Hotel Phillips Kansas City, Curio Collection by Hilton

- **Why now:** initialized only — stronger HI completeness for first substantive cycle
- **Research gap:** fits/targets/init run exist without substantive target research
- **Lanes:** 
- **Expected sources:** 
- **Budget:** queries ≤ 18, fetches ≤ 28
- **Success:** target-run ledger written; readiness gates applied; no weak promotions

### 4. The Westin Monterrey Valle

- **Why now:** initialized only — high HI/target readiness for first substantive cycle
- **Research gap:** fits/targets/init run exist without substantive target research
- **Lanes:** 
- **Expected sources:** 
- **Budget:** queries ≤ 18, fetches ≤ 28
- **Success:** target-run ledger written; readiness gates applied; no weak promotions

### 5. W Rome

- **Why now:** real research exhausted general pass; focused lane gap remains
- **Research gap:** Target runs present (12) with 0 customer-ready — deepen selected lanes, not broad re-init.
- **Lanes:** association / corporate Rome; fashion / production; Embassy / institutional programs
- **Expected sources:** official program calendars; housing blocks; institutional event pages
- **Budget:** queries ≤ 16, fetches ≤ 24
- **Success:** lane-specific lodging support or documented ceiling

---

## State model fix

Suggested / implemented labels:

- `INITIALIZED`
- `RESEARCH_IN_PROGRESS`
- `RESEARCHED_NO_READY`
- `CUSTOMER_READY`
- `STALE_RESEARCH`
- `RESEARCH_BLOCKED`

Module: `lib/group-demand-intelligence/research-maturity-v1.js`  
Reconciler no longer equates init footprint runs with researched.

---

## STOP

No new research cycles launched. Founder report only.
