# Hotel-specific ADP research plans (plan-only)

**Status:** Preparation — not executed.  
**Method:** Existing ADP scenario taxonomy + verified profile facts. Marketing leads for blocked hotels stay leads.

Each plan lists: persona/trip purpose · geo intent · constraints · query family posture · branded vs unbranded · providers · market/language · sampling/cost. Stable core kept; hotel-specific adds justified only.

---

## 1. Renaissance New York Times Square — READY

**ADP ID:** `adp_renaissance_times_square` · **HPC:** `recG66DQJKP2c0UNh`  
**Prior:** CERTIFIED period 2026-10-05 · demand capture 40% · 4 providers  
**Planned builder universe:** ~80 scenarios · est. **~$10.40** (+ retry reserve in budget)  
**Run type when activated:** Refresh/remeasure via `run-ai-demand-positioning.mjs` (or dedicated baseline module if certification requires it) — preserve prior reports.

### Applicable scenarios (do not force all equally)

| Intent | Applicability | Notes |
|--------|---------------|-------|
| business | High | Midtown access; modest meeting inventory |
| leisure | High | Times Square / theater |
| couples | Medium | Urban lifestyle / R Lounge — not resort spa |
| family | Low–medium | Possible; historically weak capture — investigate, don’t invent |
| group_meeting | Medium | ~5k sq ft — small meetings, not Javits-scale primary |
| celebration | Medium | Theater / city celebration framing |
| wellness / adventure | Low | Only if property-specific pack already includes; don’t invent ski/hike |

### Query posture

- **Unbranded core:** Midtown / Times Square business & leisure hotel recommendations.  
- **Branded probes:** Separate factual-accuracy / prompted-mention family (NYCRN / full name) — never score as unprompted win.  
- **Identity guard:** Exclude Renaissance Midtown/Chelsea/Harlem/Downtown confusions.  
- **Comps:** Declared Times Square set for narrative; AI-observed persisted separately.

### Stopping

Priority intents covered to sampling policy; cap exhausted; provider failures logged as failures; do not pad.

---

## 2. Falls Church Marriott Fairview Park (WASFP) — BLOCKED_IDENTITY

**Proposed ADP slug (future):** `adp_falls_church_marriott_fairview_park`  
**Blocker:** HPC VERIFIED_ABSENT → no profile/census link yet.

### Plan once identity + profile exist

| Intent | Applicability (expected) |
|--------|--------------------------|
| business | High — Fairfax/Tysons/Falls Church corporate catchment |
| group_meeting | Medium–high — meeting inventory leads ~27k (verify) |
| leisure | Medium — DC metro leisure when lodging in VA suburbs |
| healthcare / academic visits | Medium — regional hospitals/universities as **geo intent**, not invented amenities |
| airport convenience | Low — neighborhood shuttle ≠ DCA/IAD airport product |

**Do not:** treat Mosaic/Metro shuttle as airport shuttle; force beach/resort personas.

**Est. cost provisional:** ~$8.50–10.50 after scenario freeze.

---

## 3. Renaissance Arlington Capital View (WASPY) — BLOCKED_IDENTITY

**Proposed slug:** `adp_renaissance_arlington_capital_view`  
**Caution:** Connected **Residence Inn** is a different hotel — separate identity, aliases, and entity registry.

### Plan once ready

| Intent | Applicability |
|--------|---------------|
| business | High — Crystal City / Pentagon / Amazon HQ2 corridor |
| group_meeting | Medium–high — ~17k meeting claim (verify) |
| leisure | Medium — DC visitor lodging in Arlington |
| airport convenience | Medium — DCA shuttle claims (verify; not gate access) |

Shared DMV geography facts with Falls Church may reduce duplicate **market** research narrative, but **hotel-fit scoring stays independent**.

---

## 4. Charlotte Marriott City Center (CLTCC) — BLOCKED_IDENTITY

**Proposed slug:** `adp_charlotte_marriott_city_center`

### Plan once ready

| Intent | Applicability |
|--------|---------------|
| business | High — uptown Charlotte |
| group_meeting | High — convention-adjacent claims (verify connection/walking) |
| leisure / celebration | Medium |
| sports / entertainment | Medium — venue-related travel when evidenced |
| airport convenience | Low–medium — CLT distance claim only if verified; not “airport hotel” product |

Resolve conflicting meeting sq ft and M Beta designation **before** factual-accuracy scoring treats them as truth.

---

## 5. Tampa Airport Marriott (TPAAP) — BLOCKED_IDENTITY

**Proposed slug:** `adp_tampa_airport_marriott`  
**Caution:** Not Sao Paulo Airport Marriott; not Westin Tampa Waterside.

### Plan once ready

| Intent | Applicability |
|--------|---------------|
| airport convenience / business | High — terminal-connected lodging (verify path) |
| group_meeting | Medium–high — meeting inventory claim (verify) |
| leisure | Medium — city/airport hybrid |
| day-use | Possible if evidenced — don’t assume |
| airline crew / disrupted pax | **Do not assume** as available demand |

**Critical factual boundary:** terminal/lobby connection ≠ unrestricted access to every departure gate.

---

## 6. Waterstone Boca — RESEARCH OK / PUBLICATION GATED

**ADP ID:** `adp_waterstone_boca_raton` · **HPC:** `recgMYovrrZDJMqzX`  
**Prior:** CERTIFIED · 73.1% capture · **externalDistributionHold = HOLD**  
**Planned builder:** ~93 scenarios · est. **~$12.09**

### Applicable scenarios

| Intent | Applicability |
|--------|---------------|
| leisure / couples | High — marina / waterfront |
| celebration / social events | High — outdoor event space |
| group_meeting / retreats | High — 18.9k sq ft + boutique scale |
| business | Medium–high — boutique vs big-box |
| family | Medium |
| wellness / watersports adventure | Medium — marina attributes |

**Do not** compare interchangeably to large convention hotels without use-case explanation.

### Publication

New research may proceed when authorized; **new client-ready edition / external share push** waits until HOLD clears or founder explicitly overrides with documented risk acceptance.

---

## Cross-batch rules

- Reuse verified Ren/Waterstone profile facts; re-verify material fields on activation.  
- ADP may note demand-positioning implications useful to GDI — **must not** manufacture GDI prospects or change GDI qualification/publication.  
- Keep ADP ledger/budget separate from GDI $42 and Mayflower/SD $11.
