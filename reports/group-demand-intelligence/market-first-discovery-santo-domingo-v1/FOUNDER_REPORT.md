# GDI Market-First Discovery V1 — Santo Domingo — Founder Report

**Branch:** `deploy/gdi-pe-v1-7-customer-closure`  
**FINAL SHA:** `0c463a576c2bbef0e6ec61b55f039fe54969f603`  
**PUSH:** PASS  
**Cron:** HELD · **Deploy:** NOT RUN · **Webhound/Surfe AUTO:** OFF  
**Customer apply:** NOT RUN (0 hotel pairs reached CUSTOMER_READY)  
**DIRTY LEFT:** unrelated working-tree files preserved

---

## FINAL VERDICT

**MARKET-FIRST DISCOVERY WORKS — ONE MORE MARKET NEEDED BEFORE MIGRATION**

Santo Domingo proved: discover-once + dual-hotel evaluation + **no same-metro blind fanout**.  
It did **not** yet produce customer-ready hotel opportunities because published sources lacked submarket-precision lodging evidence after bounded research.

---

## A. EXECUTIVE RESULT

| Metric | Value |
|--------|------:|
| MARKET OPPORTUNITIES DISCOVERED | **43** |
| LODGING-SUPPORTED | **16** |
| OPEN / TBD | **12** |
| JW READY | **0** |
| JW NEEDS DATA | **0** |
| RADISSON READY | **0** |
| RADISSON NEEDS DATA | **0** |
| BOTH HOTELS | **0** |
| JW ONLY | **0** |
| RADISSON ONLY | **0** |
| NEITHER | **43** |

All 43 market opps resolved as **NEITHER** hotel-applicable after geography gates — destination evidence was metro-level (`Santo Domingo`) only. Same-city was correctly insufficient.

---

## B. MARKET RESEARCH FUNNEL

| Stage | Count |
|-------|------:|
| QUERIES | **51** (26 market SERP + 25 Jev followups) |
| FETCHES | **102** (≤140 budget) |
| VALID ENTITIES | **45** |
| VALID FUTURE PROGRAMS | **26** |
| LODGING-SUPPORTED | **16** |
| OPEN / TBD | **12** |
| HOTEL-MATCHABLE | **0** |

Wrong-market / historical rejects applied (Punta Cana / Santiago / pre-2026).

---

## C. MARKET OPPORTUNITY UNIVERSE

43 shared `marketOpportunityId` rows — see `MARKET_OPPORTUNITY_UNIVERSE.json`.

Shared fields only: program, dates, geography (`Santo Domingo`), lodging relationship where evidenced, commercial status, source URL, source family.  
**No JW/Radisson duplicate source packets.**

---

## D. HOTEL PAIR MATRIX

86 pairs evaluated (43 × 2).

| Pattern | Result |
|---------|--------|
| JW geo | UNKNOWN × 43 |
| Radisson geo | UNKNOWN × 43 |
| JW final | NOT_APPLICABLE × 43 |
| Radisson final | NOT_APPLICABLE × 43 |

Full matrix: `HOTEL_PAIR_MATRIX.json`.

---

## E–F. JW / RADISSON READY

None. Strict readiness not lowered. No `--apply` writes.

---

## G. DIFFERENTIATION

| Class | Count | Why |
|-------|------:|-----|
| BOTH HOTELS | 0 | — |
| JW ONLY | 0 | — |
| RADISSON ONLY | 0 | — |
| NEITHER | **43** | Metro-only destination; Piantini/Naco precision absent after bounded Jev location followups |

**This is the correct outcome for citywide-only evidence** — not a failure of selectivity.

Unit-tested geography (when submarket evidence exists):

| Destination | JW | Radisson |
|-------------|----|----------|
| Santo Domingo (metro) | UNKNOWN | UNKNOWN |
| Piantini / Blue Mall | DIRECT/STRONG | STRONG/PLAUSIBLE |
| Naco / Tiradentes | STRONG | DIRECT |

---

## H. GEOGRAPHIC EFFECT

| Evidence class | Outcome |
|----------------|---------|
| Same metro only | **86/86** hotel evals → UNKNOWN / NOT_APPLICABLE |
| Same submarket DIRECT/STRONG | **0** in live corpus (no specific geo in sources) |
| Weak/None | 0 |

**False fanout check:** same-metro-only opportunities were **held**, not promoted.

---

## I. JEV

| Metric | Value |
|--------|------:|
| MARKET-LEVEL ACTIONS | **25** |
| HOTEL-PAIR ACTIONS | **0** |
| BLOCKERS RESOLVED | **0** (location followups did not yield new submarket pages within budget) |
| FETCHES (total run) | **102** |
| WRONG ROUTES | 0 observed |

Primary market blocker: **VERIFY_EVENT_LOCATION** (submarket precision).

---

## J. EFFICIENCY

| Metric | Value |
|--------|------:|
| MARKET OPPORTUNITIES | 43 |
| HOTEL PAIRS EVALUATED | 86 |
| READY HOTEL OPPORTUNITIES | 0 |
| FETCHES / MARKET OPPORTUNITY | **2.37** |
| FETCHES / READY HOTEL OPPORTUNITY | n/a |
| READY / MARKET OPPORTUNITY | 0 |
| EST. DUPLICATE FETCHES AVOIDED VS HOTEL-FIRST | **~102** (one shared crawl, not 2× SERP universes) |

---

## K. WATCH

**MARKET FUTURE WATCH:** **24**

Shared market-level watches for future unresolved commercial triggers. Hotels listed as potentially applicable pending location precision — **no duplicated watch research per hotel**.

---

## L. NYC REGRESSION

| Check | Result |
|-------|--------|
| Renaissance | **11** |
| Hilton | **10** |
| NOW NOW | selective unchanged (0 ready; shadow control intact from prior pass) |
| Duplicate source truth | **0** |
| Bethesda / Waterstone / card UI / share tokens | untouched |
| Surfe AUTO / Webhound / cron / deploy | OFF / HELD |

---

## M. DIRECT ANSWERS

1. **Can GDI discover SD demand once at market level?** Yes — 43 shared market opportunities from one SERP/fetch universe.
2. **Evaluated against both hotels?** Yes — 86 pairs.
3. **Fit both?** 0 (correctly; metro-only).
4. **JW only?** 0
5. **Radisson only?** 0
6. **Geography differentiate Piantini vs Naco?** Model yes (unit-tested). Live corpus lacked published submarket anchors → no live differentiation rows.
7. **Product/meeting fit differentiate further?** Not reached (geo gate first).
8. **Jev help supporting data?** Routed 25 location/lodging actions; 0 material advances in this budget.
9. **Research duplicated?** No hotel-branched SERP; shared packets only.
10. **More hotel opps per research unit?** Architecture yes; ready yield 0 this market/pass.
11. **Wrong same-metro fanout?** **No** — 43/43 held as NEITHER.
12. **Shared evidence + hotel-specific fit clean?** Yes at model layer; live yield blocked on location precision.
13. **Enough for portfolio-wide migration?** **No.**
14. **Still needs validation?** A second market (or SD pass-2) with **richer venue/submarket lodging pages** producing ≥1 BOTH and ≥1 one-hotel-only ready/needs-data split under strict gates.

---

## HOTEL GEOGRAPHY (canonical)

| Hotel | HPC ID | Submarket | Micro-Area | Archetype | Rooms |
|-------|--------|-----------|------------|-----------|------:|
| JW Marriott Santo Domingo | `recESHsNsWUFYZrxR` | Piantini / Blue Mall | Winston Churchill / Blue Mall | piantini_urban_luxury | 150 |
| Radisson Santo Domingo | `recUOyzOXn2Zdp98I` | Naco / Tiradentes | Tiradentes / Presidente González | naco_urban_upper_upscale | 160 |

---

## PERSISTENCE

- No portfolio rollout · No Webhound · No Surfe AUTO · No Bethesda · No NYC mutation · No cron · No production deploy  
- SD customer writes: **none** (0 ready)  
- Artifacts: `reports/group-demand-intelligence/market-first-discovery-santo-domingo-v1/`
