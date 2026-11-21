# HIF Track A — CB2 / CB2b Commercial Opportunity Discovery

**As of:** 2026-11-21  
**Publication:** CUSTOMER_BLOCKED  
**Research spend:** **$0**  
**Branch:** `feat/hif-v6-track-a-cb2-commercial-discovery`  
**Baseline:** `origin/main` `81425a067`  
**Open PR:** **#544** (updated with CB2b)  
**E2E classification:** **PERSISTENCE_VALIDATED** (apply + idempotent read-back) — not FULL_E2E / not CUSTOMER_PUBLISHED

---

## Checkpoint (CB2b resume)

| Item | Result |
|------|--------|
| Prior PR #541 | **MERGED** |
| Prior CB2 commit | `94098da3f` on PR #544 — preserved; no reset |
| Uncommitted at CB2b start | Clean working tree (no discard) |
| Duplicate discovery / Airtable | Existing CB2 + #541 IDs = **NOOP** only |

---

## Status re-checks (CB2b)

| Hotel | Prior | CB2b result |
|-------|-------|------------|
| **Lakeville Inn** (US) | First-100 BCT | **STILL OPEN** — no brand announcement |
| **Hilton Trinidad** (TT) | First-100 MCT | **STILL OPEN** — HMA + HotelTT EOI; no award |
| **Legacy Hotel Green Bay** | First-100 BCT | **STILL OPEN** — Marriott soft brand; collection unnamed |

---

## 1. Hotels researched (CB2 + CB2b cumulative)

**34** distinct hotels/sites (US, CA, MX, BR, GB, TT, PT, ES, TC, AT, IN).

## 2. New opportunities (CB2b delta)

| Opportunity | Type | Status |
|-------------|------|--------|
| **Kimpton Shorebreak, Huntington Beach, CA** | **BCT** (private franchise/brand eval) | **NEW first-100** |
| **Courtyard Denver Downtown, CO** | **BCT** (franchise expires 2027; multi-path eval) | **NEW first-100** |
| Hotel Murta, Xàtiva, ES | Concession retender | OPEN (4th tender ASAP) |
| Antiga Colónia Areia Branca, PT | REVIVE hotel concession 50y | OPEN (due 2027-03-17) |

### Rejected / completed this batch (not persisted as open)

- Belvedere Vienna → Pullman + VERKEHRSBUERO (**awarded**)
- Super 8 Garysburg → Quality Inn (**completed**)
- IHG PentaHotels / Verdi Handwritten (**signed**)
- Omni Mont-Royal → owner independent ops (**deflag insufficient**)
- Grand Continent India → own-brand non-renewal (**not seeking partners**)

## 3. First-100 BCT/MCT

| Metric | Count |
|--------|------:|
| Open BCT | **4** (Lakeville + Legacy + Shorebreak + Courtyard Denver) |
| Open MCT | **1** (Hilton Trinidad) |
| **First-100** | **5 / 100** (Δ +2 vs CB2) |
| Commercially relevant / accessible | **5 / 5** |
| Outreach-ready | **0** |

### Shorebreak — Huntington Beach — PRIORITY A BCT (private)

- DiamondRock: Kimpton agreement **expired / month-to-month**
- Owner evaluating retain vs rebrand vs independent (Q2/Q3 2026)
- Sources: Motley Fool DRH Q3 2026-11-04; citybiz 2026-07-31

### Courtyard Denver Downtown — PRIORITY A BCT (private / portfolio)

- Franchise expires **2027**; multi-path eval including brand change / independent / expand / sale
- Sources: DRH Q3 2026-11-04; DRHC portfolio

## 4–7. Expanded · matrix · public/private

See `MATRIX_COUNTRY_TIMING_DEALTYPE_FRESHNESS.json` and `FIRST_100_SCORECARD.json`.

| Pathway | First-100 | Expanded open |
|---------|----------:|--------------:|
| Private | 4 | — |
| Public | 1 | 4 (+ Murta retender, Areia Branca) |

MX remains UNDER_RESEARCHED for new open principals.

## 8–9. Airtable

| Action | Result |
|--------|--------|
| CB2 creates (prior) | 10 |
| CB2b creates | **8** (4 inventory + 4 findings) |
| CB2b apply NOOPs | **14** (Lakeville/Trinidad/Legacy + prior CB2) |
| Idempotent retry | **22** NOOPs |
| Conflicts | **0** |
| HPC / schema | **0** |
| Publication | CUSTOMER_BLOCKED |

CB2b Airtable IDs: Shorebreak inv `recTnywid3vtCErTu` / find `recrgvSQeupLLKe0B` · Courtyard Denver `recmomZidWqHS7vTv` / `recBL4VUbarrWCUDS` · Hotel Murta `recsX2Ug2x9tdSBZO` / `recaKFVNNO9cgTE2D` · Areia Branca `rec6zL8zx4ldHZbnw` / `rec1CJcEItR3QpyjH`

Ledgers: `PERSISTENCE_LEDGER_DRY_RUN.json` · `PERSISTENCE_LEDGER_APPLY.json` · `PERSISTENCE_LEDGER_IDEMPOTENT.json` · `PERSISTENCE_LEDGER_FIRST_APPLY.json`

## 10. Cost, tests, PR

| Item | Value |
|------|-------|
| Cost | **$0** |
| Tests | `test:hif-track-a-continuous-foundation` · `test:hif-foundation-persistence` · `test:hif-v7-commercial-dimensions` |
| PR | **#544** — Integration Controller only — **do not merge from Track A** |

**CUSTOMER_BLOCKED**
