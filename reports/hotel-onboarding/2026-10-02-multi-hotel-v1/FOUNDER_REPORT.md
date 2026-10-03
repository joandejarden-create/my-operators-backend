# ADP + GDI Multi-Hotel Onboarding Sprint V1

**Date:** 2026-10-03  
**Mode:** MODE B — finish / reconcile + YOTEL new onboard  
**Bases:** HPC `appCCUsuGsE1ifoLk` · HI/ADP/GDI `appa2cE7FTRmIbB32` · Legacy `appvtnDurnMSjINP6` unused  
**Webhound (YOTEL demand territory):** session `a84d9c38-c20f-4ea3-a2ae-0ca966db3961` (running sidecar; first-party facts already verified)

---

## A. Executive Result

| Hotel | Result |
|-------|--------|
| **YOTEL Geneva Lake** | New HPC created · HI_COMPLETE · 14 ADP attributes · GDI seed WEEKLY_READY · Official baseline **not** published · GDI discovery not yet producing Strict Ready |
| **Spice Island Beach Resort** | HI_COMPLETE reconfirmed · 46 ADP attributes reconciled · GDI apply cycle complete: 21 candidates → **2 HOLD_WATCH** · **0** customer-visible Strict Ready (public-data / surface+WHO ceiling) |
| **AC Hotel A Coruña** | HI_COMPLETE · 39 ADP attributes reconciled · GDI remains 0 Strict Ready; cycle-2 dry failed on Airtable `eventStartDate` — treat as public-data ceiling + schema defect |

---

## B. Identity / HPC

| Hotel | HPC | Identity confidence | Duplicates | Collisions |
|-------|-----|---------------------|------------|------------|
| YOTEL Geneva Lake | **`recrPQcZg7SFARRb2`** (created 2026-10-03) | HIGH | 0 | 0 — no prior YOTEL/Founex/Geneva Lake match |
| Spice Island | `recKRJjcPnb4tVDDS` | HIGH | 0 | 0 |
| AC A Coruña | `rec2PVBDavppGpenm` | HIGH | 0 | 0 |
| Bethesda | `recLuxvwwxID7U2B8` | — | — | **Untouched** |

YOTEL identity key: `yotel_ch_geneva_lake_founex`  
City = **Founex** (Vaud) — Market = Lake Geneva / La Côte — Submarket = Founex / Nyon corridor  
Rooms = **237** (first-party press)

---

## C. Hotel Intelligence

| Domain | YOTEL | Spice | AC |
|--------|-------|-------|-----|
| Commercial | POPULATED | POPULATED | POPULATED |
| Event spaces | POPULATED / partial inventory | POPULATED (outdoor wedding; indoor totals null) | POPULATED (6 rooms) |
| Demand nodes | RESEARCHED_EMPTY → territory config written for GDI; HI Airtable nodes pending Webhound close | POPULATED (6) | POPULATED (7) |
| Seasonality / need | RESEARCHED_EMPTY / NOT_PROVIDED | POPULATED / NOT_PROVIDED overlay | NOT_PROVIDED |
| Evidence | POPULATED | POPULATED | POPULATED |
| ADP Attributes | POPULATED (14) | POPULATED (46) | POPULATED (39) |
| **HI_COMPLETE** | **YES** | **YES** | **YES** |

---

## D. ADP Attributes

| Hotel | Before (historical) | After sync | Creates | Updates | Deactivates | Missing critical |
|-------|---------------------|------------|---------|---------|-------------|------------------|
| Spice | ~47 active | **46** | 0 | 46 | 1 | Meeting totals / room count (resort outdoor-first) |
| AC | 39 | **39** | 5 then settled | 39 | 0 | none |
| YOTEL | 0 | **14** | 14 path | 14 | 1 stale | Meeting Room Count, Total Meeting Space Sq Ft (first-party 6 rooms / 25–352 sqm not yet fully derived into HI commercial totals) |

Unexpected drift: **0** · Duplicate active: **0** · Legacy base writes: **0**

---

## E. ADP Readiness

| Hotel | ADP_READY | Certified run exists | Baseline state | Blockers |
|-------|-----------|----------------------|----------------|----------|
| YOTEL | YES | NO | `ADP_READY_FOR_BASELINE` — **OFFICIAL_BASELINE_PUBLISHED = NO** | Meeting attribute enrichment; no pilot start date |
| Spice | YES | Preflight/baseline artifacts under `reports/ai-demand-positioning/` — not overwritten | Leave published history intact | Meeting indoor inventory unknown (not ADP hard blocker) |
| AC | YES | Preflight/baseline artifacts present — not overwritten | Leave published history intact | none material for ADP |

---

## F. GDI Discovery

| Hotel | Seed weekly | Discovery ready | Notes |
|-------|-------------|-----------------|-------|
| YOTEL | WEEKLY_READY (applied) | YES | Market packet = La Côte / Airport / Geneva competitive / Lausanne stretch — Founex friction enforced in config |
| Spice | WEEKLY_READY | YES | Apply 2026-10-03: 25 queries · 21 candidates · VALID_WATCH 1 · promotions **HOLD_WATCH 2** · customerVisible 0 · Surfe 0 |
| AC | WEEKLY_READY | YES | Cycle2 dry failed `INVALID_VALUE_FOR_COLUMN` on `eventStartDate` — research maturity still RESEARCHED_NO_READY |

---

## G. GDI Ready Opportunities

| Hotel | Strict Ready | Notes |
|-------|--------------|-------|
| YOTEL | **0** | Discovery cycle not yet executed (seed only) |
| Spice | **0** | Apply held 2 as HOLD_WATCH (Shipbuilding/Aluminum 2026; Tugs/Towboats/Barges 2027) — surface/WHO/fit gates |
| AC | **0** | Cycle1–2 watchlist-only; schema write defect blocks cycle2 dry |

---

## H. Future Watch

| Hotel | Count (approx) | Dominant hold reasons |
|-------|----------------|----------------------|
| YOTEL | 0 this sprint | — |
| Spice | Many prior (MECA/TPA/EAPR class) | surface_eligibility, WHO, commercial_not_open / fully placed |
| AC | Watchlist-only from prior cycles | lodging evidence / future cycle / fit |

---

## I. Hotel-Specific Blockers

**YOTEL**
1. Meeting commercial totals not yet fully written into HI → ADP missing Meeting Room Count / Total Sq Ft  
2. Demand nodes not yet persisted to HI Airtable (GDI territory config exists)  
3. Permanent geocode deferred (address-only HPC)  
4. First market-first GDI discovery cycle not run  
5. Official ADP baseline intentionally unpublished (prospect/demo)

**Spice**
1. Boutique 64-suite luxury band vs large island events  
2. Indoor meeting inventory unknown  
3. Surface eligibility / WHO / housing decision gates on otherwise future lodging-supported events  
4. Public-data ceiling for Strict Ready volume is real — do not lower gates

**AC**
1. Customer-ready lodging evidence scarce under current gates  
2. Cycle-2 dry path Airtable `eventStartDate` invalid value — fix before next apply  
3. Treat as public-data ceiling until schema+lodging path repaired

---

## J. External Client / PDF / Archive

| Capability | Status |
|------------|--------|
| ADP Admin | Existing paths; YOTEL not customer-published |
| GDI Admin | Seed ready for YOTEL; Spice/AC research maturity RESEARCHED_NO_READY until Strict Ready > 0 |
| External client shares | **Not auto-published** this sprint |
| PDF generation | Plumbing exists; no fake historical reports created |
| Report Archive | Plumbing ready; no fabricated archives |

---

## K. Reusability

See `ONBOARDING_REUSABILITY.md`.

Reusable: HPC stewardship gated create · HI completeness onboard · ADP attribute sync · GDI hotel config + seed · alias map  
Market-specific: La Côte / Grenada / Galicia territory keywords + source families  
Property-specific: Founex friction, resort outdoor events, AC Expocoruña corridor  

---

## L. Regression

| Check | Result |
|-------|--------|
| Bethesda unchanged | **YES** |
| Share tokens changed | **NO** |
| Legacy base writes | **NO** |
| Surfe used | **NO** |

---

## M. Recommended Next Step

1. Close YOTEL Webhound session → persist demand nodes + meeting totals into HI → re-sync ADP attrs → first market-first GDI discovery  
2. Spice: keep HOLD_WATCH queue; do not lower gates — next cycle needs better surface/WHO evidence on Grenada lodging-open events  
3. Fix AC `eventStartDate` write validation → rerun market cycle dry  
4. Only publish official YOTEL ADP baseline when pilot start is instructed  

---

## Final verdicts

| Hotel | Verdict |
|-------|---------|
| YOTEL Geneva Lake | **ADP READY — GDI MORE RESEARCH REQUIRED** |
| Spice Island Beach Resort | **ADP READY — GDI PUBLIC DATA CEILING** |
| AC Hotel A Coruña | **ADP READY — GDI PUBLIC DATA CEILING** |
